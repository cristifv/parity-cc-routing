#!/usr/bin/env bash
# Phase 7 load scenarios (PLAN §7 "Load", §8 row 7, §6.1), each <= 5 minutes, against the real RE
# binary built -c opt (one process; --ha=on adds a local etcd + Redpanda through scripts/local-ha.sh
# and RE commits every change to re.state before it acts). The load driver (tests/load/driver.cc)
# plays the gateway's and IE's replicas over gRPC in real time; tests/load/sample.py samples RE's
# RSS and CPU per thread group; tests/load/judge.py judges the targets and writes <outdir>/report.json.
#   tests/load/run.sh shift-start|calls|optimizer <outdir> [--ha] [--agents=N] [--pre=N] [--rate=R]
#                     [--concurrent=N] [--duration=S] [--calls-rate=R] [--smb=N --mid=N --ent=N] [--shards=N]
# Scenarios:
#   shift-start  N agents (default 10,000) attach and go READY at R/s (default 500, the gateway's
#                limiter) while calls flow to --pre agents logged in before (default 500): AttachAgent
#                p99 < 300 ms; time to offer p99 < 50 ms before, during and after the storm; I9.
#   calls        N agents (default 10,000) logged in; calls ramp at 50/s to --concurrent (default
#                5,000) and hold with bursts of 50 calls/s (talk 100-140 s): time to offer p99 < 50 ms,
#                I9, every call offered, none lost; RE's RSS (I10: <= 512 MiB).
#   stepup       the ceiling (the knee), one run: storms at rising attach rates (1,000, 2,500, 5,000,
#                10,000/s) while calls flow, then call steps at rising rates (--steps=100,200,500,1000,2000,4000
#                calls/s, 20 s each, talk 3-6 s); every window judged on its own (achieved rate >= 90 % of the
#                configured one, time to offer p99 < 50 ms, AttachAgent p99 < 300 ms, I9) and the knee reported.
#   spike        I10 at full scale against the binary (CR-091: RE's own RSS): N agents (default 10,000)
#                at the busy-day rate (0.85 x N / 180 s = 47 calls/s), then 10x for 60 s, then the busy
#                rate again (talk 120-240 s); max_entries 1,000 per queue fills (5,000 entries) and
#                queue_full refuses the rest: RE's RSS <= 512 MiB (VmHWM), I9, no call lost.
#   optimizer    2,000 synthetic tenants (85 % SMB, 14 % mid, 1 % enterprise; sim/budget.cc's shapes) in
#                one bin, every agent logged in, --optimizer=on (config.routing mode auto), calls
#                flowing: the optimization threads' CPU <= 0.45 core (PLAN §3.5), I9, time to offer.
# Ports: PCR_LOAD_GRPC_PORT / PCR_LOAD_OPS_PORT, else free loopback ports (no fixed port, no e2e lock).
# Binaries: PCR_LOAD_BIN_DIR (default: the opt output base's bazel-bin). Build first:
#   bazel --output_base=$HOME/.cache/bazel/parity-cc-routing-opt build -c opt --symlink_prefix=/ \
#     //:parity-cc-routing //tests/load:load_driver
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCEN=${1:?scenario: shift-start|calls|optimizer}
OUT=${2:?outdir}
HA=0 AGENTS="" PRE=500 STEPS=100,200,500,1000,2000,4000 RATE=500 CONC=5000 DURATION="" CRATE="" SMB=1700 MID=280 ENT=20 SHARDS=""
for a in "${@:3}"; do
  case "$a" in
    --ha) HA=1 ;;
    --agents=*) AGENTS=${a#*=} ;;
    --pre=*) PRE=${a#*=} ;;
    --rate=*) RATE=${a#*=} ;;
    --concurrent=*) CONC=${a#*=} ;;
    --duration=*) DURATION=${a#*=} ;;
    --calls-rate=*) CRATE=${a#*=} ;;
    --smb=*) SMB=${a#*=} ;;
    --mid=*) MID=${a#*=} ;;
    --ent=*) ENT=${a#*=} ;;
    --shards=*) SHARDS=${a#*=} ;;
    --steps=*) STEPS=${a#*=} ;;
    *) echo "unknown option $a" >&2; exit 2 ;;
  esac
done
BIN_DIR=${PCR_LOAD_BIN_DIR:-$(bazel --output_base="$HOME/.cache/bazel/parity-cc-routing-opt" info -c opt bazel-bin 2>/dev/null)}
[[ -x "$BIN_DIR/parity-cc-routing" && -x "$BIN_DIR/tests/load/load_driver" ]] || { echo "build first (see the header)" >&2; exit 2; }
mkdir -p "$OUT"
W="$(mktemp -d -p "${TMPDIR:-/tmp}" re-load.XXXXXX)"
T0=$(date +%s)
cp "$BIN_DIR/parity-cc-routing" "$W/re"
cp "$BIN_DIR/tests/load/load_driver" "$W/load_driver"
chmod 700 "$W/re" "$W/load_driver"
RE_PID=""
cleanup() {
  touch "$W/stop"
  [[ -n "$RE_PID" ]] && kill -TERM "$RE_PID" 2>/dev/null || true
  sleep 0.5
  [[ -n "$RE_PID" ]] && kill -KILL "$RE_PID" 2>/dev/null || true
  (( HA )) && "$ROOT/scripts/local-ha.sh" down "$W" || true
  cp "$W"/*.json "$W"/*.jsonl "$W"/*.log "$W"/metrics.txt "$OUT/" 2>/dev/null || true
  rm -f "$OUT/seed.json"
  rm -rf "$W"
}
trap cleanup EXIT
echo "host: $(nproc) cores, $(awk '/MemTotal/ {printf "%.0f GiB", $2/1048576}' /proc/meminfo); load before: $(cut -d' ' -f1-3 /proc/loadavg)"

# The seed, the driver's config and the targets.
python3 -I - "$SCEN" "$W" "${AGENTS:-10000}" "$PRE" "$RATE" "$CONC" "${DURATION:-}" "${CRATE:-}" "$SMB" "$MID" "$ENT" "$STEPS" <<'PY'
import json, sys
scen, w, agents, pre, rate, conc, duration, crate, smb, mid, ent, steps = sys.argv[1:13]
agents, pre, rate, conc, smb, mid, ent = int(agents), int(pre), float(rate), int(conc), int(smb), int(mid), int(ent)

def queue(qid):
    return {"id": qid, "priority": 50, "offer_timeout_s": 15, "wrap_up_limit_s": 60, "sla_s": 20,
            "max_wait_s": 300, "max_wait_action": {"action": "disconnect"}}

def tenant(tid, nq, na, shapes, per_shape, mode=None):
    # na agents in `shapes` skill sets, each set `per_shape` queues (sim/budget.cc's tenant shape).
    qs = ["q%d" % (i + 1) for i in range(nq)]
    ags = []
    for i in range(na):
        g = i % shapes
        member = sorted({qs[(g * nq // shapes + k) % nq] for k in range(per_shape)})
        ags.append({"id": "agent-%05d" % (i + 1), "queues": member})
    t = {"id": tid, "bin": "bin-a", "reject_skip_s": 30, "assignment_deadline_s": 30, "grace_s": 60,
         "queues": [queue(q) for q in qs], "agents": ags}
    if mode:
        t["routing"] = {"version": 1, "mode": mode}
    return t

cfg = {"bin": "bin-a", "seed": w + "/seed.json", "gw_replicas": 4, "ie_replicas": 2, "workers": 128, "channels": 8}
targets = {"decision_p99_ms": 5, "decision_max_ms": 50, "tto_p99_ms": 50}
if scen == "shift-start":
    tenants = [tenant("acme", 5, pre + agents, 5, 2)]
    cfg["prelogin"] = {"agents": pre, "rate": 2000}
    cfg["storm"] = {"at_s": 10, "rate": rate}
    dur = float(duration or (10 + agents / rate + 12))
    cfg["calls"] = {"rate": float(crate or 20), "talk_s": [5, 15], "answer_ms": [50, 250], "wrap_ms": 500,
                    "duration_s": dur}
    cfg["drain_s"] = 20
    targets.update({"storm_agents": agents, "storm_s_max": agents / rate * 1.05 + 0.5, "attach_p99_ms": 300})
elif scen == "calls":
    tenants = [tenant("acme", 5, agents, 5, 2)]
    cfg["prelogin"] = {"agents": -1, "rate": 2500}
    ramp = 50.0
    hold = conc / 120.0
    dur = float(duration or (conc / ramp + 80))
    cfg["calls"] = {"rate": round(hold, 2), "ramp_rate": ramp, "target_concurrent": conc,
                    "burst": {"every_s": 15, "len_s": 5, "rate": 50}, "talk_s": [100, 140], "answer_ms": [50, 250],
                    "wrap_ms": 500, "duration_s": dur}
    cfg["drain_s"] = 30
    targets.update({"concurrent": int(conc * 0.98), "rss_mib_max": 512})
elif scen == "stepup":
    storms = [(1000, 5000), (2500, 10000), (5000, 15000), (10000, 20000)]
    tenants = [tenant("acme", 5, 1000 + sum(n for _, n in storms), 5, 2)]
    cfg["prelogin"] = {"agents": 1000, "rate": 2000}
    cfg["storms"] = [{"at_s": 5 + 10 * k, "rate": r, "agents": n} for k, (r, n) in enumerate(storms)]
    cfg["calls"] = {"rate": float(crate or 20), "start_s": 45, "steps": [{"rate": float(x), "s": 20} for x in steps.split(",")],
                    "talk_s": [3, 6], "answer_ms": [50, 250], "wrap_ms": 300}
    cfg["drain_s"] = 20
    cfg["workers"] = 256
    targets.update({"stepup": True, "attach_p99_ms": 300})
elif scen == "spike":
    tenants = [tenant("acme", 5, agents, 5, 2)]
    cfg["prelogin"] = {"agents": -1, "rate": 2500}
    busy = round(0.85 * agents / 180.0, 2)
    cfg["calls"] = {"rate": busy, "start_s": 0, "steps": [{"rate": busy, "s": 60}, {"rate": 10 * busy, "s": float(duration or 60)},
                                                          {"rate": busy, "s": 30}],
                    "talk_s": [120, 240], "answer_ms": [50, 250], "wrap_ms": 500}
    cfg["drain_s"] = 15
    cfg["workers"] = 256
    targets.update({"rss_mib_max": 512, "all_offered": False, "tto_check": False, "must_refuse": True})
elif scen == "optimizer":
    tenants = []
    for i in range(smb):
        tenants.append(tenant("smb-%04d" % (i + 1), 3, 8, 2, 2, "auto"))
    for i in range(mid):
        tenants.append(tenant("mid-%04d" % (i + 1), 15, 120, 6, 4, "auto"))
    for i in range(ent):
        tenants.append(tenant("ent-%04d" % (i + 1), 200, 3000, 50, 6, "auto"))
    cfg["prelogin"] = {"agents": -1, "rate": float(rate if rate != 500 else 6000)}
    dur = float(duration or 130)
    cfg["calls"] = {"rate": float(crate or 60), "talk_s": [30, 90], "answer_ms": [50, 250], "wrap_ms": 500,
                    "duration_s": dur}
    cfg["drain_s"] = 20
    cfg["workers"] = 192
    targets.update({"optimizer_cores_max": 0.45, "warmup_s": 25, "min_solves": 2 * (smb + mid + ent)})
else:
    sys.exit("unknown scenario " + scen)
json.dump({"schema_version": 1, "description": "phase-7 load: " + scen, "tenants": tenants}, open(w + "/seed.json", "w"))
cfg["target"] = "TARGET"
json.dump(cfg, open(w + "/driver-config.json", "w"), indent=1)
json.dump(targets, open(w + "/targets.json", "w"), indent=1)
print("seed: %d tenants, %d agents, %d queues" % (len(tenants), sum(len(t["agents"]) for t in tenants),
                                                   sum(len(t["queues"]) for t in tenants)))
PY
free_ports() { python3 -I -c "import socket
s=[socket.socket() for _ in range($1)]
[x.bind(('127.0.0.1',0)) for x in s]
print(' '.join(str(x.getsockname()[1]) for x in s))"; }
read -r G O <<<"$(free_ports 2)"
G=${PCR_LOAD_GRPC_PORT:-$G} O=${PCR_LOAD_OPS_PORT:-$O}
sed -i "s/\"TARGET\"/\"127.0.0.1:$G\"/" "$W/driver-config.json"
FLAGS=(--env=local-dev --bin=bin-a --seed="$W/seed.json" --listen="127.0.0.1:$G" --ops-listen="127.0.0.1:$O"
       --health-dir="$W/health")
[[ -n "$SHARDS" ]] && FLAGS+=(--shards="$SHARDS")
[[ "$SCEN" == optimizer ]] && FLAGS+=(--optimizer=on)
mkdir "$W/health" "$W/state"
if (( HA )); then
  "$ROOT/scripts/local-ha.sh" up "$W"
  # shellcheck disable=SC1091
  source "$W/ha.env"
  FLAGS+=(--ha=on --etcd="$ETCD" --kafka-brokers="$BROKERS" --advertise="127.0.0.1:$G" --first-start=yes)
else
  FLAGS+=(--state-dir="$W/state" --first-start=yes)
fi
"$W/re" "${FLAGS[@]}" >"$W/re.log" 2>&1 &
RE_PID=$!
for _ in $(seq 1 600); do
  page="$(curl -s --max-time 1 "http://127.0.0.1:$O/metrics" 2>/dev/null || true)"
  if (( HA )); then grep -q '^parity_cc_routing_leader 1$' <<<"$page" && break; else [[ -f "$W/health/parity-cc-routing.healthy" ]] && break; fi
  kill -0 "$RE_PID" 2>/dev/null || { tail -20 "$W/re.log"; echo "RE exited" >&2; exit 1; }
  sleep 0.1
done
echo "RE up (pid $RE_PID, grpc :$G, ops :$O, ha=$HA) after $(( $(date +%s) - T0 )) s; load: $(cut -d' ' -f1-3 /proc/loadavg)"
python3 -I "$ROOT/tests/load/sample.py" "$RE_PID" "$W/samples.jsonl" "$W/stop" "http://127.0.0.1:$O/metrics" &
D0=$(date +%s)
set +e
/usr/bin/time -f '{"driver_cpu_user_s": %U, "driver_cpu_sys_s": %S, "driver_wall_s": %e, "driver_max_rss_kib": %M}' -o "$W/driver-time.json" \
  "$W/load_driver" "$W/driver-config.json" "$W/summary.json" >"$W/driver.log" 2>&1
rc=$?
set -e
D1=$(date +%s)
curl -s --max-time 5 "http://127.0.0.1:$O/metrics" >"$W/metrics.txt" || true
touch "$W/stop"
sleep 1.2
kill -0 "$RE_PID" 2>/dev/null || echo "RE died during the run" >&2
[[ -f "$W/summary.json" ]] || { tail -30 "$W/driver.log"; echo "the driver wrote no summary (exit $rc)" >&2; exit 1; }
echo "driver: exit $rc, $(( D1 - D0 )) s; $(cat "$W/driver-time.json" 2>/dev/null)"
set +e
python3 -I "$ROOT/tests/load/judge.py" "$SCEN$( (( HA )) && echo -ha)" "$W" "$W/targets.json"
jrc=$?
set -e
echo "wall: $(( $(date +%s) - T0 )) s (driver $(( D1 - D0 )) s); load after: $(cut -d' ' -f1-3 /proc/loadavg)"
exit $jrc
