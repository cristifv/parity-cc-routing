#!/usr/bin/env bash
# Phase-6 chaos scenarios (PLAN §7 "Chaos", §8 row 6), each <= 5 minutes, against real RE instances,
# a local etcd and a local Redpanda (scripts/local-ha.sh: free ports, so no e2e lock).
#   tests/chaos/run.sh kill9|sigstop|etcdcut|restart|move|steady|smoke <outdir> [--agents=N] [--rate=R] [--ttl=S]
# Every RE instance runs under a supervisor loop (restart on exit, like `restart: on-failure`) and
# reaches etcd through its own cuttable proxy (tests/chaos/tcp_proxy.py). The chaos driver plays IE
# and the gateway (tests/chaos/driver.cc) and checks the recorded streams; state_check checks
# re.state at the end; this script times every event (failover: the event to the new leader's
# "takeover done", and the client-visible pause from the driver's gaps) and writes <outdir>/report.json.
# Needs the binaries built first: bazel build //:parity-cc-routing //tests/chaos:all
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCEN=${1:?scenario}
OUT=${2:?outdir}
AGENTS=60 RATE=10 TTL=5 DURATION=400
for a in "${@:3}"; do
  case "$a" in
    --agents=*) AGENTS=${a#*=} ;;
    --rate=*) RATE=${a#*=} ;;
    --ttl=*) TTL=${a#*=} ;;
    --duration=*) DURATION=${a#*=} ;;
    *) echo "unknown option $a" >&2; exit 2 ;;
  esac
done
mkdir -p "$OUT"
W="$(mktemp -d -p "${TMPDIR:-/tmp}" re-chaos.XXXXXX)"
T0=$(date +%s)
cp "$ROOT/bazel-bin/parity-cc-routing" "$W/re"
# Phases 5b+5c: the copy finds libtorch's shared libraries in Bazel's _solib dir.
export LD_LIBRARY_PATH="$(find "$(dirname "$(readlink -f "$ROOT/bazel-bin/parity-cc-routing")")/_solib_k8" -name libtorch_cpu.so -printf '%h\n' | head -1)${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
cp "$ROOT/bazel-bin/tests/chaos/chaos_driver" "$ROOT/bazel-bin/tests/chaos/state_check" "$W/"
chmod 700 "$W/re" "$W/chaos_driver" "$W/state_check"
ms() { date +%s%3N; }
event() { echo "$(ms) $*" >>"$W/events"; }
PIDS=()
cleanup() {
  touch "$W/stop"
  for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  pkill -TERM -f "^$W/re " 2>/dev/null || true
  sleep 0.5
  pkill -KILL -f "^$W/re " 2>/dev/null || true
  pkill -CONT -f "^$W/re " 2>/dev/null || true
  "$ROOT/scripts/local-ha.sh" down "$W" || true
  cp "$W"/*.log "$W"/events "$W"/*.json "$W"/*.jsonl "$OUT/" 2>/dev/null || true
  rm -rf "$W"
}
trap cleanup EXIT
"$ROOT/scripts/local-ha.sh" up "$W"
# shellcheck disable=SC1091
source "$W/ha.env"
python3 -I - "$AGENTS" "$W/seed.json" <<'PY'
import json, sys
n, out = int(sys.argv[1]), sys.argv[2]
def tenant(tid):
    return {"id": tid, "bin": "bin-a", "reject_skip_s": 30, "assignment_deadline_s": 30, "grace_s": 60,
            "queues": [{"id": "sales", "priority": 50, "offer_timeout_s": 15, "wrap_up_limit_s": 30, "sla_s": 20,
                        "max_wait_s": 600, "max_wait_action": {"action": "disconnect"}}],
            "agents": [{"id": "agent-%03d" % i, "queues": ["sales"]} for i in range(1, n + 1)]}
json.dump({"schema_version": 1, "description": "phase-6 chaos", "tenants": [tenant("acme"), tenant("globex")]}, open(out, "w"))
PY
free_ports() { python3 -I -c "import socket
s=[socket.socket() for _ in range($1)]
[x.bind(('127.0.0.1',0)) for x in s]
print(' '.join(str(x.getsockname()[1]) for x in s))"; }
declare -A GRPC OPS BINOF
start_re() {  # name bin
  local name=$1 bin=$2 g o
  read -r g o <<<"$(free_ports 2)"
  GRPC[$name]=$g OPS[$name]=$o BINOF[$name]=$bin
  mkdir -p "$W/health-$name"
  python3 -I "$ROOT/tests/chaos/tcp_proxy.py" 0 "$ETCD" "$W/$name.cut" "$W/$name.proxyport" 2>>"$W/proxy-$name.log" &
  PIDS+=($!)
  for _ in $(seq 1 50); do [[ -s "$W/$name.proxyport" ]] && break; sleep 0.05; done
  local pp; pp=$(cat "$W/$name.proxyport")
  (
    while [[ ! -f "$W/stop" ]]; do
      set +e
      "$W/re" --env=local-dev --bin="$bin" --seed="$W/seed.json" --ha=on --etcd="127.0.0.1:$pp" \
        --kafka-brokers="$BROKERS" --listen="127.0.0.1:$g" --advertise="127.0.0.1:$g" --ops-listen="127.0.0.1:$o" \
        --health-dir="$W/health-$name" --shards=2 --first-start=yes --lease-ttl-s="$TTL" >>"$W/re-$name.log" 2>&1
      rc=$?
      set -e
      echo "$(ms) exit $rc" >>"$W/exits-$name"
      [[ -f "$W/stop" ]] && break
      sleep 1
    done
  ) &
  PIDS+=($!)
}
pid_of() { pgrep -f "^$W/re .*--advertise=127.0.0.1:${GRPC[$1]}( |\$)" | head -1; }
is_leader() {
  local page
  page="$(curl -s --max-time 1 "http://127.0.0.1:${OPS[$1]}/metrics" 2>/dev/null || true)"
  grep -q '^parity_cc_routing_leader 1$' <<<"$page"
}
leader_of() {  # bin
  local n
  for n in "${!GRPC[@]}"; do
    [[ "${BINOF[$n]}" == "$1" ]] && is_leader "$n" && { echo "$n"; return 0; }
  done
  return 1
}
wait_leader() {  # bin timeout_s
  local i
  for i in $(seq 1 $(( $2 * 10 ))); do leader_of "$1" >/dev/null && return 0; sleep 0.1; done
  echo "no leader for $1 after $2 s" >&2
  return 1
}
targets=()
case "$SCEN" in
  move) start_re a bin-a; start_re b bin-b ;;
  restart) start_re a1 bin-a ;;
  *) start_re a1 bin-a; start_re a2 bin-a ;;
esac
wait_leader bin-a 20
[[ "$SCEN" == move ]] && wait_leader bin-b 20
sleep 1
python3 -I - "$W" "$AGENTS" "$RATE" "$DURATION" "$(for n in "${!GRPC[@]}"; do echo "${BINOF[$n]}@127.0.0.1:${GRPC[$n]}"; done | sort | tr '\n' ' ')" <<'PY'
import json, sys
w, n, rate, dur, targets = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4]), sys.argv[5].split()
cfg = {"tenant": "acme", "queue": "sales", "agents": ["agent-%03d" % i for i in range(1, n + 1)], "rate": rate,
       "duration_s": dur, "drain_s": 60, "talk_ms_min": 2000, "talk_ms_span": 3000, "log": w + "/driver.jsonl",
       "stop_file": w + "/stop-arrivals", "kill_on_accept_file": w + "/kill-on-accept",
       "targets": [{"bin": t.split("@")[0], "addr": t.split("@")[1]} for t in targets]}
json.dump(cfg, open(w + "/driver-config.json", "w"))
PY
"$W/chaos_driver" "$W/driver-config.json" "$W/summary.json" >"$W/driver.log" 2>&1 &
DRV=$!
PIDS+=($DRV)
sleep "$([[ $SCEN == smoke ]] && echo 6 || echo 15)"  # warm: agents attached and ready, calls flowing
case "$SCEN" in
  kill9)
    # Kills 1 and 3 at a random moment (offers are always in flight at this load: mid-offer); kills 2
    # and 4 by the driver itself 2 ms after it sends an accept (mid-accept).
    for k in 1 2 3 4; do
      L=$(leader_of bin-a)
      if (( k % 2 == 0 )); then
        pid_of "$L" >"$W/kill-on-accept.tmp"
        mv "$W/kill-on-accept.tmp" "$W/kill-on-accept"
        for _ in $(seq 1 100); do [[ -f "$W/kill-on-accept.taken" ]] && break; sleep 0.05; done
        rm -f "$W/kill-on-accept.taken"
        event "kill9-on-accept $L"
      else
        event "kill9 $L"
        kill -9 "$(pid_of "$L")"
      fi
      wait_leader bin-a 30
      event "leader $(leader_of bin-a)"
      sleep 18
    done ;;
  sigstop)
    for k in 1 2 3; do
      L=$(leader_of bin-a)
      P=$(pid_of "$L")
      event "sigstop $L"
      kill -STOP "$P"
      sleep 5
      kill -CONT "$P"
      event "sigcont $L"
      wait_leader bin-a 30
      event "leader $(leader_of bin-a)"
      sleep 20
    done ;;
  etcdcut)
    for k in 1 2; do
      L=$(leader_of bin-a)
      event "etcdcut $L"
      touch "$W/$L.cut"
      for _ in $(seq 1 100); do leader_of bin-a | grep -qv "^$L\$" && break; sleep 0.1; done
      event "leader $(leader_of bin-a || echo none)"
      sleep 6
      rm -f "$W/$L.cut"
      event "etcdheal $L"
      sleep 20
    done ;;
  restart)
    # Restart from an empty memory: the only instance dies; a new process rebuilds from re.state.
    for k in 1 2; do
      event "kill9 a1"
      kill -9 "$(pid_of a1)"
      wait_leader bin-a 30
      event "leader a1"
      sleep 25
    done ;;
  move)
    # Tenant move N <-> N-1 both ways, with queued and connected calls (tp/tenants/acme, handover).
    sleep 10
    event "move acme bin-b"
    docker exec "$ETCD_C" etcdctl --endpoints=http://127.0.0.1:2379 put tp/tenants/acme '{"bin":"bin-b","epoch":1}' >/dev/null
    sleep 30
    event "move acme bin-a"
    docker exec "$ETCD_C" etcdctl --endpoints=http://127.0.0.1:2379 put tp/tenants/acme '{"bin":"bin-a","epoch":2}' >/dev/null
    sleep 30 ;;
  steady)
    # No chaos: a leader, a standby and agents always free (the commit's cost on time to offer).
    sleep 45 ;;
  smoke)
    # scripts/ci.sh's HA smoke: one kill -9 of the leader 2 ms after an accept, then the checks.
    L=$(leader_of bin-a)
    pid_of "$L" >"$W/kill-on-accept.tmp"
    mv "$W/kill-on-accept.tmp" "$W/kill-on-accept"
    for _ in $(seq 1 100); do [[ -f "$W/kill-on-accept.taken" ]] && break; sleep 0.05; done
    rm -f "$W/kill-on-accept.taken"
    event "kill9-on-accept $L"
    wait_leader bin-a 30
    event "leader $(leader_of bin-a)"
    sleep 8 ;;
  *) echo "unknown scenario $SCEN" >&2; exit 2 ;;
esac
touch "$W/stop-arrivals"
event "arrivals stopped"
set +e
wait "$DRV"
DRC=$?
set -e
event "driver done $DRC"
"$W/state_check" "$BROKERS" re.state "$W/seed.json" acme >"$W/state-check.json" 2>"$W/state-check.log"
SC=$?
python3 -I - "$W" "$SCEN" "$DRC" "$SC" "$AGENTS" "$RATE" "$TTL" "$(( $(date +%s) - T0 ))" <<'PY'
import datetime, glob, json, os, sys
w, scen, drc, sc, agents, rate, ttl, wall = sys.argv[1:9]
events = [l.split(" ", 2) for l in open(w + "/events").read().splitlines()]
events = [(int(t), k, rest) for t, k, *r in events for rest in [r[0] if r else ""]]
summ = json.load(open(w + "/summary.json")) if os.path.exists(w + "/summary.json") else {}
state = json.load(open(w + "/state-check.json")) if os.path.getsize(w + "/state-check.json") else {}
log = [json.loads(l) for l in open(w + "/driver.jsonl")]
takeovers = []
for f in glob.glob(w + "/re-*.log"):
    for l in open(f):
        if '"takeover.done"' in l:  # log-record.v1 (phase 8): ts RFC 3339, the time in attrs
            j = json.loads(l)
            ts = datetime.datetime.fromisoformat(j["ts"].replace("Z", "+00:00")).timestamp()
            takeovers.append((int(round(ts * 1000)), int(j["attrs"]["takeover_ms"]), os.path.basename(f)))
takeovers.sort()
gaps = summ.get("gaps", [])
out = {"scenario": scen, "agents": int(agents), "rate_per_s": float(rate), "lease_ttl_s": int(ttl), "wall_s": int(wall),
       "driver_exit": int(drc), "state_check_exit": int(sc), "events": []}
for t, kind, rest in events:
    if kind not in ("kill9", "kill9-on-accept", "sigstop", "etcdcut", "move"):
        continue
    if kind == "kill9-on-accept":
        k = [x for x in log if x["k"] == "killed_on_accept" and x["t"] <= t]
        if k:
            t = k[-1]["t"]  # the kill's own time (the event line is written after it)
    e = {"event": kind, "at": t, "who": rest}
    nxt = [x for x in takeovers if x[0] > t]
    if nxt and kind != "move":
        e["new_leader_serving_after_ms"] = nxt[0][0] - t
        e["takeover_ms"] = nxt[0][1]
    g = [x for x in gaps if x["from"] <= t + 50 and x["to"] >= t]
    if not g and kind == "etcdcut":
        # The cut leader serves until it steps down (TTL - 1.5 s): the pause starts then.
        g = [x for x in gaps if t <= x["from"] <= t + 10000]
    if g:
        e["client_pause_ms"] = g[0]["ms"]
        e["client_recovered_after_ms"] = g[0]["to"] - t
    # In flight at the event: offers made and not yet answered or revoked; accepts sent, not answered.
    opened, accepts = {}, {}
    for x in log:
        if x["t"] > t:
            break
        if x["k"] == "offer":
            opened[x["offer"]] = x["t"]
        elif x["k"] in ("offer_revoked", "accept_done") and x.get("offer") in opened:
            del opened[x["offer"]]
        if x["k"] == "accept_sent":
            accepts[x["offer"]] = x["t"]
        elif x["k"] == "accept_done":
            accepts.pop(x["offer"], None)
    e["offers_in_flight"] = len(opened)
    e["accepts_in_flight"] = len(accepts)
    # Calls waiting in the queue (enqueued, no Assigned yet) and calls connected (not yet ended) then.
    enq_t, asg, con, end = {}, set(), set(), set()
    for x in log:
        if x["t"] > t:
            break
        if x["k"] == "enqueued" and x.get("ok"):
            enq_t[x["ix"]] = x["t"]
        elif x["k"] == "assigned":
            asg.add(x["ix"])
        elif x["k"] == "outcome" and x.get("recorded") == "OUTCOME_CONNECTED":
            con.add(x["ix"])
        elif x["k"] == "ended" and x.get("ok"):
            end.add(x["ix"])
    e["queued_at_event"] = len([i for i in enq_t if i not in asg])
    e["connected_at_event"] = len(con - end)
    out["events"].append(e)
out["driver"] = summ
out["state_check"] = state
fo = [e["new_leader_serving_after_ms"] for e in out["events"] if "new_leader_serving_after_ms" in e]
cp = [e["client_pause_ms"] for e in out["events"] if "client_pause_ms" in e]
out["failover_ms"] = {"n": len(fo), "max": max(fo) if fo else None, "values": fo}
out["client_pause_ms"] = {"n": len(cp), "max": max(cp) if cp else None, "values": cp}
out["takeover_ms"] = [x[1] for x in takeovers]
# Time to offer outside the failover pauses (the call enqueued and offered with no gap in between).
enq = {x["ix"]: x.get("sent", x["t"]) for x in log if x["k"] == "enqueued" and x.get("ok")}
first = {}
for x in log:
    if x["k"] == "offer" and x["ix"] not in first:
        first[x["ix"]] = x["t"]
tto = []
for ix, t1 in first.items():
    t0 = enq.get(ix)
    if t0 is None:
        continue
    if any(g["from"] - 6000 <= t1 and t0 <= g["to"] for g in gaps):
        continue
    tto.append(t1 - t0)
tto.sort()
if tto:
    out["time_to_offer_steady_ms"] = {"n": len(tto), "p50": tto[len(tto) // 2], "p99": tto[min(len(tto) - 1, int(len(tto) * 0.99))], "max": tto[-1],
                                      "note": "Enqueue sent -> Offer on the agent stream (commit included), calls outside failover pauses"}
out["pass"] = int(drc) == 0 and int(sc) == 0 and not summ.get("violations")
json.dump(out, open(w + "/report.json", "w"), indent=1)
print(json.dumps({k: out.get(k) for k in ("scenario", "pass", "wall_s", "failover_ms", "client_pause_ms", "takeover_ms", "time_to_offer_steady_ms")}))
print("violations:", summ.get("violations", [])[:5], "state:", state.get("record_violations", [])[:3], state.get("invariant_violations", [])[:3])
PY
[[ $DRC == 0 && $SC == 0 ]]
