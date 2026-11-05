#!/usr/bin/env bash
# parity-cc-routing CI (PLAN §8 1.1): build, tests, sanitizers, the binary's smoke run, the Erlang
# gate, dependency ages (14-day rule) and OSV. Run from anywhere; no fixed ports are used.
# Phase 2: the CTMC numerics, the DES smoke, the model's EWT behind --model-ewt, the invariant search
# and its regression replays, the RE-side scenario files' schema check.
# Phases 4 and 3: the moves and phase-3 unit tests (also under ASan), every routing.v1 and agents.v1
# script, and the Postgres store against a throwaway postgres container (scripts/store-it.sh).
#
# Two tiers (the user's rule, 2026-10-31: a test run takes at most 5 minutes):
#   scripts/ci.sh          the default tier, <= 5 min with warm Bazel caches: every unit, numerics, DES,
#                          script, scenario, fault, regression and 12-seed search test; ASan/UBSan and
#                          TSan on the core targets and the canonical scenario sets (CR-094: the user's
#                          rule shrinks runs, not gates); the smoke run, the Erlang gate, dependency
#                          ages, OSV. The plain, ASan and TSan lanes run at the same time (own output bases).
#   scripts/ci.sh --full   adds the 40-seed search, the 10-seed search under ASan, and the scaled spike
#                          and shift-start runs (tests/scenarios/scaled, -c opt, I9/I10 judged). Longer;
#                          not needed for a phase-2 acceptance.
# The true full-scale runs (10k agents) stay manual: //tests/scenarios:full_scale_test (CR-090).
# Phase 5 (redesign): phase5_test (VIP, levers, the per-agent model, the forecast interface, events,
# outbox cap and quarantine) in all three lanes; the binary's --optimizer=on smoke and refusal; the
# store IT's bucket history load. The matrix, closed loop and budget runs are evidence (manual).
# Phase 6: the replay property test (plain + ASan), `buf breaking` of re.state's state.proto, the
# binary's HA refusals, and an HA smoke in parallel with the test lanes (tests/chaos/run.sh smoke: a
# local etcd + Redpanda, a leader and a standby, one kill -9 mid-accept, the stream and re.state
# invariant checks).
#   scripts/ci.sh --chaos  adds the phase-6 chaos scenarios one after another (kill9, sigstop, etcdcut,
#                          restart, move, steady; each <= 5 min, ~12 min in all).
# Phase 8: campaign lending (lending_test, the routing.v1 campaign scripts, the replay test's lending
# operations); log-record.v1: one scenario's records (tests/scenarios:runner --log) are checked against
# log-record.v1 and logging/events.parity-cc-routing.v1.json by parity-contracts' contract-check (built
# into ~/.cache, never into parity-contracts' own target), and one call's flow is printed (--flow).
# Phase 7: the default tier builds the load toolkit (//tests/load) and runs none of it.
#   scripts/ci.sh --load   adds the load scenarios against the -c opt binary, one after another on an
#                          otherwise idle host (tests/load/run.sh: shift-start ~1 min, calls ~4 min; the
#                          targets of PLAN §7 "Load"). stepup, spike, optimizer and --ha stay manual.
# Phases 5b+5c (the learned forecaster): source_test (every fallback trigger, hysteresis, shadow,
# promotion, demotion) in all three lanes, forecaster_test (the guard, the early snapshot) plain +
# ASan, the base model and torch tests plain; a tiny training run (~1 min, 2 threads, in parallel with
# the lanes: parity-contracts' catalogue expanded, `trainer base --tiny`, its log validated); the
# default binary is built WITHOUT the forecaster (--define=forecaster=on adds it) and refuses
# --forecaster=on; the store IT's trainer_it (tenant isolation, roles, rollback, `trainer finetune`). The base training (20 min) and the OOD suite (//sim:forecast_ood)
# are evidence runs (manual).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
UA="parity-ci (generic)"
FULL=0 CHAOS=0 LOAD=0
for a in "$@"; do
  case "$a" in
    --full) FULL=1 ;;
    --chaos) CHAOS=1 ;;
    --load) LOAD=1 ;;
    *) echo "usage: scripts/ci.sh [--full] [--chaos] [--load]" >&2; exit 2 ;;
  esac
done
# 16 cores: the plain lane takes 8, each sanitizer lane 4. CI_NOCACHE=1 reruns every test (the
# worst-case timing of the default tier; otherwise Bazel's test cache skips unchanged tests).
NC=()
[[ "${CI_NOCACHE:-}" == 1 ]] && NC=(--nocache_test_results)
# CI_JOBS / CI_SAN_JOBS shrink the lanes on a shared host (default 8 and 4).
JP=(--jobs="${CI_JOBS:-8}" --local_test_jobs="${CI_JOBS:-8}" "${NC[@]}")
JS=(--jobs="${CI_SAN_JOBS:-4}" --local_test_jobs="${CI_SAN_JOBS:-4}" "${NC[@]}")
ASAN=(bazel --output_base="$HOME/.cache/bazel/parity-cc-routing-asan")
TSAN=(bazel --output_base="$HOME/.cache/bazel/parity-cc-routing-tsan")
T0=$(date +%s)
stage() { printf '\n==== %s (%d s) ====\n' "$1" $(( $(date +%s) - T0 )); }
fail() { echo "CI FAILED: $*" >&2; exit 1; }

stage "tools"
want="$(cat .bazelversion)"
have="$(bazel --version | awk '{print $2}')"
[[ "$have" == "$want" ]] || fail "bazel $have, .bazelversion says $want"
command -v curl >/dev/null && command -v python3 >/dev/null || fail "curl and python3 are needed"

stage "erlang gate (D1: Erlang-A and Erlang-C removed completely)"
dirs=()
for d in src model sim; do [[ -d "$d" ]] && dirs+=("$d"); done
if grep -rniE 'erlang|use_erlang' "${dirs[@]}"; then fail "Erlang is back in ${dirs[*]}"; fi
echo "clean: ${dirs[*]}"

stage "build"
bazel build "${JP[@]}" //:parity-cc-routing //src/... //model/... //sim/... //tests/...
# bazel-bin follows the last configuration built: keep this build's binary for the smoke run.
BIN="$(mktemp)"
cp "$ROOT/bazel-bin/parity-cc-routing" "$BIN"
# Phases 5b+5c: the binary links CPU libtorch's shared libraries (Bazel's _solib dir next to it): a
# copy finds them through LD_LIBRARY_PATH (the image: /usr/local/lib/parity-cc-routing).
# (Only a --define=forecaster=on build links them; the default binary is torch-free.)
TORCH_LIB_DIR="$(find "$(dirname "$(readlink -f "$ROOT/bazel-bin/parity-cc-routing")")/_solib_k8" -name libtorch_cpu.so -printf '%h\n' 2>/dev/null | head -1 || true)"
if [[ -n "$TORCH_LIB_DIR" ]]; then export LD_LIBRARY_PATH="$TORCH_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"; fi
chmod 700 "$BIN"
WORK="$(mktemp -d)"
LANES=()
cleanup() {
  for p in "${LANES[@]}"; do kill "$p" 2>/dev/null || true; done
  rm -rf "$WORK" "$BIN"
}
trap cleanup EXIT

stage "phase 8: log-record.v1 (a scenario's records vs the schema and RE's catalogue; one call's flow)"
CONTRACTS="$ROOT/../parity-contracts"
CC_TARGET="$HOME/.cache/bazel/parity-cc-routing-contract-check"
CARGO_TARGET_DIR="$CC_TARGET" CARGO_BUILD_JOBS=4 cargo build --quiet --offline --locked \
  --manifest-path "$CONTRACTS/rust/Cargo.toml" -p contract-check || fail "building contract-check"
CC="$CC_TARGET/debug/contract-check"
"$ROOT/bazel-bin/tests/scenarios/runner" --contracts="$CONTRACTS/VERSION" --scenario=max-wait-actions \
  --log="$WORK/re-logs.ndjson" --log-decisions=20 >"$WORK/log-scenario.log" 2>&1 || { tail -5 "$WORK/log-scenario.log"; fail "the logged scenario run"; }
"$CC" --root "$CONTRACTS" --catalogue "$ROOT/logging/events.parity-cc-routing.v1.json" --logs "$WORK/re-logs.ndjson" ||
  fail "RE's log records break log-record.v1 or its catalogue"
grep -q '"decision"' "$WORK/re-logs.ndjson" || fail "no sampled agent.selected decision in the logged run"
XSID="$(python3 -I -c 'import json,sys
for l in open(sys.argv[1]):
    j = json.loads(l)
    if j["event"] == "wrap_up.ended":
        print(j["xsid"]); break' "$WORK/re-logs.ndjson")"
[[ -n "$XSID" ]] || fail "no answered call in the logged run"
"$CC" --root "$CONTRACTS" --logs "$WORK/re-logs.ndjson" --flow "$XSID" | cut -c1-200

stage "test lanes started: plain (all tests), ASan + UBSan and TSan (core targets + scenario sets), in parallel"
# Plain: unit + property, CTMC numerics, DES, G5 scripts, canonical scenarios (also with the model's
# EWT), the fault injections, the regression replays, the 12-seed search and its shrink check.
bazel test "${JP[@]}" --test_output=errors //tests/... >"$WORK/plain.log" 2>&1 &
LANES+=($!); P_PLAIN=$!
# ASan + UBSan: the core targets (the state machine and property tests, the model, the DES, the race
# replays, the G5 scripts, the regression replays) and the canonical scenario set (CR-094).
ASAN_CORE=(//tests/forecast:source_test //tests/unit:forecaster_test //tests/unit:lending_test //tests/unit:tenant_test //tests/unit:moves_test //tests/unit:phase3_test //tests/unit:model_ewt_test //tests/unit:replay_test //tests/unit:phase5_test //tests/race:replay_order_test //tests/scripts:scripts_test
           //tests/scenarios:regressions_test //tests/model:ctmc_test //tests/sim:des_test //tests/scenarios:scenarios_test)
"${ASAN[@]}" test "${JS[@]}" --config=asan --test_output=errors "${ASAN_CORE[@]}" >"$WORK/asan.log" 2>&1 &
LANES+=($!); P_ASAN=$!
# TSan: shards, hub, gRPC streams (the race replays, the G5 scripts over both streams, the optimizer
# threads posting plans into the shards) and the canonical scenario set with the model's EWT off and on
# (CR-094).
TSAN_CORE=(//tests/forecast:source_test //tests/race:replay_order_test //tests/scripts:scripts_test //tests/unit:model_ewt_test //tests/unit:phase5_test
           //tests/scenarios:scenarios_test //tests/scenarios:scenarios_model_ewt_test)
"${TSAN[@]}" test "${JS[@]}" --config=tsan --test_output=errors --test_env=TSAN_OPTIONS=halt_on_error=1 "${TSAN_CORE[@]}" \
  >"$WORK/tsan.log" 2>&1 &
LANES+=($!); P_TSAN=$!

stage "phases 5b+5c: the tiny training started (the catalogue expanded, trainer base --tiny, ~1 min, 2 threads)"
mkdir "$WORK/fc-samples" "$WORK/fc-tiny"
python3 -I "$CONTRACTS/scripts/forecast_scenarios.py" expand --catalogue "$CONTRACTS/scenarios/forecast-training/catalogue.json" \
  --out "$WORK/fc-samples" --samples 24 --holdout 4 >/dev/null || fail "expanding the training catalogue"
nice -n 10 "$ROOT/bazel-bin/src/trainer/parity-cc-routing-trainer" base --scenarios="$WORK/fc-samples" --out="$WORK/fc-tiny" \
  --tiny --minutes=1 --threads=2 --batch=64 >"$WORK/fc-tiny/out.json" 2>"$WORK/fc-tiny/trainer.log" &
LANES+=($!); P_TRAIN=$!

stage "phase 6: the HA smoke started (tests/chaos/run.sh smoke: etcd + Redpanda, a leader and a standby, kill -9 mid-accept)"
mkdir "$WORK/chaos"
TMPDIR="$WORK/chaos" "$ROOT/tests/chaos/run.sh" smoke "$WORK/chaos/smoke" --agents=20 --rate=5 >"$WORK/chaos-smoke.log" 2>&1 &
LANES+=($!); P_HA=$!
stage "phase 6: re.state's records are a contract between releases (buf breaking, state.proto vs its baseline)"
"$ROOT/scripts/state-breaking.sh"
stage "RE-side routing-scenario.v1 files (regressions, full-scale, scaled, the generator's output): schema + cross-field"
GEN="$(mktemp -d -p "$WORK")"
"$ROOT/bazel-bin/tests/scenarios/search_test" --dump-only="$GEN" --seeds=1:300 >/dev/null
python3 -I tests/scenarios/validate.py ../parity-contracts tests/scenarios/regressions tests/scenarios/full-scale tests/scenarios/scaled --name-is-stem
python3 -I tests/scenarios/validate.py ../parity-contracts "$GEN" --name-is-stem

stage "smoke: the binary (health files, /livez /readyz /metrics, incarnation, SIGTERM drain, refusals)"
mkdir "$WORK/state" "$WORK/health"
ports="$(python3 -I -c 'import socket
s=[socket.socket() for _ in range(2)]
[x.bind(("127.0.0.1",0)) for x in s]
print(" ".join(str(x.getsockname()[1]) for x in s))')"
read -r GRPC OPS <<<"$ports"
# The page is fetched whole, then searched: `curl | grep -q` under pipefail fails (or, negated,
# passes) whenever grep exits before curl has written everything (SIGPIPE).
has_metric() {
  local page
  page="$(curl -s "http://127.0.0.1:$OPS/metrics")"
  grep -q "$1" <<<"$page"
}
run() {
  "$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" \
    --health-dir="$WORK/health" --listen="127.0.0.1:$GRPC" --ops-listen="127.0.0.1:$OPS" --shards=2 \
    "$@" 2>>"$WORK/log" &
  PID=$!
  for _ in $(seq 1 100); do [[ -f "$WORK/health/parity-cc-routing.healthy" ]] && return 0; sleep 0.1; done
  fail "never healthy: $(tail -5 "$WORK/log")"
}
# CR-059: an uninitialised state dir is refused without --first-start=yes.
set +e
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" \
  --listen=127.0.0.1:0 --ops-listen=127.0.0.1:0 >/dev/null 2>"$WORK/fresh.log"; rc0=$?
set -e
[[ $rc0 == 2 ]] && grep -q 'no incarnation' "$WORK/fresh.log" || fail "a fresh state dir without --first-start: exit $rc0"
run --first-start=yes
[[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$OPS/livez")" == 200 ]] || fail "/livez"
# CR-059: a second process on the same state dir is refused while the first holds its lock.
set +e
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" \
  --listen=127.0.0.1:0 --ops-listen=127.0.0.1:0 >/dev/null 2>"$WORK/second.log"; rc5=$?
set -e
[[ $rc5 == 2 ]] && grep -q 'another parity-cc-routing holds this state dir' "$WORK/second.log" || fail "a second instance on one state dir: exit $rc5"
# CR-060: a client dribbling a byte every 0.5 s can't hold the ops loop: /livez answers in time.
python3 -I - "$OPS" <<'DRIBBLE' &
import socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
try:
    for c in b"GET /metrics HTTP/1.1\r\nX-Slow: " + b"a" * 40:
        s.send(bytes([c])); time.sleep(0.5)
except OSError:
    pass
DRIBBLE
DRIBBLER=$!
sleep 0.3
t0=$(date +%s%N)
[[ "$(curl -s --max-time 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$OPS/livez")" == 200 ]] || fail "/livez blocked by a slow client"
ms=$(( ($(date +%s%N) - t0) / 1000000 ))
kill "$DRIBBLER" 2>/dev/null; wait "$DRIBBLER" 2>/dev/null || true
(( ms < 3000 )) || fail "/livez took $ms ms behind a slow client (budget 2 s)"
echo "ops http: /livez in $ms ms behind a dribbling client"
[[ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$OPS/readyz")" == 200 ]] || fail "/readyz"
has_metric '^parity_cc_routing_incarnation 1$' || fail "incarnation 1"
grep -q '"msg":"phase-1 RE started: restart interaction-engine and parity-gateway".*"incarnation":1[,}]' "$WORK/log" || fail "restart-rule log line"
kill -TERM "$PID"; wait "$PID" || fail "exit code $? after SIGTERM"
[[ -f "$WORK/health/parity-cc-routing.drained" && ! -f "$WORK/health/parity-cc-routing.healthy" ]] || fail "drain files"
run
has_metric '^parity_cc_routing_incarnation 2$' || fail "the incarnation must rise across restarts"
has_metric '^parity_cc_routing_model_solves_total' && fail "the model runs with --model-ewt off"
kill -TERM "$PID"; wait "$PID"
# Phase 2: --model-ewt=on starts the optimization threads (their metrics appear); a bad value is refused.
run --model-ewt=on
has_metric '^parity_cc_routing_model_solves_total' || fail "--model-ewt=on: no model metrics"
has_metric '^parity_cc_routing_ewt_heuristic_total' || fail "no EWT source metrics"
kill -TERM "$PID"; wait "$PID"
# Phase 5: --optimizer=on runs the lever loop (its metrics appear); a bad value is refused below.
run --optimizer=on
has_metric '^parity_cc_routing_lever_plans_total' || fail "--optimizer=on: no lever metrics"
has_metric '^parity_cc_routing_levers_installed_total' || fail "--optimizer=on: no writer lever metrics"
kill -TERM "$PID"; wait "$PID"
# Phases 5b+5c: the default binary is built without the learned forecaster (BUILD.bazel,
# --define=forecaster=on): --forecaster=on is refused below and nothing of it runs (the phase-8 engine).
if grep -iE '\+[0-9]{7,}|password|secret' "$WORK/log"; then fail "the log holds a number or a secret"; fi
# Phase 8: every line the binary wrote is a log-record.v1 record of its catalogue (or the common one).
"$CC" --root "$CONTRACTS" --catalogue "$ROOT/logging/events.parity-cc-routing.v1.json" --logs "$WORK/log" ||
  fail "the binary's log breaks log-record.v1 or its catalogue"
set +e
"$BIN" --env=prod --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" >/dev/null 2>&1; rc1=$?
"$BIN" --env=dev --bin=bin-a --state-dir="$WORK/state" --test-control=127.0.0.1:0 >/dev/null 2>&1; rc2=$?
python3 -I -c 'import json,sys; d=json.load(open(sys.argv[1])); del d["tenants"][0]["queues"][0]["max_wait_s"]; json.dump(d,open(sys.argv[2],"w"))' \
  "$ROOT/seed/local-dev.json" "$WORK/bad-seed.json"
"$BIN" --env=local-dev --bin=bin-a --seed="$WORK/bad-seed.json" --state-dir="$WORK/state" >/dev/null 2>&1; rc3=$?
# CR-057: a max-wait QUEUE chain that loops (sales -> support -> sales) is refused.
python3 -I -c 'import json,sys; d=json.load(open(sys.argv[1])); q=d["tenants"][0]["queues"]
q[0]["max_wait_action"]={"action":"queue","queue_id":q[1]["id"]}; q[1]["max_wait_action"]={"action":"queue","queue_id":q[0]["id"]}
json.dump(d,open(sys.argv[2],"w"))' "$ROOT/seed/local-dev.json" "$WORK/cycle-seed.json"
"$BIN" --env=local-dev --bin=bin-a --seed="$WORK/cycle-seed.json" --state-dir="$WORK/state" 2>"$WORK/cycle.log" >/dev/null; rc4=$?
grep -q 'never ends' "$WORK/cycle.log" || rc4=99
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" --model-ewt=maybe >/dev/null 2>&1; rc6=$?
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" --optimizer=maybe >/dev/null 2>&1; rc11=$?
# Phases 5b+5c: --forecaster=on in a binary built without it; a bad --forecaster value.
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" --optimizer=on --forecaster=on \
  --forecast-model-dir="$ROOT/model/forecast" >/dev/null 2>"$WORK/fc-off.log"; rc12=$?
grep -q 'built without the forecaster' "$WORK/fc-off.log" || rc12=99
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" --forecaster=maybe >/dev/null 2>&1; rc13=$?
# Phase 3: a missing Postgres conninfo file is refused (the store's secret is read from a file only).
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --state-dir="$WORK/state" --pg-conninfo-file="$WORK/none" >/dev/null 2>"$WORK/pg.log"; rc7=$?
grep -q 'pg-conninfo-file is unreadable' "$WORK/pg.log" || rc7=99
set -e
# Phase 6: --ha=on needs etcd and Kafka; an unknown --ha value and --test-control with HA are refused.
set +e
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --ha=on >/dev/null 2>"$WORK/ha.log"; rc8=$?
grep -q 'needs --etcd and --kafka-brokers' "$WORK/ha.log" || rc8=99
"$BIN" --env=local-dev --bin=bin-a --seed="$ROOT/seed/local-dev.json" --ha=refused-without-PARITY_RE_HA >/dev/null 2>&1; rc9=$?
"$BIN" --env=local-dev --bin=bin-a --ha=on --etcd=127.0.0.1:1 --kafka-brokers=127.0.0.1:1 --test-control=127.0.0.1:0 >/dev/null 2>&1; rc10=$?
set -e
[[ $rc8 == 2 && $rc9 == 2 && $rc10 == 2 ]] || fail "HA refusals: no etcd/kafka=$rc8 bad --ha=$rc9 ha+test-control=$rc10 (want 2)"
[[ $rc1 == 2 && $rc2 == 2 && $rc3 == 2 && $rc4 == 2 && $rc6 == 2 && $rc7 == 2 && $rc11 == 2 && $rc12 == 2 && $rc13 == 2 ]] || fail "refusals: prod=$rc1 test-control-outside-local=$rc2 seed-without-max-wait=$rc3 max-wait-cycle=$rc4 model-ewt=maybe:$rc6 pg-conninfo-missing:$rc7 optimizer=maybe:$rc11 forecaster-not-built:$rc12 forecaster=maybe:$rc13 (want 2)"
echo "smoke ok: healthy, metrics, incarnation 1 -> 2 -> 3, --model-ewt on/off, drained, refusals"

stage "dependencies: newest BCR release at least 14 days old (unless an advisory says otherwise)"
python3 -I - "$ROOT/MODULE.bazel" "$UA" <<'PY'
import json, re, sys, urllib.request, datetime
module, ua = sys.argv[1], sys.argv[2]
deps = re.findall(r'bazel_dep\(name = "([^"]+)", version = "([^"]+)"', open(module).read())
deps += re.findall(r'single_version_override\(module_name = "([^"]+)", version = "([^"]+)"', open(module).read())
now = datetime.datetime.now(datetime.timezone.utc)
bad = []
# Pins below the newest >= 14-day release, each with its reason (kept short; PROGRESS.md explains).
held = {"protobuf": "grpc 1.84.0 asks for 35.1", "googletest": "1.18 needs abseil 20260526 (compat 0), grpc 1.84 can't take it",
        "rules_swift": "Bazel 9's bazel_tools needs 3.x", "eigen": "5.0.1.bcr.3 (2026-10-26) is under 14 days old at phase 2"}
# postgres 18.4 is BCR's newest (only libpq is built: CR-106 tracks the server-side CVEs fixed in 18.6).
def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    return json.load(urllib.request.urlopen(req, timeout=20))
for name, ver in deps:
    c = get(f"https://api.github.com/repos/bazelbuild/bazel-central-registry/commits?path=modules/{name}/{ver}&per_page=1")
    when = datetime.datetime.fromisoformat(c[0]["commit"]["committer"]["date"].replace("Z", "+00:00"))
    age = (now - when).days
    meta = get(f"https://bcr.bazel.build/modules/{name}/metadata.json")
    newer = [v for v in meta["versions"] if "-pre" not in v and "rc" not in v][-1]
    note = "" if newer == ver else f" (newest {newer}{'; ' + held[name] if name in held else ''})"
    print(f"{name} {ver}: {age} days{note}")
    if age < 14:
        bad.append(f"{name} {ver} is {age} days old")
if bad:
    sys.exit("too new: " + "; ".join(bad))
PY

stage "OSV (grpc and protobuf through their PyPI/Maven twins of the same cores)"
python3 -I - "$UA" <<'PY'
import json, sys, urllib.request
ua = sys.argv[1]
queries = [("PyPI", "grpcio", "1.84.0"), ("Maven", "com.google.protobuf:protobuf-java", "4.35.1"),
           ("GIT", "https://gitlab.com/libeigen/eigen", "5.0.1")]
found = []
# Phase 3: libpq (postgres REL_18_4) and librdkafka v2.15.0 by their tags' commits.
for name, commit in [("postgres REL_18_4", "f5cc81719e6da4cbdb1f797c48b693e91018153a"),
                     ("librdkafka v2.15.0", "9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab")]:
    req = urllib.request.Request("https://api.osv.dev/v1/query", data=json.dumps({"commit": commit}).encode(),
                                 headers={"User-Agent": ua, "Content-Type": "application/json"})
    vulns = json.load(urllib.request.urlopen(req, timeout=20)).get("vulns", [])
    print(f"GIT {name} ({commit[:7]}): {len(vulns)} advisories")
    found += [f"{name}: {v['id']}" for v in vulns]
# Eigen 5.0.1 (BCR 5.0.1.bcr.2) by its tag's commit as well (OSV's GIT ranges).
req = urllib.request.Request("https://api.osv.dev/v1/query", data=json.dumps({"commit": "bc3b39870ecb690a623a3f49149a358b95c5781d"}).encode(),
                             headers={"User-Agent": ua, "Content-Type": "application/json"})
vulns = json.load(urllib.request.urlopen(req, timeout=20)).get("vulns", [])
print(f"GIT eigen commit bc3b398 (5.0.1): {len(vulns)} advisories")
found += [f"eigen: {v['id']}" for v in vulns]
for eco, name, ver in queries:
    body = json.dumps({"package": {"ecosystem": eco, "name": name}, "version": ver}).encode()
    req = urllib.request.Request("https://api.osv.dev/v1/query", data=body, headers={"User-Agent": ua, "Content-Type": "application/json"})
    vulns = json.load(urllib.request.urlopen(req, timeout=20)).get("vulns", [])
    print(f"{eco} {name} {ver}: {len(vulns)} advisories")
    found += [f"{name}: {v['id']}" for v in vulns]
if found:
    sys.exit("OSV: " + ", ".join(found))
PY

stage "waiting for the test lanes"
lane() {  # name pid log
  if wait "$2"; then
    grep -E '^(//| *Executed|INFO: Elapsed)' "$3" | tail -40
  else
    tail -60 "$3"
    fail "the $1 test lane"
  fi
}
lane plain "$P_PLAIN" "$WORK/plain.log"
grep -E '^scripts:' bazel-testlogs/tests/scripts/scripts_test/test.log
grep -E '^scenarios:' bazel-testlogs/tests/scenarios/scenarios_test/test.log
grep -E '^property:' bazel-testlogs/tests/unit/tenant_test/test.log
grep -E '^race:' bazel-testlogs/tests/race/replay_order_test/test.log
grep -E '^golden:' bazel-testlogs/tests/model/ctmc_test/test.log
grep -E '^des smoke:' bazel-testlogs/tests/sim/des_test/test.log
grep -E '^scenarios:' bazel-testlogs/tests/scenarios/scenarios_model_ewt_test/test.log
grep -E '^(determinism|search:)' bazel-testlogs/tests/scenarios/search_test/test.log
grep -E '^scenarios:' bazel-testlogs/tests/scenarios/regressions_test/test.log
grep -E '^replay:' bazel-testlogs/tests/unit/replay_test/test.log
lane "ASan + UBSan" "$P_ASAN" "$WORK/asan.log"
lane TSan "$P_TSAN" "$WORK/tsan.log"
stage "phases 5b+5c: the tiny training (a base model on 20 samples, holdout WAPE vs the baseline, its log)"
wait "$P_TRAIN" || { tail -5 "$WORK/fc-tiny/trainer.log"; fail "the tiny training run"; }
python3 -I - "$WORK/fc-tiny/base.json" "$WORK/fc-tiny/base.pt" <<'PY' || fail "the tiny training's model"
import hashlib, json, sys
m = json.load(open(sys.argv[1]))
b = open(sys.argv[2], "rb").read()
assert m["sha256"] == hashlib.sha256(b).hexdigest(), "sha256"
assert m["training"]["steps"] > 0 and m["holdout"]["anchors"] > 0, "no steps or holdout"
print("tiny training: %d steps in %.0f s, val loss %.4f, holdout H1 WAPE model %.3f vs baseline %.3f, %d bytes" % (
    m["training"]["steps"], m["training"]["total_s"], m["training"]["val_loss"], m["holdout"]["h1_wape_model"],
    m["holdout"]["h1_wape_baseline"], len(b)))
PY
"$CC" --root "$CONTRACTS" --catalogue "$ROOT/logging/events.parity-cc-routing.v1.json" --logs "$WORK/fc-tiny/trainer.log" ||
  fail "the trainer's log breaks log-record.v1 or its catalogue"
stage "phase 6: the HA smoke"
if wait "$P_HA"; then
  grep -E '^\{"scenario"|^violations' "$WORK/chaos-smoke.log"
else
  tail -30 "$WORK/chaos-smoke.log"
  fail "the HA smoke (tests/chaos/run.sh smoke)"
fi
LANES=()

stage "phase 3: the Postgres store (a throwaway postgres:17-alpine on a random port; restart, duplicate RunCallback, RLS, Postgres down)"
"$ROOT/scripts/store-it.sh" "${JP[@]}" >"$WORK/store.log" 2>&1 || { tail -40 "$WORK/store.log"; fail "the store tests"; }
for it in pg_store_it trainer_it; do
  grep -E '^\[ +(OK|FAILED|SKIPPED) +\]' "bazel-testlogs/tests/store/$it/test.log"
  grep -q 'SKIPPED' "bazel-testlogs/tests/store/$it/test.log" && fail "a store test skipped (no Postgres?): $it"
done


if (( FULL )); then
  stage "full tier: the 40-seed search, and the scaled spike and shift-start runs (-c opt: I9, I10, KPI bounds)"
  OPT=(bazel --output_base="$HOME/.cache/bazel/parity-cc-routing-opt")
  bazel test "${JP[@]}" --test_output=errors //tests/scenarios:search_full_test || fail "full tier: the 40-seed search"
  # Alone: I9 is a latency bound, judged on a quiet host.
  "${OPT[@]}" test -c opt --symlink_prefix=/ "${JS[@]}" --test_output=errors //tests/scenarios:scaled_test ||
    fail "full tier: the scaled runs"
  grep -E '^(determinism|search:)' bazel-testlogs/tests/scenarios/search_full_test/test.log
  grep -E '^(PASS|FAIL|scenarios:)' "$("${OPT[@]}" info -c opt bazel-testlogs)/tests/scenarios/scaled_test/test.log" | cut -c1-300
  stage "full tier: a 10-seed search under ASan + UBSan"
  "${ASAN[@]}" test "${JS[@]}" --config=asan --test_output=errors //tests/scenarios:search_test \
    --test_arg=--seeds=1:10 --test_arg=--determinism=2 || fail "full tier: ASan search"
fi

if (( CHAOS )); then
  for sc in kill9 sigstop etcdcut restart move steady; do
    stage "chaos tier: $sc (<= 5 min)"
    extra=(); [[ $sc == steady ]] && extra=(--rate=5)
    TMPDIR="$WORK/chaos" "$ROOT/tests/chaos/run.sh" "$sc" "$WORK/chaos/$sc" "${extra[@]}" >"$WORK/chaos-$sc.log" 2>&1 ||
      { tail -30 "$WORK/chaos-$sc.log"; fail "chaos tier: $sc"; }
    grep -E '^\{"scenario"' "$WORK/chaos-$sc.log"
  done
fi

if (( LOAD )); then
  stage "load tier: building the -c opt binary and the load driver"
  OPT=(bazel --output_base="$HOME/.cache/bazel/parity-cc-routing-opt")
  "${OPT[@]}" build -c opt --symlink_prefix=/ "${JP[@]}" //:parity-cc-routing //tests/load:load_driver || fail "load tier: build"
  for sc in shift-start calls; do
    stage "load tier: $sc (<= 5 min; host load $(cut -d' ' -f1 /proc/loadavg))"
    mkdir -p "$WORK/load"
    TMPDIR="$WORK/load" "$ROOT/tests/load/run.sh" "$sc" "$WORK/load/$sc" >"$WORK/load-$sc.log" 2>&1 ||
      { tail -30 "$WORK/load-$sc.log"; fail "load tier: $sc"; }
    grep -E '^\{"scenario"' "$WORK/load-$sc.log" | cut -c1-400
  done
fi

stage "CI PASSED$( (( FULL )) && echo ' (full tier)')$( (( CHAOS )) && echo ' (chaos tier)')$( (( LOAD )) && echo ' (load tier)')"
