#!/usr/bin/env bash
# parity-cc-routing M1 end-to-end (scratch; nothing in IE, the gateway or parity-deploy is edited):
# parity-deploy's compose (media, core, app, tools) + compose.e2e.yml (RE, IE with [routing] on and
# a queue flow, a caller that queues), the real gateway (its jar, profile dev) and a simulated agent
# desktop (agent.mjs) for acme/agent-a1.
set -euo pipefail
E=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
R=/home/cristifg/Repos/parity-contact-center
DEPLOY=$R/parity-deploy
OUT=$E/run-$(date +%H%M%S)
mkdir "$OUT"
COMPOSE=(docker compose -f "$DEPLOY/compose/compose.yml" -f "$E/compose.e2e.yml" --profile media --profile core --profile app --profile tools)
export PARITY_UID PARITY_GID SIPP_IID IE_FLOWS_FILE IE_TENANTS_FILE SIPP_SCENARIO=e2e-call.xml
# RE's ports (defaults 9800/9810): compose.e2e.yml reads them; ie.toml's [routing] endpoint and the
# gateway's RE_PORT follow.
export RE_GRPC_PORT=${RE_GRPC_PORT:-9800} RE_OPS_PORT=${RE_OPS_PORT:-9810}
python3 -I - "$E/ie.toml" "$RE_GRPC_PORT" <<'TOML'
import re, sys
p, port = sys.argv[1], sys.argv[2]
s = open(p).read()
head, sep, tail = s.partition("[routing]")
tail = re.sub(r'endpoint = "http://127\.0\.0\.1:\d+"', f'endpoint = "http://127.0.0.1:{port}"', tail, count=1)
open(p, "w").write(head + sep + tail)
TOML
PARITY_UID=$(id -u); PARITY_GID=$(id -g)
SIPP_IID=$(python3 -I -c 'import uuid; print(uuid.uuid4())')
IE_FLOWS_FILE=$E/flows.json
IE_TENANTS_FILE=$R/parity-contracts/tenants/test-tenants.v1.json
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }
"$DEPLOY/scripts/preflight.sh" --wait 900
rm -rf "$DEPLOY/compose/.run" && "$DEPLOY/scripts/preflight.sh" >/dev/null
python3 -I "$DEPLOY/scripts/demo_media.py" gen "$DEPLOY/compose/.run"
chmod -R a+rX "$DEPLOY/compose/.run/prompts" "$DEPLOY/compose/.run/media"
rm -rf "$E/re-state" && mkdir "$E/re-state"
GW_PID=; AGENT_PID=
down() {
  set +e
  log "logs and teardown"
  [[ -n "$AGENT_PID" ]] && kill "$AGENT_PID" 2>/dev/null
  [[ -n "$GW_PID" ]] && { kill -TERM "$GW_PID"; wait "$GW_PID"; }
  curl -s "http://127.0.0.1:$RE_OPS_PORT/metrics" >"$OUT/re-metrics.txt" 2>/dev/null
  for s in etcd freeswitch parity-ms telephony-controller postgres parity-cc-routing interaction-engine sipp; do
    "${COMPOSE[@]}" logs --no-color --no-log-prefix "$s" >"$OUT/$s.log" 2>&1
  done
  "${COMPOSE[@]}" down --remove-orphans --timeout 20 >>"$OUT/run.log" 2>&1
  cp "$DEPLOY/compose/.run"/sipp/* "$OUT/" 2>/dev/null
  log "remaining parity-dev containers: $(docker ps -a --filter label=com.docker.compose.project=parity-dev -q | wc -l)"
}
trap down EXIT
log "interaction id $SIPP_IID"
log "up: media + core + RE + IE"
# Not --wait: the freeswitch image's own HEALTHCHECK fails with TC's config (p1-run.sh doesn't wait on it either).
"${COMPOSE[@]}" up -d etcd freeswitch parity-ms telephony-controller postgres parity-cc-routing interaction-engine >>"$OUT/run.log" 2>&1
for _ in $(seq 1 180); do
  [[ "$(docker inspect -f '{{.State.Health.Status}}' "$("${COMPOSE[@]}" ps -q interaction-engine)" 2>/dev/null)" == healthy ]] && break
  sleep 1
done
log "RE: $(docker inspect -f '{{.State.Health.Status}}' "$("${COMPOSE[@]}" ps -q parity-cc-routing)"), IE: $(docker inspect -f '{{.State.Health.Status}}' "$("${COMPOSE[@]}" ps -q interaction-engine)")"
log "gateway (jar, profile dev) -> RE 127.0.0.1:$RE_GRPC_PORT"
rm -f "$E/gw/gw.healthy" "$E/gw/gw.drained"
DEV_SIP_PASSWORDS_FILE=$R/secrets/local-dev/sip-agents.properties RE_HOST=127.0.0.1 RE_PORT=$RE_GRPC_PORT \
  EVENTS_ENABLED=false PARITY_BIN=bin-a \
  java -jar "$R/parity-gateway/target/parity-gateway-0.1.0.jar" --spring.profiles.active=dev \
    --server.port=18090 --management.server.port=18091 --parity.gateway.instance=gw-e2e-1 \
    --parity.gateway.lifecycle.healthy-file="$E/gw/gw.healthy" --parity.gateway.lifecycle.drained-file="$E/gw/gw.drained" \
    --parity.gateway.agents.auth.jwks-file="$E/gw/jwks.json" --parity.gateway.agents.auth.issuer=parity-gateway-dev \
    --parity.gateway.agents.sip.key-file="$E/gw/unused-in-dev" --parity.gateway.agents.turn.urls= \
    --parity.gateway.agents.turn.secret-file="$R/secrets/local-dev/cr-e2e-turn.secret" >"$OUT/gateway.log" 2>&1 &
GW_PID=$!
for _ in $(seq 1 120); do [[ -f "$E/gw/gw.healthy" ]] && break; sleep 0.5; done
[[ -f "$E/gw/gw.healthy" ]] || { log "gateway never healthy"; exit 1; }
log "agent desktop (acme/agent-a1)"
node "$E/agent.mjs" run "$R/secrets/local-dev/gateway-dev-jwt.jwk.json" 18090 "$OUT/agent-timeline.jsonl" >"$OUT/agent.log" 2>&1 &
AGENT_PID=$!
for _ in $(seq 1 60); do grep -q '"state":"ready"' "$OUT/agent.log" 2>/dev/null && break; sleep 0.5; done
log "agent: $(grep -c recv "$OUT/agent.log") frames so far; the caller (SIPp) dials +15550100100"
"${COMPOSE[@]}" up -d pcap sipp >>"$OUT/run.log" 2>&1
rc=$(docker wait "$("${COMPOSE[@]}" ps -a -q sipp)")
log "SIPp exit $rc"
set +e
wait "$AGENT_PID"; arc=$?
AGENT_PID=
log "agent exit $arc"
exit $(( rc != 0 || arc != 0 ))
