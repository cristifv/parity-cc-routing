# M1 end-to-end (2026-10-28; superseded in M2 by `parity-deploy/scripts/re-e2e-run.sh`)

Run from a scratch directory (nothing in IE, the gateway or parity-deploy is edited): copy this
directory there, put the RE binary at `<e2e dir>/parity-cc-routing` (`bazel build //:parity-cc-routing`),
replace `<e2e dir>` in `compose.e2e.yml`, make `gw/` (700) with `jwks.json` from
`node agent.mjs jwks ../../secrets/local-dev/gateway-dev-jwt.jwk.json gw/jwks.json`, then `./run-e2e.sh`.

- parity-deploy's compose (`media`, `core`, `app`, `tools`) plus `compose.e2e.yml`: the RE service
  (Fedora 43 by digest, the binary mounted, a state volume; one replica), IE
  `localhost/parity-interaction-engine:cr-e2e` (built from IE's current sources, step 5a) with
  `ie.toml` (`[routing] enabled = true`, `endpoint = "http://127.0.0.1:9800"`), `flows.json` (IE's demo
  flows with `+15550100100` → say, `{"dial": {"queue": "sales"}}`, hangup), and SIPp's
  `e2e-call.xml` (a caller that queues and hangs up after 30 s). `preflight.sh --wait` first.
- The real gateway (`parity-gateway-0.1.0.jar`, profile `dev`, `RE_HOST=127.0.0.1 RE_PORT=9800`,
  events off, a local-dev TURN secret file) on 18090/18091 as a host process.
- `agent.mjs`: acme/agent-a1's desktop over agent-ws.v1 (a dev token signed with the local-dev key,
  never printed): auth, ready, accept the offer, `wrap_up_done`, READY again.

Evidence of the passing run: `../../evidence/m1-e2e-2026-10-28/` (`run.log`, `agent-timeline.jsonl`,
`re-metrics.txt`, every service's log; scanned: no secret value, no JWT, no private key in any file).
