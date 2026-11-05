# parity-cc-routing

The routing engine (the role and APIs keep the name "routing-engine": `parity.routing.v1` for
interaction-engine, `parity.agents.v1` for parity-gateway). C++23, Bazel 9 (bzlmod), gRPC. Plan:
`PLAN.md`; progress and evidence: `PROGRESS.md`; bugs: `../BUGS.md` (prefix `CR-`).

## Phase 1: single instance, state in memory

**Restart rule (PLAN §8):** a phase-1 restart loses every queue entry, offer, reservation and agent
state. **Restarting parity-cc-routing means restarting interaction-engine and parity-gateway too.**
The engine logs `phase-1 RE started (incarnation N): restart interaction-engine and parity-gateway`.
A monotonic incarnation counter in `--state-dir` (bumped and fsynced before the port opens) makes
`leader_epoch` = N and starts `attach_generation`, every `delivery_seq` and `state_version` at
(N − 1) × 2³², so nothing is ever reused. Only `local-*` environments; one replica (a StatefulSet
with `replicas: 1` or a Deployment with `strategy: Recreate`; locally one compose service).

```
bazel build //:parity-cc-routing
bazel-bin/parity-cc-routing --env=local-dev --bin=bin-a --seed=seed/local-dev.json \
  --state-dir=<dir that survives restarts> [--first-start=yes] [--listen=127.0.0.1:9800] [--ops-listen=127.0.0.1:9810]
```

**Single instance (CR-059):** RE holds a lock on `<state-dir>/lock` for its life (a second process
on the same state dir exits 2), and refuses a state dir without an incarnation unless
`--first-start=yes` (a new deployment, with IE and the gateway fresh), so an emptied volume can't
silently reuse epochs. In compose: parity-deploy's `parity-cc-routing` service (profile `app`;
`scripts/restart-local.sh` restarts it with IE and the gateway); in Kubernetes:
`parity-deploy/helm/parity-cc-routing` (one replica, a StatefulSet or a `Recreate` Deployment).
The build pins parity-contracts (MODULE.bazel, `version = "0.11.1"`; CR-063).

Both listeners default to loopback (CR-058): phase 1 has no mTLS, so widening them takes an explicit
address.

| Port | What |
|---|---|
| 9800 | gRPC `RoutingService` + `AgentService` (mTLS from phase 6) |
| 9810 | `/livez`, `/readyz`, `/metrics` (`parity_cc_routing_*`) |
| 9811 | `--test-control=127.0.0.1:9811`: `/_test/v1/{reset,seed,advance,inject,sync,restart,invariants}`, a manual clock; refused outside `local-*` |

Health files `/tmp/parity-cc-routing.{healthy,drained}` (`--health-dir`). `SIGTERM`: not healthy,
no new offers, a second for acks in flight, close the streams, `drained`, exit 0.

**Config:** `seed/local-dev.json` (`seed.v1`, schema `seed/seed.v1.schema.json`): the shared test
tenants of parity-contracts, each in its bin, two queues, the shared agent ids, and (phase 4) each
tenant's `routing` (config.routing) and `routing_pins` (config.routing_pin) until admin-console
publishes them. A queue without `max_wait_s` and `max_wait_action` is refused. Unknown queues get
`queue_unknown`, unknown agents `agent_unknown`.

**Phase 3 (PROGRESS.md):** callbacks and voicemail tasks live in Postgres:
`--pg-conninfo-file=<file>` (a libpq conninfo; a secret, never logged); without it they are refused
(`store_unavailable`). Schema: `db/V1__routing_store.sql`, applied by the owner role (row-level
security; the service role has no DDL). A local Postgres for tests: `scripts/local-postgres.sh up
<dir>` (its password in `../secrets/local-dev/cc-routing-postgres.password`, made on first use);
`scripts/store-it.sh` runs the store tests against one. `--kafka-brokers=<host:port>` publishes
`agent.events` (plaintext: CR-110).

## Phase 6: a leader and a standby per bin (`--ha=on`)

`--ha=on --etcd=<host:port> --kafka-brokers=<host:port> [--state-topic=re.state] [--advertise=<this
instance's gRPC host:port, default --listen>] [--lease-ttl-s=5]` (PLAN §5.5, PROGRESS.md). Every change
is a record in the compacted topic `re.state` (key `{tenant}/{kind}/{id}`, `src/state/state.proto`)
committed in a Kafka transaction before anything it caused leaves the process (Watch messages, RPC
answers, agent.events, Postgres writes). Instances of a bin elect a leader through etcd
(`re/{bin}/leader` = the leader's `--advertise`, lease TTL 5 s, keep-alive 1 s; `leader_epoch` = the
key's create revision); a standby follows `re.state` and answers every RPC `UNAVAILABLE`/`not_leader`
with `parity-leader`. A new leader fences the old one's producer (`transactional.id
parity-cc-routing.<bin>`), reads the topic to its end, rebuilds every owned tenant (timers from their
absolute deadlines) and serves; then raises each tenant's Postgres fence (`db/V2__routing_fence.sql`)
and reconciles its callbacks. A restart is the same path from an empty memory. **Stepping down exits
with code 3** (no keep-alive answer within TTL − 1.5 s, a fenced or failed commit, the key gone): run it
under a supervisor that restarts it (`restart: on-failure`), and it comes back as the standby. Tenant
moves follow `tp/tenants/{tenant}` = `{"bin": ..., "epoch": N}` (a handover marker in `re.state`).
`--state-dir` is unused with `--ha=on`; `--first-start=yes` allows an empty `re.state` for the bin.
`/readyz` is the serving leader only; the health file is a leader or a caught-up standby.

Every port is a flag (`--listen`, `--ops-listen`, `--test-control`, `--advertise`; 9800/9810/9811 are
only defaults), so several instances share a host. Local etcd + Redpanda for tests:
`scripts/local-ha.sh up|down <dir>` (free ports, or `PCR_ETCD_PORT`/`PCR_KAFKA_PORT`); the chaos
scenarios: `tests/chaos/run.sh kill9|sigstop|etcdcut|restart|move|steady|smoke <outdir>` (build
`//:parity-cc-routing //tests/chaos:all` first). In parity-deploy: `PARITY_RE_HA=on
PARITY_RE_RESTART=on-failure` and the `app-ha` profile (ports `PARITY_RE_GRPC_PORT`, `PARITY_RE_OPS_PORT`,
`PARITY_RE_STANDBY_*`), Helm `ha: true` with `replicas: 2`; not runnable against the compose Redpanda
until RE speaks SASL (CR-110, CR-136). Local environments only until the BEFORE DEPLOY rows close.

## Phase 7: load (opt-in)

`tests/load/run.sh` drives the `-c opt` binary over gRPC in real time (PLAN "Phase 7 as built (cut
short)", PROGRESS "Capacity"). Build first, run one scenario at a time on an otherwise idle host (each
<= 5 minutes); `<outdir>/report.json` holds the checks, RE's RSS and CPU, and per-window numbers:

    bazel --output_base=$HOME/.cache/bazel/parity-cc-routing-opt build -c opt --symlink_prefix=/ \
      //:parity-cc-routing //tests/load:load_driver
    tests/load/run.sh shift-start <outdir>          # 10k agents at 500/s while calls flow
    tests/load/run.sh calls <outdir>                # 10k agents, 5k concurrent calls, bursts of 50/s
    tests/load/run.sh stepup <outdir>               # the knee: attach and call rates stepped up
    tests/load/run.sh calls <outdir> --ha           # + etcd and Redpanda (scripts/local-ha.sh); not yet run
    tests/load/run.sh spike|optimizer <outdir>      # written, not yet run (CR-090, CR-194)

Ports are free loopback ports unless `PCR_LOAD_GRPC_PORT` / `PCR_LOAD_OPS_PORT` are set.
`scripts/ci.sh --load` runs shift-start and calls after the default tier.

## Tests

`scripts/ci.sh` runs everything: the Erlang gate, the build, `//tests/unit` (unit, property, the
masking cases), `//tests/race` (CR-053: replays are never overtaken across reconnects),
`//tests/scripts` (parity-contracts' executable routing.v1 and agents.v1 scripts: 44 since 0.11.0,
`skip.txt` empty), `//tests/unit:moves_test` (phase 4) and `:phase3_test`, the Postgres store
(`scripts/store-it.sh`), `//tests/scenarios` (the canonical
`routing-scenario.v1` files and the CR-055 variants with the invariant checker I1–I11, per-invariant
exercise counts, and two injected-bug runs that must be caught), ASan/UBSan, TSan, a smoke run of the
binary (health, metrics, incarnation, lock, first start, drain, refusals, the ops HTTP budget),
dependency ages and OSV; phase 6 adds `//tests/unit:replay_test` (replaying `re.state` into an empty
memory gives the same state), `scripts/state-breaking.sh` (`buf breaking` of `state.proto` against
`src/state/baseline/`) and an HA smoke (`tests/chaos/run.sh smoke`); `scripts/ci.sh --chaos` runs every
chaos scenario. Play one scenario: `bazel run //tests/scenarios:scenarios_test --
--contracts=<parity-contracts> --scenario=<name>` (`--fault=<kind>@<s> --expect-violation=<I>` injects a bug).
The end-to-end (compose: RE, IE, the gateway, the media node, SIPp and a simulated desktop) is
`parity-deploy/scripts/re-e2e-run.sh` since M2; `tests/e2e/` is M1's override, kept as its record.

`attic/` and `parked/nnmpc/` hold PoC sources kept as references only (never built): the NNMPC and
its transformer are rewritten in phases 5b/5c.
