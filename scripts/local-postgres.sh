#!/usr/bin/env bash
# A throwaway Postgres for parity-cc-routing's store tests (PLAN §8 row 3): postgres:17-alpine on a
# random loopback port (PCR_PG_PORT picks one; no fixed port, so no e2e lock), db/V*.sql applied in order by the
# owner, the service's login role parity_cc_routing (a member of parity_cc_routing_app, no DDL).
# Its password lives in secrets/local-dev/cc-routing-postgres.password (dir 700, file 600; made on
# first use, never printed). The conninfo for RE goes to <workdir>/conninfo (600). Phases 5b+5c: the
# trainer's login role parity_cc_routing_trainer_job (a member of parity_cc_routing_trainer, db/V3;
# password secrets/local-dev/cc-routing-trainer-postgres.password) in <workdir>/conninfo-trainer.
#   scripts/local-postgres.sh up <workdir>     # <workdir>/conninfo, <workdir>/container
#   scripts/local-postgres.sh down <workdir>   # removes the container
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SECRETS="$(cd "$ROOT/.." && pwd)/secrets/local-dev"
PW="$SECRETS/cc-routing-postgres.password"
TPW="$SECRETS/cc-routing-trainer-postgres.password"  # phases 5b+5c: the trainer's login role
IMAGE="postgres:17-alpine"
die() { echo "local-postgres: $*" >&2; exit 2; }
[[ $# == 2 ]] || die "usage: local-postgres.sh up|down <workdir>"
cmd=$1 work=$2
[[ -d "$work" ]] || die "no workdir $work"
case "$cmd" in
  up)
    [[ -d "$SECRETS" ]] || die "no $SECRETS"
    [[ "$(stat -c %a "$SECRETS")" == 700 ]] || die "$SECRETS must be 700"
    if [[ ! -f "$PW" ]]; then
      (umask 077; head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 32 >"$PW")
    fi
    chmod 600 "$PW"
    if [[ ! -f "$TPW" ]]; then
      (umask 077; head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 32 >"$TPW")
    fi
    chmod 600 "$TPW"
    admin="$(head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 32)"  # this container only
    name="parity-cc-routing-pg-$$-$RANDOM"
    docker run -d --name "$name" --label parity.test=parity-cc-routing-store --security-opt label=disable \
      -e POSTGRES_DB=routing -e POSTGRES_PASSWORD="$admin" \
      -v "$PW:/run/secrets/app_pw:ro" -v "$TPW:/run/secrets/trainer_pw:ro" -v "$ROOT/db:/db:ro" -p "127.0.0.1:${PCR_PG_PORT:-}:5432" "$IMAGE" >/dev/null
    echo "$name" >"$work/container"
    for _ in $(seq 1 60); do
      docker exec "$name" pg_isready -q -U postgres -d routing 2>/dev/null && break
      sleep 0.5
    done
    # The image restarts the server once after init: wait for the final one.
    sleep 1
    for _ in $(seq 1 60); do
      docker exec "$name" psql -q -U postgres -d routing -c 'select 1' >/dev/null 2>&1 && break
      sleep 0.5
    done
    for f in $(cd "$ROOT/db" && ls V*__*.sql | sort -V); do
      docker exec "$name" psql -q -U postgres -d routing -v ON_ERROR_STOP=1 -f "/db/$f" >/dev/null
    done
    docker exec -i "$name" psql -q -U postgres -d routing -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
\set pw `cat /run/secrets/app_pw`
SELECT format('CREATE ROLE parity_cc_routing LOGIN PASSWORD %L IN ROLE parity_cc_routing_app', :'pw') \gexec
\set tpw `cat /run/secrets/trainer_pw`
SELECT format('CREATE ROLE parity_cc_routing_trainer_job LOGIN PASSWORD %L IN ROLE parity_cc_routing_trainer', :'tpw') \gexec
SQL
    port="$(docker port "$name" 5432/tcp | head -1 | sed 's/.*://')"
    (umask 077; printf 'host=127.0.0.1 port=%s dbname=routing user=parity_cc_routing password=%s connect_timeout=2\n' \
      "$port" "$(cat "$PW")" >"$work/conninfo")
    (umask 077; printf 'host=127.0.0.1 port=%s dbname=routing user=parity_cc_routing_trainer_job password=%s connect_timeout=2\n' \
      "$port" "$(cat "$TPW")" >"$work/conninfo-trainer")
    echo "postgres up: container $name, 127.0.0.1:$port"
    ;;
  down)
    if [[ -f "$work/container" ]]; then
      docker rm -f "$(cat "$work/container")" >/dev/null 2>&1 || true
      rm -f "$work/container"
    fi
    rm -f "$work/conninfo" "$work/conninfo-trainer"
    ;;
  *) die "usage: local-postgres.sh up|down <workdir>" ;;
esac
