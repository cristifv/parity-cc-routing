#!/usr/bin/env bash
# Phase 3's store acceptance (PLAN §8 row 3): a throwaway Postgres (scripts/local-postgres.sh), then
# //tests/store:pg_store_it against it (a callback survives an RE restart; a duplicate RunCallback
# dials once; RLS; Postgres down) and, phases 5b+5c, //tests/store:trainer_it (the trainer's tenant
# isolation, the model table's roles, a rollback, `trainer finetune`). The container is removed on exit.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# Not under /tmp: Bazel's sandbox gives tests a hermetic /tmp.
mkdir -p "$HOME/.cache/parity-cc-routing"
WORK="$(mktemp -d -p "$HOME/.cache/parity-cc-routing" store-it.XXXXXX)"
chmod 700 "$WORK"
trap '"$ROOT/scripts/local-postgres.sh" down "$WORK"; rm -rf "$WORK"' EXIT
"$ROOT/scripts/local-postgres.sh" up "$WORK"
bazel test "$@" --test_output=errors --test_env=PCR_PG_CONNINFO_FILE="$WORK/conninfo" \
  --test_env=PCR_PG_TRAINER_CONNINFO_FILE="$WORK/conninfo-trainer" --nocache_test_results \
  //tests/store:pg_store_it //tests/store:trainer_it
