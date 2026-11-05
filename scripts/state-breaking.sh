#!/usr/bin/env bash
# re.state's records are a contract between releases (PLAN §5.5: bins of release N and N-1 read each
# other's records during tenant moves): `buf breaking` (wire and JSON compatibility) of
# src/state/state.proto against src/state/baseline/state.proto, the last released shape. Uses
# parity-contracts' pinned buf image in the same hardened container. After a release that changes
# state.proto (additively), copy it to baseline/.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/../parity-contracts/scripts/tools.env"
docker image inspect "$BUF_IMAGE" >/dev/null 2>&1 || { echo "state-breaking: $BUF_IMAGE missing (parity-contracts scripts/tools.sh)" >&2; exit 2; }
W="$(mktemp -d)"
trap 'rm -rf "$W"' EXIT
mkdir -p "$W/cur/parity/cc_routing/state/v1" "$W/base/parity/cc_routing/state/v1"
cp "${PCR_STATE_PROTO:-$ROOT/src/state/state.proto}" "$W/cur/parity/cc_routing/state/v1/state.proto"
cp "$ROOT/src/state/baseline/state.proto" "$W/base/parity/cc_routing/state/v1/"
for d in cur base; do printf 'version: v2\nbreaking:\n  use:\n    - WIRE_JSON\n' >"$W/$d/buf.yaml"; done
chmod -R a+rX "$W"
docker run --rm --network none --read-only --tmpfs /tmp --cap-drop ALL --security-opt no-new-privileges \
  --security-opt label=disable --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$W:/workspace:ro" -w /workspace \
  "$BUF_IMAGE" breaking cur --against base
echo "state.proto: no breaking change against the baseline (WIRE_JSON)"
