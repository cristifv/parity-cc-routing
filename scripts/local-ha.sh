#!/usr/bin/env bash
# A throwaway etcd and Redpanda for parity-cc-routing's phase-6 chaos runs (PLAN §5.5, §7), like
# scripts/local-postgres.sh: the pinned images of parity-deploy's compose, each on a free loopback
# port picked here (no fixed port, so no e2e lock; PCR_ETCD_PORT / PCR_KAFKA_PORT override them),
# labelled parity.test=parity-cc-routing-ha.
# re.state is created compacted with 6 partitions. No credentials: etcd and the Kafka listener are
# unauthenticated loopback-only test services (etcd auth, Kafka TLS/SASL: CR-133, CR-110, BEFORE
# DEPLOY), so nothing goes to secrets/.
#   scripts/local-ha.sh up <workdir>     # <workdir>/ha.env (ETCD=, BROKERS=, ETCD_C=, KAFKA_C=)
#   scripts/local-ha.sh down <workdir>
set -euo pipefail
ETCD_IMAGE="gcr.io/etcd-development/etcd@sha256:7c6c239825d00e3f6328a69caafd54be92063acf0c2ce78b8394699f52b75dc3"
RP_IMAGE="docker.redpanda.com/redpandadata/redpanda@sha256:9e83cfa99278f30d0133271c26bf670cd69c94ffa6ba0b42830dd0c3bd9dcfd9"
die() { echo "local-ha: $*" >&2; exit 2; }
[[ $# == 2 ]] || die "usage: local-ha.sh up|down <workdir>"
cmd=$1 work=$2
[[ -d "$work" ]] || die "no workdir $work"
case "$cmd" in
  up)
    read -r EP KP <<<"$(python3 -I -c 'import socket
s=[socket.socket() for _ in range(2)]
[x.bind(("127.0.0.1",0)) for x in s]
print(" ".join(str(x.getsockname()[1]) for x in s))')"
    EP=${PCR_ETCD_PORT:-$EP} KP=${PCR_KAFKA_PORT:-$KP}
    tag="$$-$RANDOM"
    etcd_c="parity-cc-routing-etcd-$tag"
    kafka_c="parity-cc-routing-rp-$tag"
    docker run -d --name "$etcd_c" --label parity.test=parity-cc-routing-ha --security-opt label=disable \
      -p "127.0.0.1:$EP:2379" "$ETCD_IMAGE" etcd --name=chaos --data-dir=/tmp/etcd \
      --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls="http://127.0.0.1:$EP" \
      --listen-peer-urls=http://127.0.0.1:2380 --initial-advertise-peer-urls=http://127.0.0.1:2380 \
      --initial-cluster=chaos=http://127.0.0.1:2380 >/dev/null
    docker run -d --name "$kafka_c" --label parity.test=parity-cc-routing-ha --security-opt label=disable \
      -p "127.0.0.1:$KP:$KP" "$RP_IMAGE" redpanda start --mode dev-container --smp=1 --memory=1G \
      --reserve-memory=0M --overprovisioned --default-log-level=warn --check=false \
      --kafka-addr "PLAINTEXT://0.0.0.0:$KP" --advertise-kafka-addr "PLAINTEXT://127.0.0.1:$KP" >/dev/null
    printf 'ETCD=127.0.0.1:%s\nBROKERS=127.0.0.1:%s\nETCD_C=%s\nKAFKA_C=%s\n' "$EP" "$KP" "$etcd_c" "$kafka_c" >"$work/ha.env"
    for _ in $(seq 1 120); do
      docker exec "$etcd_c" etcdctl --endpoints=http://127.0.0.1:2379 endpoint health >/dev/null 2>&1 && break
      sleep 0.25
    done
    for _ in $(seq 1 240); do
      docker exec "$kafka_c" rpk topic list -X "brokers=127.0.0.1:$KP" >/dev/null 2>&1 && break
      sleep 0.25
    done
    docker exec "$kafka_c" rpk topic create re.state -p 6 -r 1 -c cleanup.policy=compact -X "brokers=127.0.0.1:$KP" >/dev/null
    echo "etcd 127.0.0.1:$EP, redpanda 127.0.0.1:$KP (re.state: 6 partitions, compacted)"
    ;;
  down)
    if [[ -f "$work/ha.env" ]]; then
      # shellcheck disable=SC1091
      source "$work/ha.env"
      docker rm -f "$ETCD_C" "$KAFKA_C" >/dev/null 2>&1 || true
      rm -f "$work/ha.env"
    fi
    ;;
  *) die "usage: local-ha.sh up|down <workdir>" ;;
esac
