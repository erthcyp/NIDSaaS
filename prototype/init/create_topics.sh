#!/usr/bin/env bash
# Creates per-tenant Kafka topics for the NIDSaaS prototype.
# Idempotent: re-running is a no-op if topics already exist.

set -euo pipefail

: "${KAFKA_BOOTSTRAP:=kafka:9092}"
: "${TENANTS:=acme,globex,initech}"
: "${KAFKA_PARTITIONS:=3}"
: "${KAFKA_REPLICATION:=1}"
# pcap_chunks carries binary pcap payloads (up to ~8 MB in pcap mode),
# so its per-topic max.message.bytes must exceed the broker default (1 MB).
: "${PCAP_CHUNK_MAX_BYTES:=16777216}"

# apache/kafka image ships scripts at /opt/kafka/bin; fall back to PATH otherwise.
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
if [[ ! -x "${KAFKA_TOPICS}" ]]; then
  KAFKA_TOPICS="kafka-topics.sh"
fi

echo "[topic_init] waiting for kafka at ${KAFKA_BOOTSTRAP} ..."
for i in {1..30}; do
  if "${KAFKA_TOPICS}" --bootstrap-server "${KAFKA_BOOTSTRAP}" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

# create_topic <name> [extra --config args...]
create_topic () {
  local t="$1"; shift
  if "${KAFKA_TOPICS}" --bootstrap-server "${KAFKA_BOOTSTRAP}" --list | grep -qx "${t}"; then
    echo "[topic_init]   exists: ${t}"
  else
    "${KAFKA_TOPICS}" --bootstrap-server "${KAFKA_BOOTSTRAP}" \
      --create --topic "${t}" \
      --partitions "${KAFKA_PARTITIONS}" \
      --replication-factor "${KAFKA_REPLICATION}" \
      "$@" >/dev/null
    echo "[topic_init]   created: ${t}"
  fi
}

IFS=',' read -r -a TENANT_ARR <<< "${TENANTS}"
for u in "${TENANT_ARR[@]}"; do
  # Flow-sized topics (default 1 MB max.message.bytes is fine).
  for suffix in raw clean quarantine signature alerts; do
    create_topic "tenant.${u}.${suffix}"
  done
  # Pcap-chunk topic: needs a large max.message.bytes override.
  create_topic "tenant.${u}.pcap_chunks" \
    --config "max.message.bytes=${PCAP_CHUNK_MAX_BYTES}"
done

echo "[topic_init] done."
