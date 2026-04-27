#!/usr/bin/env bash
# Quickstart demo. Assumes docker and docker compose are installed.
# Run from prototype/ directory.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

echo "==> bringing up the NIDSaaS prototype stack"
docker compose up -d --build

echo "==> waiting for kafka + topic_init"
for i in {1..60}; do
  state=$(docker inspect -f '{{.State.Status}}' nidsaas_topic_init 2>/dev/null || true)
  if [[ "$state" == "exited" ]]; then
    break
  fi
  sleep 2
done

echo "==> tailing detector + fan-out logs (Ctrl-C to stop)"
docker compose logs -f detector alert_fanout webhook_receiver
