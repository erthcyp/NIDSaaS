#!/usr/bin/env bash
# Watch the end-to-end attack flow. Assumes the stack is already up.

set -euo pipefail

echo "==> gateway health"
curl -s http://localhost:8080/healthz | jq . || curl -s http://localhost:8080/healthz

echo
echo "==> fetching OAuth2 token for tenant 'acme'"
tok=$(curl -s -X POST http://localhost:8080/oauth/token \
  -d "grant_type=client_credentials" \
  -d "client_id=acme" \
  -d "client_secret=acme-secret" | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
echo "token=${tok:0:40}..."

echo
echo "==> injecting a synthetic SYN-flood-like flow via /ingest"
curl -s -X POST http://localhost:8080/ingest \
  -H "Authorization: Bearer ${tok}" \
  -H "Content-Type: application/json" \
  -d '{
    "flow_id": "demo-syn-flood",
    "features": {
      "Flow Duration": 1000000,
      "Total Fwd Packets": 500,
      "Total Backward Packets": 5,
      "SYN Flag Count": 450,
      "ACK Flag Count": 10,
      "RST Flag Count": 2,
      "Flow Packets/s": 500,
      "Destination Port": 80
    }
  }' | jq . || true

echo
echo "==> waiting 3s for detector + fanout"
sleep 3

echo
echo "==> alerts seen by tenant acme webhook"
curl -s http://localhost:9000/alerts/acme | jq . || curl -s http://localhost:9000/alerts/acme
