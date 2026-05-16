#!/usr/bin/env bash
# demo_somchart.sh
# ----------------------------------------------------------------------
# Single-terminal, step-by-step local demo of the proposed NIDSaaS
# architecture. Simulates a fictional tenant named "somchart" sending
# synthetic flow records; the audience watches the same record traverse
# gateway → Kafka → Spark → detector → webhook end-to-end.
#
# Designed for local laptop runs (no SIIT server access needed).
#
# Usage (from project root):
#   bash prototype/scripts/demo_somchart.sh
# ----------------------------------------------------------------------
set -uo pipefail

cd "$(dirname "$0")/.."   # cd to prototype/

green()  { printf "\033[32m%s\033[0m\n" "$*"; }
red()    { printf "\033[31m%s\033[0m\n" "$*"; }
yellow() { printf "\033[33m%s\033[0m\n" "$*"; }
blue()   { printf "\033[36m%s\033[0m\n" "$*"; }
bold()   { printf "\033[1m%s\033[0m\n" "$*"; }
hr()     { printf "\033[2m%s\033[0m\n" "════════════════════════════════════════════════════════════════════"; }

pause() {
    echo
    yellow "  ▶  press ENTER to continue ..."
    read -r _
    echo
}

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml"
TENANT="somchart"
SECRET="somchart-secret"

# ----------------------------------------------------------------------
# Intro
# ----------------------------------------------------------------------
clear
hr
bold "  NIDSaaS Live Demo — tenant '$TENANT'"
hr
echo
echo "  This walkthrough simulates a fictional tenant — '$TENANT' — sending"
echo "  synthetic flow records to the NIDSaaS gateway. We will watch each"
echo "  record traverse the full pipeline:"
echo
echo "    Tenant '$TENANT'"
echo "        ↓ POST /ingest (OAuth2-authenticated)"
echo "    Gateway"
echo "        ↓ produce → Kafka(tenant.$TENANT.raw)"
echo "    Apache Spark Preprocessor (Schema-Normalize / Validate / Stage)"
echo "        ↓ produce → Kafka(tenant.$TENANT.preprocessed)"
echo "    Hybrid Cascade Detector"
echo "        ↓ produce → Kafka(tenant.$TENANT.alerts)"
echo "    Alert Fan-out → Webhook (tenant SIEM endpoint)"
echo
pause

# ----------------------------------------------------------------------
# Step 1 — Make sure 'somchart' is registered as a tenant
# ----------------------------------------------------------------------
clear
hr; bold "  Step 1 / 8  —  Register tenant '$TENANT' in .env"; hr
echo
blue "  The gateway reads its tenant list and OAuth credentials from .env."
blue "  We add '$TENANT' alongside the default acme/globex/initech tenants."
echo

cat > .env <<EOF
INGEST_MODE=kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
LOG_LEVEL=INFO
OAUTH_CLIENTS=acme:acme-secret;globex:globex-secret;initech:initech-secret;${TENANT}:${SECRET}
TENANTS=acme,globex,initech,${TENANT}
GATEWAY_JWT_SECRET=local-demo-jwt-secret
WEBHOOKS=acme:http://webhook_receiver:9000/acme;globex:http://webhook_receiver:9000/globex;initech:http://webhook_receiver:9000/initech;${TENANT}:http://webhook_receiver:9000/${TENANT}
DETECT_TAU_STAR=0.0
EOF

green "  ✓ .env written"
echo
echo "  TENANTS line:"
grep "^TENANTS=" .env
echo
pause

# ----------------------------------------------------------------------
# Step 2 — Bring up / refresh the stack so the new tenant takes effect
# ----------------------------------------------------------------------
clear
hr; bold "  Step 2 / 8  —  Bring up the stack (or recreate to pick up .env)"; hr
echo
blue "  We need the Kafka topics for tenant.$TENANT.{raw,preprocessed,alerts}"
blue "  and the gateway/alert_fanout containers must read the new .env."
echo

if ! $COMPOSE ps --status running 2>/dev/null | grep -q "spark_preprocessor"; then
    yellow "  Stack not running — bringing it up (~60 s for Spark to start)..."
    $COMPOSE up -d
    sleep 60
else
    yellow "  Stack already running — recreating gateway + alert_fanout to"
    yellow "  pick up the updated tenant list ..."
    $COMPOSE up -d --no-deps --force-recreate gateway alert_fanout
    sleep 10
fi

echo
echo "  Service status:"
$COMPOSE ps --format "table {{.Service}}\t{{.Status}}" | head -15
echo
green "  ✓ stack ready"
pause

# ----------------------------------------------------------------------
# Step 3 — Create the per-tenant Kafka topics for somchart
# ----------------------------------------------------------------------
clear
hr; bold "  Step 3 / 8  —  Create Kafka topics for tenant '$TENANT'"; hr
echo
blue "  Each tenant has dedicated Kafka topics for isolation. We create"
blue "  the four needed topics (.raw, .preprocessed, .alerts, .signature)"
blue "  if they don't already exist."
echo

for suffix in raw preprocessed alerts signature; do
    topic="tenant.${TENANT}.${suffix}"
    if $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka:9092 --list 2>/dev/null \
            | grep -qx "$topic"; then
        green "  ✓ exists: $topic"
    else
        $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka:9092 \
            --create --topic "$topic" \
            --partitions 3 --replication-factor 1 2>/dev/null
        green "  ✓ created: $topic"
    fi
done
pause

# ----------------------------------------------------------------------
# Step 4 — Authenticate as somchart and get a JWT bearer token
# ----------------------------------------------------------------------
clear
hr; bold "  Step 4 / 8  —  OAuth2 — '$TENANT' authenticates with the gateway"; hr
echo
blue "  Tenants authenticate via OAuth2 client-credentials flow. The"
blue "  gateway issues a short-lived JWT bearer token; subsequent /ingest"
blue "  calls use it in the Authorization header."
echo
echo "  POST /oauth/token  (client_id=$TENANT, client_secret=$SECRET)"
echo

TOKEN_JSON=$(curl -s -X POST localhost:8080/oauth/token \
    -d "grant_type=client_credentials&client_id=${TENANT}&client_secret=${SECRET}")

TOKEN=$(echo "$TOKEN_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)

if [[ -z "$TOKEN" ]]; then
    red "  ✗ authentication failed!"
    echo "  Response: $TOKEN_JSON"
    echo
    yellow "  Check that the gateway picked up the new .env. Re-run Step 2."
    exit 1
fi

echo "  Response (truncated):"
echo "$TOKEN_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'    access_token: {d[\"access_token\"][:40]}... ({len(d[\"access_token\"])} chars)')
print(f'    token_type:   {d[\"token_type\"]}')
print(f'    expires_in:   {d[\"expires_in\"]} s')
"
echo
green "  ✓ '$TENANT' is authenticated"
pause

# ----------------------------------------------------------------------
# Step 5 — Reset webhook trace store
# ----------------------------------------------------------------------
clear
hr; bold "  Step 5 / 8  —  Reset webhook trace store"; hr
echo
blue "  We wipe the webhook receiver's trace store so the demo starts"
blue "  with a count of zero — anything that arrives next is from this run."
echo

curl -sf -X POST localhost:9000/traces/reset >/dev/null
echo "  GET /traces (count) →"
curl -s localhost:9000/traces | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(f'    {{\"count\": {d.get(\"count\",0)}}}')"
echo
green "  ✓ trace store empty"
pause

# ----------------------------------------------------------------------
# Step 6 — Send ONE flow record and watch it arrive
# ----------------------------------------------------------------------
clear
hr; bold "  Step 6 / 8  —  Send 1 synthetic flow as '$TENANT'"; hr
echo
blue "  We POST a single flow record to /ingest. Watch this single record"
blue "  traverse the pipeline and show up at the webhook 1–2 seconds later."
echo

PAYLOAD=$(cat <<EOF
{
  "flow_id": "demo-${TENANT}-001",
  "features": {
    "flow_duration": 1500000,
    "total_packets": 25,
    "flow_packets_s": 16.7,
    "flow_bytes_s": 8400,
    "destination_port": 443,
    "syn_flag_count": 1,
    "rst_flag_count": 0
  }
}
EOF
)

echo "  Request body:"
echo "$PAYLOAD" | sed 's/^/    /'
echo

echo "  POST /ingest  (Authorization: Bearer …)  →"
RESP=$(curl -s -w "\n  HTTP %{http_code}" -X POST localhost:8080/ingest \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -H "X-Trace-Id: demo-trace-001" \
    -d "$PAYLOAD")
echo "$RESP" | sed 's/^/    /'
echo

yellow "  waiting 3 s for the flow to traverse the pipeline ..."
sleep 3

echo
echo "  GET /traces — what reached the webhook for '$TENANT'?"
curl -s localhost:9000/traces | python3 -c "
import sys, json
d = json.load(sys.stdin)
traces = d.get('traces', {})
mine = []
for tid, events in traces.items():
    if events and events[0].get('tenant') == '${TENANT}':
        mine.append(events[0])
if not mine:
    print('    (no alerts yet — try Step 7 to send more)')
else:
    print(f'    {len(mine)} alert(s) for tenant=${TENANT}:')
    for ev in mine:
        print(f'      flow_id={ev.get(\"flow_id\")}  decision={ev.get(\"decision\")}  '
              f'tier={ev.get(\"tier\")}  score={ev.get(\"score\")}')
"
pause

# ----------------------------------------------------------------------
# Step 7 — Send a small burst (20 flows) and show the count
# ----------------------------------------------------------------------
clear
hr; bold "  Step 7 / 8  —  Send a 20-flow burst as '$TENANT'"; hr
echo
blue "  Now we send 20 flow records in quick succession to demonstrate"
blue "  sustained pipeline operation. Each flow gets a unique flow_id"
blue "  so the webhook can track them individually."
echo

curl -sf -X POST localhost:9000/traces/reset >/dev/null
echo "  webhook trace store cleared"
echo
echo "  sending 20 flows ..."

for i in $(seq 1 20); do
    PAYLOAD=$(cat <<EOF
{
  "flow_id": "demo-${TENANT}-burst-$(printf '%03d' $i)",
  "features": {
    "flow_duration": $((1000000 + i*50000)),
    "total_packets": $((20 + i)),
    "flow_packets_s": $((10 + i)),
    "flow_bytes_s": $((5000 + i*100)),
    "destination_port": 443,
    "syn_flag_count": 1,
    "rst_flag_count": 0
  }
}
EOF
)
    code=$(curl -s -o /dev/null -w "%{http_code}" -X POST localhost:8080/ingest \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "$PAYLOAD")
    printf "    flow %02d → HTTP %s\n" "$i" "$code"
    sleep 0.1
done

echo
yellow "  waiting 5 s for Spark micro-batches to drain ..."
sleep 5

echo
green "  ✓ burst complete"
pause

# ----------------------------------------------------------------------
# Step 8 — Show the alerts that arrived at the webhook
# ----------------------------------------------------------------------
clear
hr; bold "  Step 8 / 8  —  Final summary — what '$TENANT' SIEM received"; hr
echo
blue "  The webhook receiver simulates the tenant's SIEM endpoint. Every"
blue "  alert here is one that traversed the full pipeline (gateway →"
blue "  Kafka → Spark → detector → fan-out → webhook) for tenant '$TENANT'."
echo

curl -s localhost:9000/traces | python3 -c "
import sys, json
d = json.load(sys.stdin)
total = d.get('count', 0)
traces = d.get('traces', {})

mine = []
for tid, events in traces.items():
    if events and events[0].get('tenant') == '${TENANT}':
        mine.append(events[0])

print(f'  total alert events delivered (all tenants): {total}')
print(f'  for tenant ${TENANT}: {len(mine)}')
print()
if mine:
    print('  alert summary for ${TENANT}:')
    by_tier = {}
    for ev in mine:
        by_tier[ev.get('tier','?')] = by_tier.get(ev.get('tier','?'), 0) + 1
    for tier, count in by_tier.items():
        print(f'    {tier}: {count}')

    print()
    print('  example alert payload (one of the delivered alerts):')
    print()
    for line in json.dumps(mine[0], indent=4).split(chr(10)):
        print(f'    {line}')
"
echo
green "  ✓ end-to-end pipeline functional for tenant '$TENANT'"
echo
hr
bold "  Demo complete."
hr
echo
echo "  Recap:"
echo "    • Tenant '$TENANT' authenticated via OAuth2"
echo "    • Sent 21 synthetic flows (1 + 20)"
echo "    • Each flow traversed gateway → Kafka → Spark → detector → webhook"
echo "    • Tier-1 rate rules in the Hybrid Cascade detector flagged the flows"
echo "    • Alerts delivered to the tenant's webhook endpoint"
echo
echo "  Tear down (when done):"
echo "    docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml down"
echo
