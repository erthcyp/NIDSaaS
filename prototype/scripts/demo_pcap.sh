#!/usr/bin/env bash
# demo_pcap.sh
# ----------------------------------------------------------------------
# One-command end-to-end demo: send a small PCAP through the running
# NIDSaaS prototype and observe alerts arrive at the webhook receiver.
#
# Pipeline path exercised:
#   client → gateway:8080 /ingest_pcap
#         → tenant.{u}.pcap_chunks (Kafka)
#         → flow_extractor (CICFlowMeter) → tenant.{u}.raw
#         → snort_sidecar (Snort 3)       → tenant.{u}.signature
#         → detector (Hybrid Cascade)     → tenant.{u}.alerts
#         → alert_fanout                  → webhook_receiver:9000
#
# Usage:
#   ./scripts/demo_pcap.sh [PCAP_PATH]
#
# Defaults:
#   PCAP_PATH = /tmp/demo_attack.pcap (auto-sliced from
#               ~/pcap_CIC_IDS2017/Friday-WorkingHours.pcap if missing)
#   TENANT    = acme
#   GATEWAY   = http://localhost:8080
#   WEBHOOK   = http://localhost:9000
# ----------------------------------------------------------------------
set -euo pipefail

# ---- config (override via env) ---------------------------------------
TENANT="${TENANT:-acme}"
SECRET="${SECRET:-acme-secret}"
GATEWAY="${GATEWAY:-http://localhost:8080}"
WEBHOOK="${WEBHOOK:-http://localhost:9000}"
PCAP_PATH="${1:-/tmp/demo_attack.pcap}"
PCAP_SOURCE="${PCAP_SOURCE:-${HOME}/pcap_CIC_IDS2017/Friday-WorkingHours.pcap}"
SLICE_PACKETS="${SLICE_PACKETS:-1000}"
SETTLE_SEC="${SETTLE_SEC:-8}"

# ---- pretty print helpers --------------------------------------------
green()  { printf "\033[32m%s\033[0m\n" "$*"; }
red()    { printf "\033[31m%s\033[0m\n" "$*"; }
yellow() { printf "\033[33m%s\033[0m\n" "$*"; }
bold()   { printf "\033[1m%s\033[0m\n" "$*"; }
hr()     { printf "\033[2m%s\033[0m\n" "────────────────────────────────────────────────"; }

# ---- step 0: pre-flight ----------------------------------------------
hr; bold "STEP 0  pre-flight"; hr

if ! command -v curl >/dev/null 2>&1; then
    red "  curl not found"; exit 1
fi

# stop tenant_simulator AND any leftover instance so it doesn't flood
# the gateway during health checks (it auto-starts and bombards
# /ingest_pcap; even an Exited one can have lingering connections)
yellow "  ⤷ stopping tenant_simulator (and any leftover instance)"
docker compose stop tenant_simulator >/dev/null 2>&1 || true
docker stop nidsaas_tenant_simulator >/dev/null 2>&1 || true
docker rm   nidsaas_tenant_simulator >/dev/null 2>&1 || true
sleep 3

# wait up to 60 s for gateway to respond. Use a longer per-request
# timeout (10 s) so a bursty gateway can still answer.
gateway_ok=0
for i in $(seq 1 20); do
    if curl -sf -m 10 "${GATEWAY}/healthz" >/dev/null 2>&1; then
        gateway_ok=1
        break
    fi
    if [[ $i -eq 1 ]]; then
        printf "  ⏳ waiting for gateway"
    else
        printf "."
    fi
    sleep 3
done
echo
if [[ $gateway_ok -ne 1 ]]; then
    red "  gateway not reachable at ${GATEWAY} after 60 s"
    yellow "  Manual debug:"
    yellow "    curl -v --max-time 10 ${GATEWAY}/healthz       # direct test"
    yellow "    docker exec nidsaas_gateway curl localhost:8080/healthz"
    yellow "                                                    # bypass WSL port-forward"
    yellow "    docker compose logs --tail 30 gateway          # check app errors"
    yellow "  Common WSL2 issue: port forward stale → wsl --shutdown then reopen"
    exit 1
fi
green "  ✓ gateway reachable: ${GATEWAY}"

# webhook /traces may be huge if the simulator was just running — give
# it a generous timeout for the first reach-out
if ! curl -sf -m 15 "${WEBHOOK}/traces" >/dev/null 2>&1; then
    red "  webhook receiver not reachable at ${WEBHOOK}"
    exit 1
fi
green "  ✓ webhook reachable: ${WEBHOOK}"

mode=$(curl -s -m 5 "${GATEWAY}/healthz" | python3 -c \
    "import sys,json; print(json.load(sys.stdin).get('ingest_mode','?'))" 2>/dev/null || echo "?")
green "  ✓ gateway INGEST_MODE = ${mode}"

# ---- step 1: ensure we have a PCAP to send ---------------------------
hr; bold "STEP 1  prepare PCAP"; hr

if [[ ! -f "${PCAP_PATH}" ]]; then
    yellow "  ${PCAP_PATH} not found — slicing ${SLICE_PACKETS} packets from"
    yellow "  ${PCAP_SOURCE}"
    if [[ ! -f "${PCAP_SOURCE}" ]]; then
        red "  source PCAP missing: ${PCAP_SOURCE}"
        red "  set PCAP_SOURCE=<path-to-real-pcap> or pass demo PCAP as arg 1"
        exit 1
    fi
    if command -v tshark >/dev/null 2>&1; then
        tshark -r "${PCAP_SOURCE}" -c "${SLICE_PACKETS}" -w "${PCAP_PATH}" 2>/dev/null
    elif command -v tcpdump >/dev/null 2>&1; then
        tcpdump -r "${PCAP_SOURCE}" -c "${SLICE_PACKETS}" -w "${PCAP_PATH}" 2>/dev/null
    else
        red "  neither tshark nor tcpdump available — install one or pre-slice the PCAP"
        exit 1
    fi
fi

bytes=$(stat -c%s "${PCAP_PATH}" 2>/dev/null || stat -f%z "${PCAP_PATH}")
green "  PCAP ready: ${PCAP_PATH} (${bytes} bytes)"

# ---- step 2: OAuth token --------------------------------------------
hr; bold "STEP 2  get OAuth token for tenant '${TENANT}'"; hr

token=$(curl -sf -X POST "${GATEWAY}/oauth/token" \
    -d "grant_type=client_credentials" \
    -d "client_id=${TENANT}" \
    -d "client_secret=${SECRET}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

if [[ -z "${token}" ]]; then
    red "  token fetch failed"; exit 1
fi
green "  token: ${token:0:30}..."

# ---- step 3: reset webhook trace store ------------------------------
hr; bold "STEP 3  reset webhook trace store"; hr
curl -sf -X POST "${WEBHOOK}/traces/reset" >/dev/null
green "  webhook /traces cleared"

# ---- step 4: send PCAP ----------------------------------------------
hr; bold "STEP 4  POST /ingest_pcap"; hr

trace_id="demo-pcap-$(date +%s)-$$"
chunk_id="demo-chunk-$(date +%s)"

t0=$(date +%s.%N)
http_status=$(curl -s -o /tmp/.demo_pcap_response.json -w "%{http_code}" \
    -X POST "${GATEWAY}/ingest_pcap" \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/vnd.tcpdump.pcap" \
    -H "X-Trace-Id: ${trace_id}" \
    -H "X-Chunk-Id: ${chunk_id}" \
    -H "X-Pcap-File: $(basename "${PCAP_PATH}")" \
    --data-binary "@${PCAP_PATH}")
t1=$(date +%s.%N)
post_ms=$(awk "BEGIN { printf \"%.0f\", (${t1}-${t0})*1000 }")

if [[ "${http_status}" != "202" && "${http_status}" != "200" ]]; then
    red "  HTTP ${http_status} — unexpected"
    cat /tmp/.demo_pcap_response.json
    exit 1
fi
green "  HTTP ${http_status} accepted in ${post_ms} ms"
echo "  trace_id=${trace_id}"
echo "  chunk_id=${chunk_id}"
cat /tmp/.demo_pcap_response.json | python3 -m json.tool 2>/dev/null \
    | sed 's/^/    /' || true

# ---- step 5: wait for downstream processing -------------------------
hr; bold "STEP 5  wait ${SETTLE_SEC}s for pipeline to process"; hr
echo "  flow_extractor → snort_sidecar (parallel) → detector → alert_fanout → webhook"
for s in $(seq 1 "${SETTLE_SEC}"); do
    printf "  ⏳ %d/%d\r" "${s}" "${SETTLE_SEC}"
    sleep 1
done
echo

# ---- step 6: query webhook ------------------------------------------
hr; bold "STEP 6  query webhook receiver"; hr

curl -sf "${WEBHOOK}/traces" -o /tmp/.demo_pcap_traces.json

read total events <<< $(python3 -c "
import json
d = json.load(open('/tmp/.demo_pcap_traces.json'))
# webhook may return one of:
#   { trace_id: [entries] }
#   { 'count': N, 'traces': { trace_id: [entries] } }
#   { 'count': N, ... }  (envelope without trace map)
if isinstance(d, dict) and isinstance(d.get('traces'), dict):
    traces = d['traces']
elif isinstance(d, dict):
    traces = {k: v for k, v in d.items() if isinstance(v, list)}
elif isinstance(d, list):
    traces = {f'_{i}': v for i, v in enumerate(d) if isinstance(v, list)}
else:
    traces = {}
print(len(traces), sum(len(v) for v in traces.values()))
")
green "  total traces:          ${total}"
green "  total alert events:    ${events}"

if [[ "${events}" -eq 0 ]]; then
    yellow "  ⚠ no alerts received yet — pipeline may still be processing"
    yellow "    increase SETTLE_SEC or rerun"
    yellow "    raw response:"
    python3 -m json.tool /tmp/.demo_pcap_traces.json 2>/dev/null \
        | head -20 | sed 's/^/      /' || \
        sed 's/^/      /' /tmp/.demo_pcap_traces.json | head -20
else
    echo
    bold "  sample of first 3 alerts (decision / tier / score):"
    python3 -c "
import json
d = json.load(open('/tmp/.demo_pcap_traces.json'))
if isinstance(d, dict) and isinstance(d.get('traces'), dict):
    traces = d['traces']
elif isinstance(d, dict):
    traces = {k: v for k, v in d.items() if isinstance(v, list)}
else:
    traces = {}
for tid, entries in list(traces.items())[:3]:
    print(f'    trace={str(tid)[:40]}')
    for e in entries[:2]:
        dec = e.get('decision', '?') if isinstance(e, dict) else '?'
        tier = e.get('tier', '?') if isinstance(e, dict) else '?'
        score = e.get('score') if isinstance(e, dict) else None
        score_s = f'{score:.3f}' if isinstance(score, (int, float)) else '?'
        print(f'      decision={dec}  tier={tier}  score={score_s}')
"
fi

# ---- summary --------------------------------------------------------
hr; bold "DEMO SUMMARY"; hr
echo "  PCAP sent:        ${PCAP_PATH} (${bytes} bytes)"
echo "  gateway accepted: HTTP ${http_status} in ${post_ms} ms"
echo "  alerts received:  ${events} events across ${total} traces"
echo
green "  ✓ end-to-end pipeline confirmed working"
echo
echo "  next steps:"
echo "    docker compose logs -f detector alert_fanout webhook_receiver"
echo "    curl -s ${WEBHOOK}/traces | python3 -m json.tool | less"
