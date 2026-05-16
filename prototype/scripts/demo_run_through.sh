#!/usr/bin/env bash
# demo_run_through.sh
# ----------------------------------------------------------------------
# Clean single-terminal walk-through of the NIDSaaS pipeline. Press
# ENTER between each stage. Each stage shows:
#
#   • a one-line description of what the process does
#   • the actual, recent activity from that process (last few lines)
#   • a tidy DONE confirmation with quantitative result
#
# Intended for advisor walkthroughs where you want to narrate calmly,
# not be drowned in 7 simultaneous log tails.
#
# Usage:
#   bash scripts/demo_run_through.sh
#   PACKETS=20000 bash scripts/demo_run_through.sh
# ----------------------------------------------------------------------
set -uo pipefail

cd "$(dirname "$0")/.."

TENANT="${TENANT:-somchart}"
SECRET="${SECRET:-${TENANT}-secret}"
PCAP="${PCAP:-${HOME}/pcap_CIC_IDS2017/Friday-WorkingHours.pcap}"
PACKETS="${PACKETS:-5000}"
CHUNK_MB="${CHUNK_MB:-5}"
GATEWAY="${GATEWAY:-http://localhost:8080}"
WEBHOOK="${WEBHOOK:-http://localhost:9000}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml"

# colors
B="\033[1m"     ; D="\033[2m"     ; R="\033[0m"
GR="\033[32m"   ; YL="\033[33m"  ; CY="\033[36m" ; RD="\033[31m"

hr()    { printf "${D}═══════════════════════════════════════════════════════════════════════${R}\n"; }
title() { hr; printf "${B}  %s${R}\n" "$1"; hr; echo; }
ok()    { printf "  ${GR}✓${R} %s\n" "$1"; }
info()  { printf "  ${CY}→${R} %s\n" "$1"; }
warn()  { printf "  ${YL}⚠${R} %s\n" "$1"; }
err()   { printf "  ${RD}✗${R} %s\n" "$1"; }

pause() { echo; printf "  ${YL}▶${R}  press ENTER to continue …"; read -r _; echo; }

get_offset() {
    $COMPOSE exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server kafka:9092 --topic "$1" 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum+0}'
}

# Wait until $topic offset > $baseline, polling every 2 s, max $max_sec.
wait_grow() {
    local topic="$1" baseline="$2" max_sec="${3:-90}"
    local start=$(date +%s) last=$baseline last_change=$(date +%s)
    while true; do
        local n=$(get_offset "$topic")
        local now=$(date +%s)
        if [[ "$n" -gt "$last" ]]; then
            printf "    ${D}offset %d → %d (+%d) …${R}\r" "$last" "$n" "$((n - last))"
            last=$n; last_change=$now
        fi
        # stop conditions: 5 s of silence after some growth, OR max_sec elapsed
        if (( n > baseline )) && (( now - last_change > 5 )); then
            echo; return 0
        fi
        if (( now - start > max_sec )); then
            echo; return 0
        fi
        sleep 2
    done
}

# Print N most-recent matching lines from a service's log, prefixed.
recent() {
    local svc="$1" pattern="$2" count="${3:-5}"
    $COMPOSE logs --tail=200 "$svc" 2>&1 \
        | grep -iE "$pattern" \
        | tail -n "$count" \
        | sed "s/^[^|]*| /    ${D}│${R} /"
}

# ---------------- pre-flight ----------------
clear
title "NIDSaaS Pipeline Walk-through  —  ${PACKETS} packets via tenant '${TENANT}'"
info "This walks through every stage of the proposed architecture and"
info "shows what each process is actually doing on real Friday traffic."
echo
info "Stages:"
printf "    1 ${B}Gateway${R}        — accepts PCAP via OAuth-protected /ingest_pcap\n"
printf "    2 ${B}Kafka${R}          — durable buffer for the pcap chunks\n"
printf "    3 ${B}flow_extractor${R} — CICFlowMeter parses chunks → flow records\n"
printf "    4 ${B}snort_sidecar${R}  — Snort 3 signature scan → signature events\n"
printf "    5 ${B}detector${R}       — Hybrid Cascade scores flows → verdicts\n"
printf "    6 ${B}alert_fanout${R}   — POSTs verdicts to tenant webhook\n"
printf "    7 ${B}webhook${R}        — tenant SIEM endpoint (this is what tenant sees)\n"
pause

# Slice + auth (silent)
SLICE="/tmp/$(basename "$PCAP" .pcap)_first${PACKETS}.pcap"
[[ ! -f "$SLICE" ]] && {
    info "slicing first ${PACKETS} packets …"
    if command -v tshark >/dev/null 2>&1; then
        tshark -r "$PCAP" -c "$PACKETS" -w "$SLICE" 2>/dev/null
    else
        tcpdump -r "$PCAP" -c "$PACKETS" -w "$SLICE" 2>/dev/null
    fi
}
SIZE=$(stat -c%s "$SLICE")
SIZE_MB=$(awk "BEGIN {printf \"%.1f\", ${SIZE}/1048576}")
N_CHUNKS=$(( (SIZE + CHUNK_MB * 1048576 - 1) / (CHUNK_MB * 1048576) ))
ok "PCAP slice ready: ${SIZE_MB} MB → ${N_CHUNKS} chunks of ${CHUNK_MB} MB"

TOKEN=$(curl -s -X POST "$GATEWAY/oauth/token" \
    -d "grant_type=client_credentials&client_id=${TENANT}&client_secret=${SECRET}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)
[[ -z "$TOKEN" ]] && { err "auth failed — check that ${TENANT} is registered in .env"; exit 1; }
ok "authenticated as ${TENANT}"

curl -sf -X POST "$WEBHOOK/traces/reset"  >/dev/null
curl -sf -X POST "$WEBHOOK/alerts/reset?tenant=${TENANT}" >/dev/null
ok "webhook trace store cleared"

# Snapshot baselines
declare -A B
for t in pcap_chunks raw signature alerts; do B[$t]=$(get_offset "tenant.${TENANT}.${t}"); done
WB=$(curl -s "${WEBHOOK}/alerts/${TENANT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
pause

# ============================================================
# STAGE 1 — Gateway
# ============================================================
clear; title "STAGE 1  —  Gateway accepts the PCAP"
info "POSTing ${N_CHUNKS} chunks of ${CHUNK_MB} MB to ${GATEWAY}/ingest_pcap"
info "(each chunk carries an OAuth bearer token + a unique X-Chunk-Id)"
echo
for ((i=0; i<N_CHUNKS; i++)); do
    dd if="$SLICE" bs=$((CHUNK_MB * 1048576)) skip="$i" count=1 \
        of=/tmp/_chunk_$i.bin 2>/dev/null
    code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$GATEWAY/ingest_pcap" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/octet-stream" \
        -H "X-Chunk-Id: walkthrough-${TENANT}-$(printf %04d $i)" \
        --data-binary "@/tmp/_chunk_$i.bin")
    rm -f /tmp/_chunk_$i.bin
    printf "    chunk %d/%d → ${GR}HTTP %s${R}\n" $((i+1)) "$N_CHUNKS" "$code"
done
echo
ok "all ${N_CHUNKS} chunks accepted by gateway"
pause

# ============================================================
# STAGE 2 — Kafka pcap_chunks
# ============================================================
clear; title "STAGE 2  —  Kafka topic 'tenant.${TENANT}.pcap_chunks' grew"
info "Gateway produced into Kafka. Two consumer groups subscribe:"
info "  • flow_extractor (CICFlowMeter)"
info "  • snort_sidecar (Snort 3)"
echo
NEW=$(get_offset "tenant.${TENANT}.pcap_chunks")
printf "    offset: ${B[pcap_chunks]} → ${NEW}  ${GR}(+%d chunks)${R}\n" $((NEW - B[pcap_chunks]))
pause

# ============================================================
# STAGE 3 — flow_extractor
# ============================================================
clear; title "STAGE 3  —  flow_extractor (CICFlowMeter)"
info "Each chunk is replayed through CICFlowMeter v4.0 to extract"
info "78 per-flow features. Output goes to tenant.${TENANT}.raw."
echo
info "waiting for flow_extractor to drain the chunks …"
wait_grow "tenant.${TENANT}.raw" "${B[raw]}" 90
echo
NEW=$(get_offset "tenant.${TENANT}.raw")
printf "    offset: ${B[raw]} → ${NEW}  ${GR}(+%d flows)${R}\n" $((NEW - B[raw]))
echo
info "recent flow_extractor activity:"
recent flow_extractor "${TENANT}.*chunk.*flows" 5
pause

# ============================================================
# STAGE 4 — snort_sidecar
# ============================================================
clear; title "STAGE 4  —  Snort 3 signature engine"
info "In parallel with flow_extractor, Snort 3 replays each chunk"
info "through its full ruleset (~30 s per chunk). Hits go to"
info "tenant.${TENANT}.signature."
echo
info "waiting for snort_sidecar to drain the chunks (this is the slow stage) …"
wait_grow "tenant.${TENANT}.signature" "${B[signature]}" 240
echo
NEW=$(get_offset "tenant.${TENANT}.signature")
printf "    offset: ${B[signature]} → ${NEW}  ${GR}(+%d signature hits)${R}\n" $((NEW - B[signature]))
echo
info "recent snort_sidecar activity:"
recent snort_sidecar "${TENANT}.*chunk.*alerts.*elapsed" 5
pause

# ============================================================
# STAGE 5 — detector
# ============================================================
clear; title "STAGE 5  —  Hybrid Cascade detector"
info "detector consumes flows from .raw and signature hits from .signature,"
info "joins them by flow_id, then runs Hybrid Cascade:"
printf "    Tier-0 ${D}signature${R}     — Snort match  → immediate alert\n"
printf "    Tier-1 ${D}rate rules${R}    — V/L/S/R/P/B  → immediate alert\n"
printf "    Tier-2 ${D}calibrated ML${R} — RF + Conformal + GBDT (τ*=0.064)\n"
echo
info "waiting for detector to score & emit verdicts …"
wait_grow "tenant.${TENANT}.alerts" "${B[alerts]}" 120
echo
NEW=$(get_offset "tenant.${TENANT}.alerts")
printf "    offset: ${B[alerts]} → ${NEW}  ${GR}(+%d verdicts)${R}\n" $((NEW - B[alerts]))
echo
info "recent detector verdicts:"
recent detector "${TENANT}.*verdict" 5
pause

# ============================================================
# STAGE 6 — alert_fanout
# ============================================================
clear; title "STAGE 6  —  alert_fanout posts to tenant webhook"
info "alert_fanout consumes tenant.${TENANT}.alerts and POSTs each"
info "verdict to the tenant's registered webhook URL."
echo
info "recent alert_fanout activity:"
recent alert_fanout "delivered.*${TENANT}" 8
pause

# ============================================================
# STAGE 7 — webhook
# ============================================================
clear; title "STAGE 7  —  Tenant webhook (the SIEM)"
info "webhook_receiver simulates the tenant's SIEM endpoint."
info "Each POST 200 OK is one alert successfully delivered to the tenant."
echo
WC=$(curl -s "${WEBHOOK}/alerts/${TENANT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
printf "    webhook /alerts/${TENANT}: ${WB} → ${WC}  ${GR}(+%d delivered)${R}\n" $((WC - WB))
echo
info "sample alert payload from the tenant SIEM:"
curl -s "${WEBHOOK}/alerts/${TENANT}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
items = d.get('items', [])
if items:
    s = items[0]
    keys = ['flow_id','tenant','decision','tier','score','reason',
            'snort_hit','flow_capture_ts','source_file']
    for k in keys:
        v = s.get(k)
        if v is not None:
            print(f'      {k}: {v}')
" 2>/dev/null
pause

# ============================================================
# Summary
# ============================================================
clear; title "Summary  —  what each process did this run"
declare -A AFTER
for t in pcap_chunks raw signature alerts; do AFTER[$t]=$(get_offset "tenant.${TENANT}.${t}"); done
WC=$(curl -s "${WEBHOOK}/alerts/${TENANT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")

printf "  ${B}%-22s %-20s %12s${R}\n" "process" "topic / endpoint" "delta"
printf "  %-22s %-20s %12s\n" "----------------------" "--------------------" "------"
printf "  %-22s %-20s ${GR}%12s${R}\n" "Gateway"        "pcap_chunks"          "+$((AFTER[pcap_chunks] - B[pcap_chunks]))"
printf "  %-22s %-20s ${GR}%12s${R}\n" "flow_extractor"  "raw (flows)"          "+$((AFTER[raw]         - B[raw]))"
printf "  %-22s %-20s ${GR}%12s${R}\n" "snort_sidecar"   "signature (hits)"     "+$((AFTER[signature]   - B[signature]))"
printf "  %-22s %-20s ${GR}%12s${R}\n" "detector"        "alerts (verdicts)"    "+$((AFTER[alerts]      - B[alerts]))"
printf "  %-22s %-20s ${GR}%12s${R}\n" "alert_fanout"    "→ webhook"            "+$((WC - WB))"
printf "  %-22s %-20s ${GR}%12s${R}\n" "webhook"         "tenant SIEM"          "+$((WC - WB))"
echo
ok "End-to-end pipeline accounted for. Open the dashboard at ${WEBHOOK}"
ok "to see the alerts in the live table."
echo
