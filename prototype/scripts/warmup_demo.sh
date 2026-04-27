#!/usr/bin/env bash
# warmup_demo.sh
# ----------------------------------------------------------------------
# Pre-demo warm-up: boots the stack, waits for services to be healthy,
# slices a small demo PCAP if missing, and verifies the gateway is in
# the expected mode. Run ~1 minute before walking into the demo room.
#
# Idempotent: safe to re-run any time.
#
# Usage:
#   ./scripts/warmup_demo.sh
# ----------------------------------------------------------------------
set -uo pipefail

cd "$(dirname "$0")/.."   # cd to prototype/

green()  { printf "\033[32m%s\033[0m\n" "$*"; }
red()    { printf "\033[31m%s\033[0m\n" "$*"; }
yellow() { printf "\033[33m%s\033[0m\n" "$*"; }
bold()   { printf "\033[1m%s\033[0m\n" "$*"; }
hr()     { printf "\033[2m%s\033[0m\n" "────────────────────────────────────────────────"; }

# ---- step 1: docker engine reachable ---------------------------------
hr; bold "1. Check Docker engine"; hr
if ! docker info >/dev/null 2>&1; then
    red "  Docker engine not reachable"
    yellow "  hint: start Docker Desktop or 'sudo systemctl start docker' in WSL"
    exit 1
fi
green "  ✓ docker engine OK"

# ---- step 2: bring stack up ------------------------------------------
hr; bold "2. Boot the stack"; hr
docker compose up -d
green "  ✓ docker compose up -d"

# ---- step 3: stop tenant_simulator FIRST (so it stops flooding before checks) ----
hr; bold "3. Stop tenant_simulator (clean demo state)"; hr
docker compose stop tenant_simulator >/dev/null 2>&1 || true
green "  ✓ tenant_simulator stopped"

# ---- step 4: reset webhook traces so /traces query is fast ----------
hr; bold "4. Reset webhook trace store (drop accumulated noise)"; hr
reset_ok=0
for i in $(seq 1 5); do
    if curl -sf -m 5 -X POST localhost:9000/traces/reset >/dev/null 2>&1; then
        reset_ok=1
        green "  ✓ webhook /traces reset"
        break
    fi
    sleep 2
done
if [[ $reset_ok -ne 1 ]]; then
    yellow "  ⤷ webhook unresponsive — restarting receiver"
    docker compose restart webhook_receiver >/dev/null 2>&1 || true
    sleep 8
    for i in $(seq 1 5); do
        if curl -sf -m 5 -X POST localhost:9000/traces/reset >/dev/null 2>&1; then
            green "  ✓ webhook restarted + /traces reset"
            break
        fi
        sleep 2
    done
fi

# ---- step 5: wait for services to be ready ---------------------------
hr; bold "5. Wait for services to be ready (max 60 s)"; hr
for i in $(seq 1 30); do
    if curl -sf -m 5 localhost:8080/healthz >/dev/null 2>&1 \
            && curl -sf -m 5 localhost:9000/traces >/dev/null 2>&1; then
        green "  ✓ gateway + webhook ready (after ${i}×2 s)"
        break
    fi
    printf "  ⏳ waiting... %d/30\r" "${i}"
    sleep 2
done
echo

if ! curl -sf -m 5 localhost:8080/healthz >/dev/null 2>&1; then
    red "  gateway never came up — check 'docker compose logs gateway'"
    exit 1
fi

# ---- step 6: container status ---------------------------------------
hr; bold "6. Container status"; hr
docker compose ps --format "table {{.Service}}\t{{.Status}}"

# ---- step 7: ingest mode --------------------------------------------
hr; bold "7. Gateway INGEST_MODE (force kafka for proposed-system demo)"; hr
mode=$(curl -s localhost:8080/healthz | python3 -c \
    "import sys,json; print(json.load(sys.stdin).get('ingest_mode','?'))")
echo "  current: ${mode}"
if [[ "${mode}" != "kafka" ]]; then
    yellow "  ⤷ flipping .env to INGEST_MODE=kafka and recreating gateway..."
    if grep -q '^INGEST_MODE=' .env; then
        sed -i 's/^INGEST_MODE=.*/INGEST_MODE=kafka/' .env
    else
        echo 'INGEST_MODE=kafka' >> .env
    fi
    docker compose up -d --no-deps --force-recreate gateway >/dev/null
    # wait for gateway to come back
    for i in $(seq 1 15); do
        m=$(curl -s -m 2 localhost:8080/healthz 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('ingest_mode','?'))" 2>/dev/null \
            || echo "?")
        if [[ "${m}" == "kafka" ]]; then
            green "  ✓ gateway recreated in kafka mode (after ${i}×2 s)"
            break
        fi
        sleep 2
    done
else
    green "  ✓ kafka mode (proposed system)"
fi

# ---- step 8: prepare demo PCAP ---------------------------------------
hr; bold "8. Prepare demo PCAP"; hr
PCAP_PATH="/tmp/demo_attack.pcap"
PCAP_SOURCE="${HOME}/pcap_CIC_IDS2017/Friday-WorkingHours.pcap"

if [[ -f "${PCAP_PATH}" ]]; then
    bytes=$(stat -c%s "${PCAP_PATH}" 2>/dev/null || stat -f%z "${PCAP_PATH}")
    green "  ✓ ${PCAP_PATH} already exists (${bytes} bytes)"
elif [[ -f "${PCAP_SOURCE}" ]]; then
    if command -v tshark >/dev/null 2>&1; then
        tshark -r "${PCAP_SOURCE}" -c 1000 -w "${PCAP_PATH}" 2>/dev/null
    elif command -v tcpdump >/dev/null 2>&1; then
        tcpdump -r "${PCAP_SOURCE}" -c 1000 -w "${PCAP_PATH}" 2>/dev/null
    else
        red "  install tshark or tcpdump: sudo apt install tshark"
        exit 1
    fi
    bytes=$(stat -c%s "${PCAP_PATH}" 2>/dev/null || stat -f%z "${PCAP_PATH}")
    green "  ✓ sliced 1000 packets → ${PCAP_PATH} (${bytes} bytes)"
else
    red "  source PCAP missing: ${PCAP_SOURCE}"
    yellow "  copy a CIC-IDS2017 PCAP to ~/pcap_CIC_IDS2017/ then re-run"
    exit 1
fi

# ---- step 9: final webhook reset (clear any stragglers from boot) ---
hr; bold "9. Final webhook reset"; hr
curl -sf -X POST localhost:9000/traces/reset >/dev/null
green "  ✓ /traces cleared"

# ---- ready ----------------------------------------------------------
echo
hr; bold "✓ READY FOR DEMO"; hr
echo
echo "  Run the demo now:"
echo "    bash scripts/demo_pcap.sh"
echo
echo "  Or open log streaming in another terminal first:"
echo "    docker compose logs -f --tail=0 gateway flow_extractor snort_sidecar detector alert_fanout webhook_receiver"
echo
