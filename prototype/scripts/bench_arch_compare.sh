#!/usr/bin/env bash
# bench_arch_compare.sh
# ----------------------------------------------------------------------
# A/B end-to-end latency benchmark: Python-microservice (no Spark) vs
# the proposed-with-Spark architecture (gateway → Kafka → Spark → Kafka
# → detector). Yo the same synthetic load at both variants, capture the
# p50/p95/p99 end-to-end latency from each, and emit a side-by-side
# summary table.
#
# Usage:
#   chmod +x scripts/bench_arch_compare.sh
#   ./scripts/bench_arch_compare.sh
#   RATE=50 DURATION=60 ./scripts/bench_arch_compare.sh
# ----------------------------------------------------------------------
set -uo pipefail

cd "$(dirname "$0")/.."   # cd to prototype/

RATE="${RATE:-30}"
DURATION="${DURATION:-60}"
SETTLE="${SETTLE:-15}"
OUT_DIR="${OUT_DIR:-/tmp/bench_arch}"

mkdir -p "$OUT_DIR"

green()  { printf "\033[32m%s\033[0m\n" "$*"; }
red()    { printf "\033[31m%s\033[0m\n" "$*"; }
yellow() { printf "\033[33m%s\033[0m\n" "$*"; }
bold()   { printf "\033[1m%s\033[0m\n" "$*"; }
hr()     { printf "\033[2m%s\033[0m\n" "────────────────────────────────────────────────"; }

bold "Architecture A/B latency benchmark"
echo "  rate     = $RATE req/s"
echo "  duration = $DURATION s"
echo "  settle   = $SETTLE s"
echo "  out_dir  = $OUT_DIR"
echo

# ------------------------------------------------------------------
# Helper: yo one load run, parse summary, write to result file.
# ------------------------------------------------------------------
run_load () {
    local label="$1"
    local out_json="$OUT_DIR/${label}.json"

    yellow "  ⏳ resetting webhook traces ..."
    curl -sf -X POST localhost:9000/traces/reset >/dev/null

    yellow "  ⏳ load test (${label}) ..."
    python3 loadtest/run_experiment.py e1 \
        --mode kafka \
        --rate "$RATE" --duration "$DURATION" --settle "$SETTLE" \
        --gateway http://localhost:8080 \
        --webhook http://localhost:9000 \
        2>&1 | tee "$OUT_DIR/${label}.log" \
             | grep -E "sent=|e2e ms" \
             | tail -2

    # The load harness writes its own JSON in loadtest/outputs/. Copy
    # the latest e1_* file into our result dir for archival.
    local latest=$(ls -1t loadtest/outputs/e1_*.json | head -1)
    cp "$latest" "$out_json"
    green "  ✓ saved $out_json"
    echo
}

# ------------------------------------------------------------------
# Variant A — no Spark (current Python-microservice)
# ------------------------------------------------------------------
hr; bold "Variant A — Python μS (no Spark)"; hr
docker compose -f docker-compose.yml down -v >/dev/null 2>&1 || true
docker compose -f docker-compose.yml up -d
sleep 35

# Sanity check — make sure detector is on .raw
docker compose exec detector env | grep -q "DETECT_INPUT_TOPIC_SUFFIX" \
    && yellow "  detector input suffix: $(docker compose exec detector env | grep DETECT_INPUT_TOPIC_SUFFIX || echo 'raw (default)')" \
    || yellow "  detector input suffix: raw (default)"
docker compose stop tenant_simulator >/dev/null 2>&1 || true

run_load "A_nospark"

# ------------------------------------------------------------------
# Variant B — with Spark preprocessor
# ------------------------------------------------------------------
hr; bold "Variant B — with Spark preprocessor"; hr
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml down -v >/dev/null 2>&1 || true
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml up -d
echo "  ⏳ waiting 60 s for Spark to start consuming ..."
sleep 60

docker compose exec detector env | grep DETECT_INPUT_TOPIC_SUFFIX
docker compose stop tenant_simulator >/dev/null 2>&1 || true

run_load "B_spark"

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
hr; bold "Summary"; hr
echo
python3 - <<EOF
import json, sys
from pathlib import Path

def summary(path):
    with open(path) as f:
        d = json.load(f)
    s = d.get("summary", d)   # tolerate either flat or nested
    return {
        "sent":     s.get("sent", 0),
        "delivered": s.get("delivered", 0),
        "p50":     s.get("e2e_p50_ms", s.get("p50", "-")),
        "p95":     s.get("e2e_p95_ms", s.get("p95", "-")),
        "p99":     s.get("e2e_p99_ms", s.get("p99", "-")),
        "max":     s.get("e2e_max_ms", s.get("max", "-")),
    }

a = summary("$OUT_DIR/A_nospark.json")
b = summary("$OUT_DIR/B_spark.json")

cols = ["variant", "sent", "delivered", "p50_ms", "p95_ms", "p99_ms", "max_ms"]
def row(name, s):
    return [name, s["sent"], s["delivered"], s["p50"], s["p95"], s["p99"], s["max"]]

rows = [row("A_nospark", a), row("B_spark", b)]

w = [max(len(str(r[i])) for r in [cols] + rows) for i in range(len(cols))]
def fmt(r): return "  ".join(str(c).ljust(w[i]) for i, c in enumerate(r))
print(fmt(cols))
print("  ".join("-" * x for x in w))
for r in rows: print(fmt(r))
print()
if isinstance(a["p50"], (int, float)) and isinstance(b["p50"], (int, float)) and a["p50"] > 0:
    print(f"Spark adds: p50 +{b['p50']-a['p50']:.1f}ms ({b['p50']/a['p50']:.2f}x), "
          f"p95 +{b['p95']-a['p95']:.1f}ms ({b['p95']/a['p95']:.2f}x)")
EOF
