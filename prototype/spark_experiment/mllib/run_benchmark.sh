#!/usr/bin/env bash
# run_benchmark.sh
# ----------------------------------------------------------------------
# Spark MLlib vs sklearn retraining benchmark — full sweep.
#
# For each dataset size in SIZES:
#   1. Generate synthetic parquet via synth_dataset_gen.py (skip if exists)
#   2. Train RF with sklearn (native venv)
#   3. Train RF with Spark MLlib (via Docker — apache/spark:3.5.4-python3)
#
# Both trainers append rows to the same results CSV so we can plot a
# single comparison figure: dataset size (x) vs train time (y), one line
# per engine.
#
# Usage:
#   chmod +x run_benchmark.sh
#   ./run_benchmark.sh                    # default 0.5/1/2/5 GB sweep
#   SIZES="1 5 10" ./run_benchmark.sh     # custom sweep
#   N_EST=200 MAX_DEPTH=15 ./run_benchmark.sh   # tweak hyperparams
# ----------------------------------------------------------------------
set -uo pipefail

cd "$(dirname "$0")"

# ---------- knobs ----------
SIZES="${SIZES:-0.5 1 2 5}"
N_EST="${N_EST:-100}"
MAX_DEPTH="${MAX_DEPTH:-15}"
SYNTH_DIR="${SYNTH_DIR:-/tmp/synth/sweep}"
RESULT_CSV="${RESULT_CSV:-/tmp/synth/results.csv}"
SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.4-python3}"
SPARK_DRIVER_MEM="${SPARK_DRIVER_MEM:-12g}"
SKIP_SKLEARN="${SKIP_SKLEARN:-0}"
SKIP_SPARK="${SKIP_SPARK:-0}"
# ---------------------------

green()  { printf "\033[32m%s\033[0m\n" "$*"; }
red()    { printf "\033[31m%s\033[0m\n" "$*"; }
yellow() { printf "\033[33m%s\033[0m\n" "$*"; }
bold()   { printf "\033[1m%s\033[0m\n" "$*"; }
hr()     { printf "\033[2m%s\033[0m\n" "────────────────────────────────────────────────"; }

mkdir -p "$SYNTH_DIR" "$(dirname "$RESULT_CSV")"

bold "Spark MLlib vs sklearn benchmark"
echo "  sizes        = $SIZES (GB)"
echo "  n_estimators = $N_EST"
echo "  max_depth    = $MAX_DEPTH"
echo "  synth_dir    = $SYNTH_DIR"
echo "  result_csv   = $RESULT_CSV"
echo "  spark_image  = $SPARK_IMAGE"
echo

# Make sure Spark image is local (avoids docker pull mid-benchmark)
if [[ "$SKIP_SPARK" != "1" ]]; then
    if ! docker image inspect "$SPARK_IMAGE" >/dev/null 2>&1; then
        yellow "Pulling $SPARK_IMAGE (~700 MB, one-time) ..."
        docker pull "$SPARK_IMAGE"
    fi
fi

# ============================================================
# Step 1 — generate datasets
# ============================================================
hr; bold "Step 1: Generate synthetic datasets"; hr
for sz in $SIZES; do
    out="$SYNTH_DIR/${sz}gb.parquet"
    if [[ -f "$out" ]]; then
        bytes=$(stat -c%s "$out" 2>/dev/null || stat -f%z "$out")
        green "  ✓ $out exists ($((bytes/1024/1024)) MB) — skipping gen"
    else
        yellow "  generating ${sz} GB → $out ..."
        python3 synth_dataset_gen.py --size-gb "$sz" --output "$out"
    fi
done

# ============================================================
# Step 2 — sklearn benchmark
# ============================================================
if [[ "$SKIP_SKLEARN" != "1" ]]; then
    hr; bold "Step 2: sklearn RF training"; hr
    for sz in $SIZES; do
        parquet="$SYNTH_DIR/${sz}gb.parquet"
        echo
        bold ">>> sklearn @ ${sz} GB"
        python3 train_sklearn.py \
            --input "$parquet" \
            --n-estimators "$N_EST" \
            --max-depth "$MAX_DEPTH" \
            --result-csv "$RESULT_CSV"
    done
else
    yellow "Skipping sklearn (SKIP_SKLEARN=1)"
fi

# ============================================================
# Step 3 — Spark MLlib benchmark (via Docker)
# ============================================================
if [[ "$SKIP_SPARK" != "1" ]]; then
    hr; bold "Step 3: Spark MLlib RF training"; hr
    for sz in $SIZES; do
        parquet="$SYNTH_DIR/${sz}gb.parquet"
        echo
        bold ">>> spark_mllib @ ${sz} GB"

        # Mount the data dir and the mllib code dir into the container.
        # Both .py files live in the same dir as this script.
        docker run --rm \
            -v "$(realpath "$(dirname "$SYNTH_DIR")"):/data" \
            -v "$(pwd):/app" \
            "$SPARK_IMAGE" \
            /opt/spark/bin/spark-submit \
                --master "local[*]" \
                --conf spark.driver.memory="$SPARK_DRIVER_MEM" \
                --conf spark.driver.maxResultSize=2g \
                --conf spark.sql.adaptive.enabled=true \
                /app/train_spark_mllib.py \
                    --input "/data/$(basename "$SYNTH_DIR")/${sz}gb.parquet" \
                    --n-estimators "$N_EST" \
                    --max-depth "$MAX_DEPTH" \
                    --result-csv "/data/$(basename "$RESULT_CSV")"
    done
else
    yellow "Skipping Spark (SKIP_SPARK=1)"
fi

# ============================================================
# Summary
# ============================================================
hr; bold "Summary"; hr
if [[ -f "$RESULT_CSV" ]]; then
    green "Results CSV: $RESULT_CSV"
    column -ts, "$RESULT_CSV"
else
    red "No result CSV produced"
fi
