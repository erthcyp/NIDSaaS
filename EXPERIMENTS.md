# Running the Experiments

This guide walks through running **each of the three experiments**
reported in the paper, one at a time. For first-time setup of the
server / stack itself, see [`REPRODUCE.md`](./REPRODUCE.md).

| # | Experiment | Wall-clock | Where the results land |
|---|------------|-----------|------------------------|
| 1 | Architecture validation | ~2 min | webhook trace store + console |
| 2 | Throughput sweep | ~10 min | `/tmp/synth/throughput_sweep/*.log` |
| 3 | sklearn vs Spark MLlib | ~60 min | `/tmp/synth/results.csv` |

---

## Common prerequisites (do this once per session)

Before any experiment, make sure the stack is up and the venv is
activated.

```bash
ssh siit@10.10.11.96
cd ~/NIDSaaS-Earth/prototype

# 1. Bring the with-Spark stack up (if not already)
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml ps
# If services are missing or "Exited":
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml up -d
sleep 60

# 2. Sanity check
curl -s localhost:8080/healthz | python3 -m json.tool

# 3. Activate the Python venv
source ~/NIDSaaS-Earth/.venv/bin/activate
which python3      # should point inside .venv
```

You should see `"ingest_mode": "kafka"` and three tenants registered.

---

## Experiment 1 — Architecture validation

**Goal**: prove the proposed architecture (with Apache Spark in the
data path) delivers alerts end-to-end, with a small load.

### Run

```bash
cd ~/NIDSaaS-Earth/prototype

# Reset webhook trace store so the count starts at 0
curl -X POST localhost:9000/traces/reset
echo

# Yo a single 30 req/s load for 60 s + 20 s settle
python3 loadtest/run_experiment.py e1 \
    --mode kafka \
    --rate 30 --duration 60 --settle 20 \
    | tee /tmp/synth/arch_validate.log
```

### Expected output (last 10 lines)

```
[loadtest] sent=1800 accepted=1800 deduped=0 failed=0 delivered=1800 (100.0%)
[loadtest] e2e ms: p50=200-220  p95=400-500  p99=600-800
[loadtest]   acme       sent=1800 delivered=1800 (100.0%) ...
```

The headline number is **delivered=1800/1800 (100.0%)** — every flow
record traversed the full pipeline (gateway → Kafka → Spark → Kafka
→ detector → alert\_fanout → webhook).

### Inspect the alerts

```bash
curl -s localhost:9000/traces | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'total: {d.get(\"count\", 0)}')
sample = next(iter(d.get('traces', {}).values()), [None])[0]
print(json.dumps(sample, indent=2))"
```

### One-liner (for advisor demos)

The same experiment plus narrated steps and pauses:

```bash
bash scripts/demo_sequential.sh
```

This is the single-terminal walkthrough — six numbered steps with
`press ENTER to continue` between each.

---

## Experiment 2 — Throughput sweep

**Goal**: characterise the throughput ceiling of the with-Spark
architecture. Sweep offered load from 50 to 1000 req/s and measure
delivery rate + latency.

### Run

```bash
cd ~/NIDSaaS-Earth/prototype
mkdir -p /tmp/synth/throughput_sweep

for rate in 50 100 200 500 1000; do
    echo "==== Rate ${rate} req/s ===="
    curl -sf -X POST localhost:9000/traces/reset >/dev/null

    python3 loadtest/run_experiment.py e1 \
        --mode kafka \
        --rate ${rate} --duration 30 --settle 15 \
        > /tmp/synth/throughput_sweep/rate${rate}.log 2>&1

    grep -E "sent=|e2e ms" /tmp/synth/throughput_sweep/rate${rate}.log \
        | tail -2 | sed "s/^/  rate=${rate}: /"

    sleep 5
done
```

The 500 and 1000 req/s rates take 5–10 minutes each because their
latency tails extend into the multi-minute range, so the full sweep
takes roughly 20 minutes.

### Expected output

```
==== Rate 50 req/s ====
  rate=50: sent=1500 delivered=1500 (100.0%) p50=157 p95=292 p99=428
==== Rate 100 req/s ====
  rate=100: sent=3000 delivered=3000 (100.0%) p50=144 p95=218 p99=256
==== Rate 200 req/s ====
  rate=200: sent=6000 delivered=6000 (100.0%) p50=3142 p95=5461 p99=6340
==== Rate 500 req/s ====
  rate=500: sent=4494 delivered=1053  (23.4%) p50=2223 p95=605059 p99=605469
==== Rate 1000 req/s ====
  rate=1000: sent=3827 delivered=568  (14.8%) p50=243103 p95=547107 p99=547386
```

### Three regimes to read out of this

| Regime | Rate | Delivery | Reading |
|--------|------|----------|---------|
| Sustained | 50–100 req/s | 100\,% | production target — p99 < 300 ms |
| Saturating | 200 req/s | 100\,% | working but latency 10× higher |
| Overload | 500+ req/s | < 25\,% | Kafka backlog collapses delivery |

### Plot the figure

The `plot_throughput.py` script has the data points hard-coded near
the top. Edit `DATA = [...]` if you re-run the sweep with different
numbers, then:

```bash
# On laptop (pull script from server, or use local copy)
python3 prototype/spark_experiment/mllib/plot_throughput.py --out-dir .
```

Output: `fig_throughput_sweep.{pdf,png}`

---

## Experiment 3 — sklearn vs Spark MLlib retraining

**Goal**: justify the inclusion of Apache Spark MLlib in the cold-path
retraining stage by demonstrating where each engine wins and where
each fails. Sweep three synthetic dataset sizes (0.1, 0.25, 0.5 GB)
and train a RandomForest with each.

### One-time prep — stop the NIDSaaS stack

The benchmark needs the full 16 GB RAM for Spark. Stop the live
NIDSaaS containers first (you can restart them at the end):

```bash
cd ~/NIDSaaS-Earth/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml stop
free -h    # should show ~14 GB available
```

### Run the full sweep

The benchmark takes ~60 minutes. Run it in `tmux` so an SSH
disconnect doesn't kill it:

```bash
cd ~/NIDSaaS-Earth/prototype/spark_experiment/mllib

tmux new -s sparkbench

# Inside tmux:
SIZES="0.1 0.25 0.5" \
N_EST=100 MAX_DEPTH=15 \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
SPARK_DRIVER_MEM=12g \
    ./run_benchmark.sh 2>&1 | tee /tmp/synth/bench.log

# Detach: Ctrl-b d  (session keeps running)
# Re-attach later: tmux a -t sparkbench
```

### What the script does

For each `(engine, size)` combination:

1. Generate a synthetic parquet dataset if `${size}gb.parquet`
   doesn't exist (~10 s per 0.1 GB).
2. Train a RandomForest with `n_estimators=100`, `max_depth=15`.
3. Evaluate on a 20\,% held-out test split.
4. Append a row to `/tmp/synth/results.csv`.

sklearn results are emitted from the host venv; Spark MLlib results
come from the `nidsaas-spark-mllib:3.5.4` Docker container.

### Expected results

| Size | sklearn | Spark MLlib | Speedup |
|------|---------|-------------|---------|
| 0.1 GB | ~94 s | ~125 s | sklearn wins (1.33×) |
| 0.25 GB | ~275 s | ~163 s | **Spark wins (1.69×)** |
| 0.5 GB | ~620 s | ~395 s | **Spark wins (1.57×)** |
| 1+ GB | OOM | OOM (single-node) | both fail |

### Run only one engine

```bash
# Skip sklearn, run Spark only
SKIP_SKLEARN=1 SIZES="0.5" \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
SPARK_DRIVER_MEM=12g \
    ./run_benchmark.sh

# Skip Spark, run sklearn only
SKIP_SPARK=1 SIZES="0.1 0.25" ./run_benchmark.sh
```

### Run a custom size

```bash
# Generate one parquet file
python3 synth_dataset_gen.py --size-gb 0.05 --output /tmp/synth/sweep/0.05gb.parquet

# Train sklearn on it
python3 train_sklearn.py \
    --input /tmp/synth/sweep/0.05gb.parquet \
    --n-estimators 100 --max-depth 15 \
    --result-csv /tmp/synth/results.csv
```

### Pull results back to the laptop and plot

```bash
# On laptop
cd /path/to/NIDSaaS_Experiment
scp siit@10.10.11.96:/tmp/synth/results.csv \
    prototype/spark_experiment/mllib/results.csv

cd prototype/spark_experiment/mllib
python3 plot_results.py --csv results.csv --out-dir .
```

Output:
* `fig_sklearn_vs_spark_train.pdf` — train-time line plot
* `fig_sklearn_vs_spark_speedup.pdf` — side-by-side bar comparison

### Cleanup

After the benchmark, restart the NIDSaaS stack:

```bash
cd ~/NIDSaaS-Earth/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml start
```

If disk pressure matters, drop the synthetic datasets:

```bash
rm -rf /tmp/synth/sweep /tmp/synth/throughput_sweep
df -h /tmp
```

The Docker image `nidsaas-spark-mllib:3.5.4` weighs ~2 GB; remove
with `docker rmi nidsaas-spark-mllib:3.5.4` if you don't plan to
re-run the Spark MLlib experiment.

---

## Re-running everything end-to-end (paper figures)

If you need to regenerate every figure in the paper from scratch:

```bash
# 1. On the server, run all three experiments
ssh siit@10.10.11.96
cd ~/NIDSaaS-Earth/prototype

# Architecture validation (Experiment 1)
curl -X POST localhost:9000/traces/reset
python3 loadtest/run_experiment.py e1 --mode kafka \
    --rate 30 --duration 60 --settle 20

# Throughput sweep (Experiment 2)
mkdir -p /tmp/synth/throughput_sweep
for rate in 50 100 200 500 1000; do
    curl -sf -X POST localhost:9000/traces/reset >/dev/null
    python3 loadtest/run_experiment.py e1 --mode kafka \
        --rate ${rate} --duration 30 --settle 15 \
        > /tmp/synth/throughput_sweep/rate${rate}.log 2>&1
    sleep 5
done

# sklearn vs Spark MLlib (Experiment 3)
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml stop
cd spark_experiment/mllib
tmux new -s bench
SIZES="0.1 0.25 0.5" SPARK_DRIVER_MEM=12g \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
    ./run_benchmark.sh 2>&1 | tee /tmp/synth/bench.log
# (wait ~60 min, then Ctrl-b d to detach)

# 2. On the laptop, pull results and plot
cd /path/to/NIDSaaS_Experiment
scp siit@10.10.11.96:/tmp/synth/results.csv \
    prototype/spark_experiment/mllib/results.csv

cd prototype/spark_experiment/mllib
python3 plot_results.py --csv results.csv --out-dir .
python3 plot_throughput.py --out-dir .

ls fig_*.pdf
# fig_sklearn_vs_spark_train.pdf
# fig_sklearn_vs_spark_speedup.pdf
# fig_throughput_sweep.pdf
```

---

## Troubleshooting

See the troubleshooting section at the end of [`REPRODUCE.md`](./REPRODUCE.md).
The two most common issues:

* **Spark preprocessor restarting** — Maven cache permission, fixed
  by Dockerfile flag `--conf spark.jars.ivy=/opt/ivy`.
* **Detector subscribed to `tenant.*.raw` instead of `.preprocessed`** —
  the patched `worker.py` did not make it into the detector image;
  rebuild with `docker compose ... build --no-cache detector`.
