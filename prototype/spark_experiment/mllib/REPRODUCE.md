# Reproducing the NIDSaaS Spark Experiments

This guide walks a fresh user through reproducing **all three Spark
experiments** on the SIIT server (10.10.11.96 / hostname `siit`):

1. Architecture validation — proposed model deployed end-to-end
2. Throughput sweep — sustained load characterisation
3. sklearn vs Spark MLlib retraining benchmark

Total wall-clock time, including image build and dataset generation,
is roughly **2 hours**. If the Docker images and synthetic datasets
already exist on the server (someone else already ran the experiments
once), this drops to **~30 minutes**.

---

## 0. Prerequisites

You need:

* Network access to `10.10.11.96` (on SIIT campus or via VPN)
* SSH credentials for user `siit` (ask the admin)
* About 30 GB of free disk on the server (datasets are kept under
  `/tmp/synth/`)

Already installed on the server (don't re-install):

* Docker 28.2 + Docker Compose v2
* Python 3.10
* Apache Spark 3.5.4 image (`apache/spark:3.5.4-python3`)
* Custom image `nidsaas-spark-mllib:3.5.4`
* `~/NIDSaaS-Earth/.venv` — Python venv with `httpx`, `numpy`,
  `pyarrow`, `pandas`, `scikit-learn`, `matplotlib`

---

## 1. Get the code on the server

If `~/NIDSaaS-Earth/` already exists, skip to Step 2.

Otherwise, from your laptop (where the source tree lives):

```bash
cd /path/to/NIDSaaS_Experiment      # the folder containing prototype/

# 1. Pack the prototype + lean pipeline modules
tar --exclude='prototype/__pycache__' \
    --exclude='prototype/**/__pycache__' \
    --exclude='prototype/**/*.pyc' \
    --exclude='prototype/loadtest/runs' \
    --exclude='prototype/.env' \
    -czf nidsaas-prototype.tar.gz prototype/

tar --exclude='pipeline/__pycache__' \
    --exclude='pipeline/**/__pycache__' \
    --exclude='pipeline/**/*.pyc' \
    --exclude='pipeline/*.csv' \
    --exclude='pipeline/**/*.csv' \
    -czf pipeline-lean.tar.gz pipeline/

# 2. Push to the server
ssh siit@10.10.11.96 'mkdir -p ~/NIDSaaS-Earth'
scp nidsaas-prototype.tar.gz pipeline-lean.tar.gz \
    siit@10.10.11.96:~/NIDSaaS-Earth/

# 3. Unpack on the server
ssh siit@10.10.11.96 '
    cd ~/NIDSaaS-Earth &&
    tar -xzf nidsaas-prototype.tar.gz &&
    tar -xzf pipeline-lean.tar.gz &&
    mkdir -p csv_CIC_IDS2017 pcap_CIC_IDS2017
'
```

The `csv_CIC_IDS2017` and `pcap_CIC_IDS2017` directories are
required as Docker mount targets even though we use synthetic data
and never write into them.

---

## 2. Bring up the NIDSaaS stack (with Spark)

```bash
ssh siit@10.10.11.96
cd ~/NIDSaaS-Earth/prototype

# 1. Make sure .env has the credentials and topic suffix the rest of
#    the stack expects. Idempotent — safe to re-run.
cat > .env <<'EOF'
INGEST_MODE=kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
LOG_LEVEL=INFO
OAUTH_CLIENTS=acme:acme-secret;globex:globex-secret;initech:initech-secret
TENANTS=acme,globex,initech
GATEWAY_JWT_SECRET=earth-prototype-jwt-secret
WEBHOOKS=acme:http://webhook_receiver:9000/acme;globex:http://webhook_receiver:9000/globex;initech:http://webhook_receiver:9000/initech
DETECT_TAU_STAR=0.0
EOF

# 2. Build images (first time ~5 min, cached afterwards ~15 s)
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    build

# 3. Bring up the full stack including the Spark preprocessor
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    up -d

# 4. Wait for everything to be healthy (Spark needs ~60 s)
sleep 60

# 5. Sanity check
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml ps
curl -s localhost:8080/healthz | python3 -m json.tool
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    logs spark_preprocessor --tail=5
```

You should see:

* All services `Up`, `spark_preprocessor` not `Restarting`
* Gateway `/healthz` returns `"ingest_mode": "kafka"` plus the three
  tenants
* Spark log ends with `[spark-preprocessor] streaming started`

If `spark_preprocessor` is restarting, see the troubleshooting
section at the bottom.

---

## 3. Activate the Python venv

Most experiment scripts run from the host (not inside Docker) and
need the project venv:

```bash
source ~/NIDSaaS-Earth/.venv/bin/activate
which python3        # should point inside .venv/bin
```

---

## 4. Experiment A — Architecture validation (5 min)

Verifies the Spark-augmented pipeline delivers every flow end-to-end
at a low offered rate.

```bash
cd ~/NIDSaaS-Earth/prototype

curl -X POST localhost:9000/traces/reset
echo

python3 loadtest/run_experiment.py e1 \
    --mode kafka \
    --rate 30 --duration 60 --settle 20 \
    | tee /tmp/synth/spark_arch_validate.log

tail -10 /tmp/synth/spark_arch_validate.log
```

Expected: `delivered=1800/1800 (100.0%)` with `p50` somewhere in the
150–250 ms range.

---

## 5. Experiment B — Throughput sweep (10 min)

Sweeps the offered load to find the sustained-throughput ceiling.

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

    tail -3 /tmp/synth/throughput_sweep/rate${rate}.log \
        | grep -E "sent=|e2e ms"
    sleep 5
done
```

The 500 and 1000~req/s rates take 5–10 minutes each to time out
(latency tail is in the minutes), so the full sweep takes roughly
20 minutes.

---

## 6. Experiment C — sklearn vs Spark MLlib (60 min)

This is the heaviest experiment. It generates synthetic parquet
datasets at three sizes (0.1, 0.25, 0.5 GB) and trains a
RandomForest with each engine. The NIDSaaS stack should be **stopped**
during this experiment so Spark gets the full 16 GB of RAM:

```bash
cd ~/NIDSaaS-Earth/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml stop
free -h     # should show ~14 GB available

cd ~/NIDSaaS-Earth/prototype/spark_experiment/mllib

# Run inside tmux so it survives an SSH disconnect
tmux new -s sparkbench

SIZES="0.1 0.25 0.5" \
N_EST=100 MAX_DEPTH=15 \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
SPARK_DRIVER_MEM=12g \
    ./run_benchmark.sh 2>&1 | tee /tmp/synth/bench.log

# Detach: Ctrl-b d
# Re-attach later: tmux a -t sparkbench
```

Datasets generate at ~220k rows/s and persist in
`/tmp/synth/sweep/*.parquet` between runs.

When the sweep is done, restart the NIDSaaS stack:

```bash
cd ~/NIDSaaS-Earth/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml start
```

---

## 7. Pull results back and plot

From your laptop:

```bash
cd /path/to/NIDSaaS_Experiment

# 1. Get the consolidated CSV (sklearn + Spark MLlib timings)
scp siit@10.10.11.96:/tmp/synth/results.csv \
    prototype/spark_experiment/mllib/results.csv

# 2. Render figures
cd prototype/spark_experiment/mllib
python3 plot_results.py    --csv results.csv --out-dir .
python3 plot_throughput.py --out-dir .

ls fig_*.pdf
```

You'll get three PDFs ready to drop into the paper:

* `fig_sklearn_vs_spark_train.pdf` — train-time vs dataset size
* `fig_sklearn_vs_spark_speedup.pdf` — side-by-side bar comparison
* `fig_throughput_sweep.pdf` — bars + dual-axis latency lines

`plot_throughput.py` has its data hard-coded inside the script; if
you re-run the throughput sweep with different numbers, edit the
`DATA = [...]` table at the top before re-plotting.

---

## 8. Tear down

When you're completely done with the server:

```bash
ssh siit@10.10.11.96
cd ~/NIDSaaS-Earth/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml down -v
```

Add `-v` only if you want to wipe the Kafka topic data too. The
`/tmp/synth/` datasets are kept regardless — delete with
`rm -rf /tmp/synth/sweep /tmp/synth/throughput_sweep` if disk
pressure matters.

---

## Troubleshooting

### Spark preprocessor keeps restarting

```bash
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    logs spark_preprocessor --tail=80
```

Common causes:

* `FileNotFoundException: /home/spark/.ivy2/cache/...` — the Maven
  resolution cache is unwritable. Confirm that the Dockerfile
  contains `RUN mkdir -p /opt/ivy && chmod 777 /opt/ivy` and that
  the spark-submit CMD uses `--conf spark.jars.ivy=/opt/ivy`. Rebuild
  the image after fixing.
* `OutOfMemoryError: Java heap space` — bump the per-container memory
  limit in `docker-compose.spark.prod.yml` (`mem_limit`) or the
  `spark.driver.memory` config in the Dockerfile CMD.

### Detector still subscribes to `tenant.*.raw`

If you see `[worker] raw consumer started` (without the
`(suffix='preprocessed')` annotation) the patched `worker.py` did
not make it into the image. Verify:

```bash
ssh siit@10.10.11.96 'grep -c INPUT_TOPIC_SUFFIX \
    ~/NIDSaaS-Earth/prototype/streaming_worker/worker.py'
# Expect: 4
```

If the count is 0, copy the patched file from the laptop:

```bash
scp prototype/streaming_worker/worker.py \
    siit@10.10.11.96:~/NIDSaaS-Earth/prototype/streaming_worker/worker.py
ssh siit@10.10.11.96 '
    cd ~/NIDSaaS-Earth/prototype &&
    docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
        build --no-cache detector &&
    docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
        up -d --force-recreate detector
'
```

### sklearn dies at 1 GB+

Expected. sklearn's pandas + numpy + train_test_split path peaks
around 28× the on-disk parquet size, and the server has 16 GB of
RAM. This is documented in the paper as the cold-path single-node
ceiling that motivates the Spark MLlib choice — it is not a bug.

### Spark MLlib also dies at 1 GB+

Also expected on this 16 GB single-node hardware. Spark's columnar
cache plus the persisted vector representation also exceed the JVM
heap. The paper notes that horizontal scale-out via Spark cluster
mode lifts this ceiling; we do not benchmark cluster mode here.
