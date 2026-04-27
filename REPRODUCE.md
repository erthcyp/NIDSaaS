# Reproducing the NIDSaaS Spark Experiments

This guide walks a fresh user through reproducing **all three Spark
experiments**:

1. Architecture validation — proposed model deployed end-to-end
2. Throughput sweep — sustained load characterisation
3. sklearn vs Spark MLlib retraining benchmark

You can run this on either the **SIIT server** (the configuration
the paper used) **or your own laptop**. The workflow is the same
either way — at each step we show both the server command and the
laptop command, side by side.

Total wall-clock time, including image build and dataset generation,
is roughly **2 hours** from a cold start. If the Docker images and
synthetic datasets already exist (someone else already ran the
experiments once), this drops to **~30 minutes**.

---

## 0a. Prerequisites — SIIT server

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

> Throughout this guide, "**SERVER:**" blocks assume you are inside
> an `ssh siit@10.10.11.96` session.

## 0b. Prerequisites — Local laptop (no server)

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| OS | Linux, macOS, or Windows + WSL2 | same |
| CPU cores | 4 | 8+ |
| RAM | 8 GB | 16 GB+ |
| Disk free | 20 GB | 40 GB |
| Docker | Desktop 4.20+ or Engine 24+ | latest |
| Python | 3.10+ | 3.10–3.12 |
| Git | any recent version | — |

**Windows users:** install Docker Desktop, enable WSL2 backend, and
in **Settings → Resources** allocate at least 8 GB RAM (12 GB+ if
you want to try Experiment 3).

> "**LOCAL:**" blocks below assume you have already cloned the repo
> and your shell is in the project root.

---

## 1. Get the code

### SERVER

If `~/NIDSaaS-Earth/` already exists, skip to Step 2. Otherwise from
your laptop:

```bash
# Pack the prototype + lean pipeline modules
cd /path/to/NIDSaaS_Experiment      # the folder containing prototype/
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

# Push to the server
ssh siit@10.10.11.96 'mkdir -p ~/NIDSaaS-Earth'
scp nidsaas-prototype.tar.gz pipeline-lean.tar.gz \
    siit@10.10.11.96:~/NIDSaaS-Earth/

# Unpack on the server
ssh siit@10.10.11.96 '
    cd ~/NIDSaaS-Earth &&
    tar -xzf nidsaas-prototype.tar.gz &&
    tar -xzf pipeline-lean.tar.gz &&
    mkdir -p csv_CIC_IDS2017 pcap_CIC_IDS2017
'
```

### LOCAL

```bash
# Clone (or copy if you already have it)
git clone https://github.com/<username>/nidsaas.git
cd nidsaas

# The compose mounts expect these to exist (even when empty —
# we use synthetic data, not the real CIC-IDS2017 dataset)
mkdir -p csv_CIC_IDS2017 pcap_CIC_IDS2017
```

---

## 2. Bring up the NIDSaaS stack (with Spark)

The same steps work on both targets. The only difference is the
working directory.

### SERVER

```bash
ssh siit@10.10.11.96
cd ~/NIDSaaS-Earth/prototype
```

### LOCAL

```bash
cd nidsaas/prototype
```

### Both

```bash
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

# 2. Build images (first time ~5–10 min, cached afterwards ~15 s)
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

The experiment scripts run from the host (not inside Docker) and
need the project venv.

### SERVER

```bash
source ~/NIDSaaS-Earth/.venv/bin/activate
which python3        # should point inside .venv/bin
```

### LOCAL

If you don't have a venv yet, create one in the repo root:

```bash
cd /path/to/nidsaas      # back to repo root, not prototype/
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r prototype/loadtest/requirements.txt
pip install numpy pyarrow pandas scikit-learn matplotlib
which python3            # should point inside ./.venv/bin
```

If the venv already exists from a previous run:

```bash
source /path/to/nidsaas/.venv/bin/activate
```

---

## 4. Experiment A — Architecture validation (5 min)

Verifies the Spark-augmented pipeline delivers every flow end-to-end
at a low offered rate. Identical commands on server and laptop.

```bash
cd <repo>/prototype     # ~/NIDSaaS-Earth/prototype on server
                        # /path/to/nidsaas/prototype on laptop

curl -X POST localhost:9000/traces/reset
echo

python3 loadtest/run_experiment.py e1 \
    --mode kafka \
    --rate 30 --duration 60 --settle 20 \
    | tee /tmp/synth/spark_arch_validate.log

tail -10 /tmp/synth/spark_arch_validate.log
```

**Expected:** `delivered=1800/1800 (100.0%)` with `p50` somewhere
in the 150–500 ms range (a bit higher on laptop due to Docker
Desktop's virtualisation overhead on Windows / macOS).

---

## 5. Experiment B — Throughput sweep (~10–20 min)

Sweeps the offered load to find the sustained-throughput ceiling.

### SERVER (full sweep)

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

The 500 and 1000 req/s rates take 5–10 minutes each because their
latency tails extend into the multi-minute range, so the full sweep
takes roughly 20 minutes.

### LOCAL (lighter sweep)

A typical laptop saturates well before 500 req/s. Use this lighter
sweep instead:

```bash
cd /path/to/nidsaas/prototype
mkdir -p /tmp/synth/throughput_sweep

for rate in 25 50 100 150 200; do
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

Total wall-clock ~10 min on a 16 GB laptop.

---

## 6. Experiment C — sklearn vs Spark MLlib (~60 min)

This is the heaviest experiment. It generates synthetic parquet
datasets and trains a RandomForest with each engine. **Stop the
NIDSaaS stack first** so the benchmark gets the full RAM:

### Both — stop the stack

```bash
cd <repo>/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml stop
free -h     # Linux/WSL — should show most RAM as available
```

### LOCAL only — build the Spark MLlib image (one-time)

The server already has `nidsaas-spark-mllib:3.5.4` baked in. On the
laptop you need to build it once:

```bash
cd <repo>/prototype/spark_experiment/mllib

cat > Dockerfile <<'EOF'
FROM apache/spark:3.5.4-python3
USER root
RUN pip install --no-cache-dir numpy pandas pyarrow
USER spark
EOF
docker build -t nidsaas-spark-mllib:3.5.4 .
```

(~3 min on a typical laptop with a decent network connection.)

### Run the sweep

```bash
cd <repo>/prototype/spark_experiment/mllib

# Run inside tmux so it survives an SSH disconnect (server)
# or terminal close (laptop). On laptop you can skip tmux if you
# leave the terminal open the whole time.
tmux new -s sparkbench
```

#### SERVER

```bash
SIZES="0.1 0.25 0.5" \
N_EST=100 MAX_DEPTH=15 \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
SPARK_DRIVER_MEM=12g \
    ./run_benchmark.sh 2>&1 | tee /tmp/synth/bench.log
```

#### LOCAL — 16 GB laptop

```bash
SIZES="0.1 0.25 0.5" \
N_EST=100 MAX_DEPTH=15 \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
SPARK_DRIVER_MEM=8g \
    ./run_benchmark.sh 2>&1 | tee /tmp/synth/bench.log
```

#### LOCAL — 8 GB laptop

```bash
SIZES="0.05 0.1 0.25" \
N_EST=100 MAX_DEPTH=15 \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
SPARK_DRIVER_MEM=4g \
    ./run_benchmark.sh 2>&1 | tee /tmp/synth/bench.log
```

`tmux` controls: detach with `Ctrl-b d`, re-attach with
`tmux a -t sparkbench`. On Windows + WSL, `tmux` isn't installed by
default — run `sudo apt install tmux` first.

Datasets generate at ~220k rows/s and persist in
`/tmp/synth/sweep/*.parquet` between runs.

When the sweep is done, restart the NIDSaaS stack:

```bash
cd <repo>/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml start
```

---

## 7. Pull results back and plot

### SERVER → laptop

```bash
# On laptop
cd /path/to/NIDSaaS_Experiment

# Get the consolidated CSV (sklearn + Spark MLlib timings)
scp siit@10.10.11.96:/tmp/synth/results.csv \
    prototype/spark_experiment/mllib/results.csv
```

### LOCAL only

`results.csv` is already in
`prototype/spark_experiment/mllib/results.csv` — nothing to copy.

### Both — plot

```bash
cd <repo>/prototype/spark_experiment/mllib

python3 plot_results.py    --csv results.csv --out-dir .
python3 plot_throughput.py --out-dir .

ls fig_*.pdf
```

You'll get three PDFs ready to drop into the paper:

* `fig_sklearn_vs_spark_train.pdf` — train-time line plot
* `fig_sklearn_vs_spark_speedup.pdf` — side-by-side bar comparison
* `fig_throughput_sweep.pdf` — bars + dual-axis latency lines

`plot_throughput.py` has its data hard-coded inside the script; if
you re-run the throughput sweep with different numbers, edit the
`DATA = [...]` table at the top before re-plotting.

---

## 8. Tear down

When you're completely done:

### Both

```bash
cd <repo>/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml down -v
```

Add `-v` only if you want to wipe the Kafka topic data too. The
`/tmp/synth/` datasets are kept regardless — delete with
`rm -rf /tmp/synth/sweep /tmp/synth/throughput_sweep` if disk
pressure matters.

### LOCAL — extra cleanup if you want the disk back

```bash
docker system prune -a   # removes all unused images (~3–4 GB freed)
rm -rf /tmp/synth        # removes synthetic datasets
deactivate               # exit the venv
```

---

## Differences cheat sheet

| Aspect | SIIT server | Laptop |
|--------|-------------|--------|
| Hardware | 12 cores / 16 GB | 4–8 cores / 8–16 GB |
| Docker network overhead | ~0 ms (bare metal) | ~50 ms (Docker Desktop) |
| Throughput ceiling | ~300 req/s | typically 100–200 req/s |
| Cold-path benchmark sizes | 0.1, 0.25, 0.5 GB | 0.05, 0.1, 0.25 (8 GB box) |
| Spark JVM heap | 12 GB | 4–8 GB |
| Python venv path | `~/NIDSaaS-Earth/.venv` | `./.venv` (project-local) |
| Spark MLlib image | pre-built `nidsaas-spark-mllib:3.5.4` | build with the Dockerfile in Step 6 |

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
grep -c INPUT_TOPIC_SUFFIX prototype/streaming_worker/worker.py
# Expect: 4
```

If the count is 0, rebuild the detector with `--no-cache`:

```bash
cd prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    build --no-cache detector
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    up -d --force-recreate detector
```

### sklearn dies at 1 GB+ (server) / 0.25–0.5 GB (laptop)

Expected. sklearn's pandas + numpy + train_test_split path peaks
around 28× the on-disk parquet size. On a 16 GB server this caps at
1 GB; on an 8 GB laptop it caps at roughly 0.25 GB. This is
documented in the paper as the cold-path single-node ceiling that
motivates the Spark MLlib choice — it is not a bug.

### Spark MLlib also dies at 1 GB+ on single-node

Also expected. Spark's columnar cache plus the persisted vector
representation also exceed the JVM heap. The paper notes that
horizontal scale-out via Spark cluster mode lifts this ceiling; we
do not benchmark cluster mode here.

### Windows / WSL: `curl localhost:8080` times out from PowerShell

Try from inside WSL first (`wsl` then `curl localhost:8080/healthz`).
If WSL works, the issue is Windows-side firewall or VPN. Add a
`localhost` exception or disable the VPN temporarily.

### Windows / WSL: shell scripts fail with `\r: command not found`

Cloned via Git for Windows with default CRLF settings. Convert:

```bash
dos2unix prototype/scripts/*.sh
```

Or set Git to check out LF:

```bash
git config --global core.autocrlf input
```
