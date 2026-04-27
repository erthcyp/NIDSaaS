# Running Locally (no server required)

This guide covers running the full NIDSaaS prototype + all three
experiments on a **single laptop**, no SIIT server access needed.

The same Docker Compose stack runs identically on a developer
workstation; only the resource limits and a few quality-of-life
shortcuts change. The numbers in the paper were collected on the
SIIT server (12 cores / 16 GB), so local runs may show somewhat
different latency / throughput depending on your hardware.

---

## Prerequisites

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| OS | Linux, macOS, or Windows + WSL2 | Same |
| CPU cores | 4 | 8+ |
| RAM | 8 GB | 16 GB+ |
| Disk free | 20 GB | 40 GB |
| Docker | Desktop 4.20+ or Engine 24+ | Latest |
| Python | 3.10+ | 3.10–3.12 |
| Git | Any recent version | — |

### Windows + WSL2 setup

1. Install **Docker Desktop** (https://www.docker.com/products/docker-desktop)
2. Settings → Resources → set
   * **CPUs**: ≥ 4
   * **Memory**: ≥ 8 GB (12 GB+ if you want to try Experiment 3)
   * **Swap**: 2 GB
   * **Disk image size**: ≥ 60 GB
3. Settings → General → enable **"Use the WSL 2 based engine"**
4. Settings → Resources → WSL Integration → enable for your WSL distro
5. Open WSL terminal and verify:
   ```bash
   docker info
   docker run --rm hello-world
   ```

### macOS / Linux

```bash
# Install Docker (if not already)
# macOS: brew install --cask docker
# Linux: see https://docs.docker.com/engine/install/

# Verify
docker info
docker compose version
```

---

## 1. Get the code

```bash
# Clone (or copy if you already have it)
git clone https://github.com/erthcyp/nidsaas.git
cd nidsaas
```

Or if you already have the source tree on this machine, skip the
clone and just `cd` to the project directory.

---

## 2. Set up the Python venv

```bash
python3 -m venv .venv
source .venv/bin/activate

# Install everything the experiments / harness need
pip install --upgrade pip
pip install -r prototype/loadtest/requirements.txt
pip install numpy pyarrow pandas scikit-learn matplotlib
```

If `pip install` complains about PEP 668 / "externally managed
environment", add `--break-system-packages` (or just stay inside the
venv as above — recommended).

---

## 3. Bring up the Docker stack

The repo ships two compose files:

* `prototype/docker-compose.yml` — base 8-service stack (no Spark)
* `prototype/docker-compose.spark.prod.yml` — overlay that inserts
  the Spark preprocessor on the hot path

For first-time runs, build everything once:

```bash
cd prototype

# Create the .env file the stack expects
cat > .env <<'EOF'
INGEST_MODE=kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
LOG_LEVEL=INFO
OAUTH_CLIENTS=acme:acme-secret;globex:globex-secret;initech:initech-secret
TENANTS=acme,globex,initech
GATEWAY_JWT_SECRET=local-demo-jwt-secret
WEBHOOKS=acme:http://webhook_receiver:9000/acme;globex:http://webhook_receiver:9000/globex;initech:http://webhook_receiver:9000/initech
DETECT_TAU_STAR=0.0
EOF

# Create dataset placeholder dirs (mounted by tenant_simulator,
# even when empty — synthetic data path doesn't read from them)
mkdir -p ../csv_CIC_IDS2017 ../pcap_CIC_IDS2017

# Build images (~5–10 min the first time)
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    build

# Bring up the with-Spark stack
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    up -d

# Wait for Spark to finish initialising (~60 s)
sleep 60

# Sanity check
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml ps
curl -s localhost:8080/healthz | python3 -m json.tool
```

You should see `"ingest_mode": "kafka"` and three tenants registered.

---

## 4. Run the experiments locally

### Experiment 1 — Architecture validation (~2 min)

Identical to the server flow:

```bash
cd prototype
source ../.venv/bin/activate

curl -X POST localhost:9000/traces/reset

python3 loadtest/run_experiment.py e1 \
    --mode kafka \
    --rate 30 --duration 60 --settle 20
```

**Expected on a typical 8-core / 16 GB laptop:**
* `delivered=1800/1800 (100.0%)`
* `p50` somewhere in the 200–500 ms range (a bit higher than the
  SIIT server because Docker Desktop adds ~50 ms of virtualisation
  overhead on Windows / macOS)

### Experiment 2 — Throughput sweep (~10 min)

Same loop as the server version, but lower the upper rate to match
laptop capacity:

```bash
mkdir -p /tmp/synth/throughput_sweep

# On a laptop, sweep 25 → 200 req/s (skip 500/1000 — they will
# saturate well before then with less RAM available to Spark)
for rate in 25 50 100 150 200; do
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

### Experiment 3 — sklearn vs Spark MLlib (limited on laptop)

This is the experiment most affected by laptop hardware. **Stop the
NIDSaaS stack first** so Spark can use all available RAM:

```bash
cd prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml stop
free -h    # Linux/WSL only — should show most of your RAM as available
```

Build the Spark MLlib image (one-time, ~3 min):

```bash
cd spark_experiment/mllib

# Use the same Dockerfile we use on the server
cat > Dockerfile <<'EOF'
FROM apache/spark:3.5.4-python3
USER root
RUN pip install --no-cache-dir numpy pandas pyarrow
USER spark
EOF
docker build -t nidsaas-spark-mllib:3.5.4 .
```

Run the sweep — adjust `SIZES` and `SPARK_DRIVER_MEM` to your laptop:

```bash
# Conservative: 8 GB laptop
SIZES="0.05 0.1 0.25" \
SPARK_DRIVER_MEM=4g \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
    bash run_benchmark.sh

# More headroom: 16 GB laptop
SIZES="0.1 0.25 0.5" \
SPARK_DRIVER_MEM=8g \
SPARK_IMAGE=nidsaas-spark-mllib:3.5.4 \
    bash run_benchmark.sh
```

**On a 16 GB laptop:** sklearn will OOM at 0.5 GB (same single-node
ceiling as the server reports). Spark MLlib similarly hits its JVM
heap ceiling around the same size. The relative comparison
(sklearn faster on small data, Spark faster on medium data) still
holds — just at smaller absolute sizes.

**On an 8 GB laptop:** stick to the 0.05–0.1 GB sizes and treat the
benchmark as a smoke-test rather than a full reproduction.

After the benchmark, restart the NIDSaaS stack:

```bash
cd ~/path/to/nidsaas/prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml start
```

---

## 5. Plot the figures

Same as the server flow, but `results.csv` is already on the laptop:

```bash
cd prototype/spark_experiment/mllib

python3 plot_results.py    --csv results.csv --out-dir .
python3 plot_throughput.py --out-dir .

ls fig_*.pdf
# fig_sklearn_vs_spark_train.pdf
# fig_sklearn_vs_spark_speedup.pdf
# fig_throughput_sweep.pdf
```

If `plot_throughput.py`'s hard-coded `DATA = [...]` doesn't match
your local sweep, edit it at the top of the file before re-running.

---

## 6. Demo (sequential, no server)

The single-terminal walkthrough script works locally as well:

```bash
cd prototype
bash scripts/demo_sequential.sh
```

It runs through six numbered steps with `press ENTER to continue`
between each, so you can narrate it for an advisor or committee
without juggling tmux panes.

---

## 7. Tear down

```bash
cd prototype
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml down
```

Add `-v` to also wipe the Kafka topic data:

```bash
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml down -v
```

To free up disk fully:

```bash
docker system prune -a   # removes all unused images
rm -rf /tmp/synth        # removes synthetic datasets
```

---

## Differences from the server flow

| Aspect | SIIT server | Laptop |
|--------|-------------|--------|
| Hardware | 12 cores / 16 GB | 4–8 cores / 8–16 GB |
| Networking | bare-metal Linux | Docker Desktop adds ~50 ms |
| Throughput ceiling | ~300 req/s | typically 100–200 req/s |
| Cold-path benchmark | sweeps 0.1–0.5 GB | sweeps 0.05–0.25 GB |
| Spark JVM heap | 12 GB | 4–8 GB |
| Python venv | `~/NIDSaaS-Earth/.venv` | `./.venv` (project-local) |
| `OAUTH_CLIENTS` | from server `.env` | from this guide's heredoc |

---

## Windows / WSL2 gotchas

* **Port forwarding to `localhost`:** Docker Desktop maps
  `localhost:8080` → WSL2 → container, so `curl localhost:8080/healthz`
  works from both Windows PowerShell and WSL. If a port refuses
  connection, restart Docker Desktop.
* **Volume mount paths:** the compose files use relative paths
  (`../csv_CIC_IDS2017`, `../pipeline`) which work when you launch
  `docker compose` from inside `prototype/`. Don't run it from the
  repo root or the mounts will fail.
* **Line endings:** if you cloned via Git for Windows, `*.sh` scripts
  may have CRLF. Convert with `dos2unix prototype/scripts/*.sh`
  before running them.
* **Spark JVM memory:** Docker Desktop's memory limit is the hard
  cap. If `SPARK_DRIVER_MEM=8g` but Docker Desktop is set to 8 GB
  total, the container will OOM. Always leave 2–3 GB headroom for
  the JVM's non-heap overhead.

---

## Troubleshooting (laptop-specific)

### Docker Desktop "WSL integration not detected"

```powershell
# In Windows PowerShell
wsl --list --verbose
wsl --set-version Ubuntu 2     # if your distro is on WSL1
```

Then re-enable WSL Integration in Docker Desktop settings.

### `curl localhost:8080` from Windows times out

Try from inside WSL first:

```bash
wsl
curl localhost:8080/healthz
```

If that works, the issue is Windows-side firewall or VPN. Add a
`localhost` exception or disable the VPN temporarily.

### Spark preprocessor restart loop (Maven cache)

Same issue as the server — fixed by the Dockerfile in
`prototype/spark_preprocessor/Dockerfile` (the `--conf
spark.jars.ivy=/opt/ivy` patch). If you cloned the repo this is
already in place.

### sklearn / Spark MLlib OOM at smaller sizes than the server reports

Expected. The paper's 1 GB ceiling is for 16 GB hardware; on an
8 GB laptop the equivalent ceiling is roughly 0.25 GB. Reduce
`SIZES` accordingly and document your hardware in any reproduced
figures.

### `tmux: command not found`

Optional dependency only used by `demo_for_advisor.sh`. The
recommended single-terminal demo (`demo_sequential.sh`) works
without tmux.
