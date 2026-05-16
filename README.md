# ESNIDSaaS — Network Intrusion Detection System as a Service

Source-code release that accompanies the IMC 2026 paper
"ESNIDSaaS: Enabling Efficient and Scalable Network Intrusion
Detection System as a Service".

This repository contains everything required to reproduce the
detection numbers in the paper and to bring up the multi-tenant
streaming prototype on a single workstation. **Raw datasets and
pre-trained model artefacts are not included** — see
[Datasets](#2-datasets) below for the official download links.

---

## Contents at a glance

```
NIDSaaS/
├── README.md                       ← you are here
├── requirements.txt                ← Python dependencies for pipeline/
├── Final_locked_environment.txt    ← the locked experimental protocol
├── pipeline/                       ← detection algorithms (Python)
│   ├── README.md                   ← per-stage run order with commands
│   ├── OUTPUTS_INDEX.md            ← what each outputs_* directory contains
│   └── *.py                        ← cascade + baselines + utilities
├── snort/                          ← signature engine (Snort 3 wrapper)
│   ├── README.md
│   ├── rules/                      ← Talos community + project-local rules
│   └── *.py / *.md
├── prototype/                      ← SaaS streaming stack (Docker Compose)
│   ├── README.md                   ← stack overview + quickstart
│   ├── docker-compose.yml          ← Kafka + Spark + microservices
│   ├── .env.example                ← copy to .env before bringing up
│   └── <one folder per service>   ← each contains its own Dockerfile + source
└── scripts/                        ← paper plot helpers + env precheck
    ├── plot_training_time.py       ← Figure 5 bars (sklearn vs Spark)
    ├── plot_training_time_line.py  ← Figure 5 line variant
    └── precheck_detection.sh       ← env sanity check before training
```

---

## 1. What the system does

ESNIDSaaS is a **multi-tenant Network Intrusion Detection System
delivered as a cloud service**. Tenants upload raw PCAP traffic; the
service deduplicates it at the edge, normalises and aggregates it on
Spark, runs a hybrid-cascade detector in parallel with Snort, and
pushes structured alerts back to each tenant's webhook endpoint.

The two contributions are co-equal:

1. **A SaaS pipeline** that survives multi-tenant load — edge dedup,
   per-tenant Kafka topics, Spark Structured Streaming preprocessing,
   and webhook fan-out with delivery accounting.
2. **A hybrid-cascade detector** with a *val-accuracy-calibrated*
   threshold protocol that beats every single-stage baseline across
   three public datasets.

### 1.1 End-to-end architecture (matches Fig. 1 of the paper)

```
   ┌──────────┐ ┌──────────┐         (Google Cloud — service plane)
   │ tenant A │ │ tenant N │
   └────┬─────┘ └────┬─────┘
        └─────┬──────┘
        Raw PCAP files (multi-tenant ingestion over HTTPS)
              │
              ▼
   ┌──────────────────────┐
   │  Edge Deduplication  │   drop / trim / forward
   │  (drop-trim-forward) │   removes duplicate packet hashes per tenant
   └──────────┬───────────┘
              │ deduplicated chunks
              ▼
   ┌──────────────────────┐    OAuth2.0 client credentials per tenant
   │  Ingestion Gateway   │    issues JWT, enforces per-tenant quotas,
   │  (gateway/)          │    publishes to Kafka
   └──────────┬───────────┘
              ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  Apache Kafka 3.9 (KRaft) — per-tenant topics                   │
   │    tenant.{A,B,…,N}.pcap_chunks                                 │
   └──────────┬──────────────────────────────┬───────────────────────┘
              │ (PCAP chunks)                │ (PCAP chunks — parallel)
              ▼                              ▼
   ┌──────────────────────┐         ┌───────────────────────────┐
   │   Flow Extractor     │         │   Snort 3 sidecar         │
   │  (CICFlowMeter v4)   │         │  signature-based fast path│
   │  packet → flow rows  │         │  rules/{community,local}  │
   └──────────┬───────────┘         └──────────┬────────────────┘
              ▼                                │
   ┌──────────────────────┐                    │
   │   Apache Spark       │                    │
   │   Structured Stream  │                    │
   │   ┌────────────────┐ │                    │
   │   │ Schema normaliz│ │                    │
   │   │      ↓         │ │                    │
   │   │   Validation   │ │                    │
   │   │      ↓         │ │                    │
   │   │ Feature staging│ │                    │
   │   │      ↓         │ │                    │
   │   │ Windowed aggr. │ │                    │
   │   └────────────────┘ │                    │
   └──────────┬───────────┘                    │
              ▼                                │
        tenant.{u}.preprocessed                │
              │                                │
              ▼                                │
   ┌─────────────────────────────────────────────────────────────────┐
   │  Hybrid-Cascade Detector (streaming_worker/)                    │
   │    Rate rules  →  RF anomaly  →  Conformal calibration  →  GBDT │
   │                                                  Escalation Gate│
   │                                                       ↓         │
   │                              Validation-calibrated threshold τ* │
   └──────────┬──────────────────────────────────────────────────────┘
              ▼ alert with score s ≥ τ*
        tenant.{u}.alerts  (Kafka)
              │
              ▼
   ┌──────────────────────┐
   │   Alert Fan-out      │   per-tenant webhook delivery (HTTPS POST)
   │  (alert_fanout/)     │   retries, dead-letter, delivery accounting
   └──────────┬───────────┘
              ▼
        tenant {u} endpoint  (customer webhook receives the alert)

   Offline:  Training ML  →  retrains cascade joblibs;
             pushed to streaming_worker without restart.
```

### 1.2 Hybrid-Cascade detector in detail

The detector node in the diagram above expands to four ordered tiers.
The *ordering* is the contribution — Reviewer 2 of the IMC submission
asked us to motivate it, and the component ablation in `outputs_abl_*`
shows that removing any single tier or reordering hurts accuracy.

```
flow features in
    │
    ▼
┌────────────────────────────────────────────────────────────────┐
│ Tier 1 — Snort 3 signatures (community + custom rules)         │
│           emits 1 if any rule fires (fast path).               │
└────────────────────────────────────────────────────────────────┘
    │ (negatives only)
    ▼
┌────────────────────────────────────────────────────────────────┐
│ Tier 2 — Rate rules (SYN flood, port scan, etc.)               │
│           hand-crafted thresholds over flow counters.          │
└────────────────────────────────────────────────────────────────┘
    │ (negatives only)
    ▼
┌────────────────────────────────────────────────────────────────┐
│ Tier 3 — Self-supervised Random Forest anomaly score           │
│           + Split-conformal calibration → p-value in [0, 1].   │
└────────────────────────────────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────────────────────────────────┐
│ Tier 4 — Gradient-boosted escalation gate                      │
│           reads (p, tier1_flag, tier2_flag, meta-features).    │
└────────────────────────────────────────────────────────────────┘
    │
    ▼
    final score s ∈ [0,1]; alert if s ≥ τ*
```

`τ*` is **not tuned on the test set**. It is chosen on the validation
set to maximise validation accuracy (the *val-accuracy-calibrated*
protocol described in `Final_locked_environment.txt`) and then applied
unchanged to the held-out test set.

### 1.3 Code map: which folder implements which block

| Architecture block (Fig. 1) | Code path |
|---|---|
| Edge Deduplication | `prototype/gateway/` (drop-trim-forward filter) |
| Ingestion Gateway + OAuth2.0 | `prototype/gateway/` (`/ingest`, `/ingest_pcap`) |
| Apache Kafka topics | `prototype/init/` (KRaft bootstrap + topic creation) |
| Flow Extractor (packet→flow) | `prototype/flow_extractor/` (CICFlowMeter v4) |
| Snort 3 sidecar | `prototype/snort_sidecar/` (+ `snort/rules/`) |
| Spark preprocessing (4 stages) | `prototype/spark_preprocessor/` |
| Hybrid-Cascade Detector | `prototype/streaming_worker/` (online) and `pipeline/hybrid_cascade_splitcal_fastsnort.py` (offline) |
| Alert fan-out → webhooks | `prototype/alert_fanout/` + `prototype/webhook_receiver/` (demo) |
| Offline ML training | `pipeline/` (all Python entry-points) |
| Tenant simulator (load gen) | `prototype/tenant_simulator/` + `prototype/loadtest/` |

### 1.4 The two ways to run it

| Setting | Code path | Use |
|---|---|---|
| Offline experiments (paper numbers) | `pipeline/` | Reproduce all detection tables and figures |
| Online streaming prototype (SaaS) | `prototype/` | Demonstrate the end-to-end SaaS pipeline live |

---

## 2. Datasets

The three datasets used in the paper are **public** and must be
downloaded separately. After download, place each one next to the
`NIDSaaS_for_advisor/` folder (or wherever convenient — the scripts
take the directory as a `--data-dir` flag).

| Dataset | Used for | Format | Approx. size | Source |
|---|---|---|---|---|
| **CIC-IDS2017** | Main results, Snort, prototype | 8 daily CSVs | ~3 GB | https://www.unb.ca/cic/datasets/ids-2017.html |
| **Lycos2017** (Rosay et al., 2021) | Cleaner CIC-IDS2017 numbers — corrected CICFlowMeter bugs | Single CSV | ~2 GB | https://gitlab.inria.fr/rosay/lycos-ids2017 |
| **UNSW-NB15** | Cross-dataset generalisation | 4 CSVs (UNSW-NB15_1..4.csv, no header) + features file | ~600 MB | https://research.unsw.edu.au/projects/unsw-nb15-dataset |

The loader (`pipeline/load_data.py`) auto-detects the dataset by
sniffing column names, so the *same* training/evaluation script works
on all three with no flag changes. The expected directory layouts:

```
csv_CIC_IDS2017/
    Monday-WorkingHours.pcap_ISCX.csv
    Tuesday-WorkingHours.pcap_ISCX.csv
    Wednesday-workingHours.pcap_ISCX.csv
    Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv
    Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv
    Friday-WorkingHours-Morning.pcap_ISCX.csv
    Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv
    Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv

csv_Lycos2017/
    lycos-ids2017.csv

csv_UNSW_NB15/
    UNSW-NB15_1.csv
    UNSW-NB15_2.csv
    UNSW-NB15_3.csv
    UNSW-NB15_4.csv
```

The pcap captures of CIC-IDS2017 are only needed if you want to
re-run Snort from scratch (see `snort/README.md`). For reproducing
the detection numbers, the pre-computed Snort signature CSV is the
only Snort artefact you need — and it is regenerated by the Snort
stage if you have the pcaps.

---

## 3. Locked experimental protocol

All numbers reported in the paper follow the protocol in
`Final_locked_environment.txt`:

| Aspect | Value |
|---|---|
| Split | `temporal_by_file` (CIC), `random_stratified` (Lycos/UNSW for fair benchmark) |
| Ratios | 64 % train · 16 % val · 20 % test |
| Random seed | 42 |
| Threshold | τ* = argmax accuracy on D_val (tie-safe achievable-cut search) |
| Test-set policy | Used exactly **once** per method, for reporting only |
| Headline metric | Test-set accuracy at val-calibrated τ* |
| Supporting metrics | Precision, Recall, F1, FAR, ROC-AUC, PR-AUC |

The val-accuracy-calibrated protocol is implemented uniformly in
`pipeline/proposed_method_valcal.py` and reused by every baseline so
that the comparison is apples-to-apples.

---

## 4. Installation

Tested on Ubuntu 22.04 inside WSL 2, Python 3.10 / 3.11.

```bash
cd NIDSaaS

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

`requirements.txt` covers the pipeline. The prototype runs entirely
in Docker containers — no host Python packages are required for it.

Optional extras:
- **PyTorch** is already in `requirements.txt`; used by the LSTM
  autoencoder baseline and the CLAD baseline.
- **Snort 3** is required only to regenerate the signature CSV
  offline. The pinned build recipe (Snort 3 master, libdaq master,
  Snort 3 community rules, Ubuntu 22.04) is in
  `prototype/snort_sidecar/Dockerfile` — use that as the reference
  if you install Snort 3 directly on the host. Per-script flags are
  documented in `snort/snort_runner.md`.
- **CLAD baseline (Wilkie et al., IEEE TNSM 2026)** — clone the
  upstream repo so its `model/` and `losses/` packages are importable
  by our adapter: `git clone https://github.com/jackwilkie/CLOSR.git ../CLOSR`
- **Docker Desktop / Docker Engine + Compose v2** for the prototype.

---

## 5. Reproducing the paper — step by step

Below is the canonical command sequence on **CIC-IDS2017**. The same
commands work on Lycos2017 and UNSW-NB15 by changing `--data-dir` —
no other flag needs to change because the loader auto-detects schema.

All commands assume `cd NIDSaaS/pipeline` and that the
virtualenv is active.

### Stage 1 — Snort signatures (skip if you already have the CSV)

```bash
# Run Snort 3 against the CIC-IDS2017 pcaps; emits per-flow predictions.
# Requires Snort 3 + the pcaps. See snort/snort_runner.md for details.
python3 ../snort/snort_runner.py \
    --pcap-dir ../pcap_CIC_IDS2017 \
    --rules-dir ../snort/rules \
    --out-dir   ../snort/outputs_snort_eval_v4a
```

The output consumed by the cascade is:

```
../snort/outputs_snort_eval_v4a/snort_signature_predictions.csv
```

### Stage 2 — Train the hybrid cascade (the main experiment)

```bash
python3 hybrid_cascade_splitcal_fastsnort.py \
    --data-dir ../csv_CIC_IDS2017 \
    --snort-predictions ../snort/outputs_snort_eval_v4a/snort_signature_predictions.csv \
    --output-dir outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50 \
    --alpha-conformal 0.05 --alpha-escalate 0.20 --gate-threshold 0.50 \
    --calibration-fraction 0.50 --split-strategy temporal_by_file
```

Runtime: ~30–45 min on a 16-core CPU. Produces:
- `rf_anomaly.joblib`, `gate.joblib`, `conformal.joblib` — saved cascade
- `val_cascade_predictions.csv` and `test_cascade_predictions.csv`
- `cascade_export.joblib.tar.gz` — bundled artefact for the prototype

### Stage 3 — Apply the val-accuracy-calibrated threshold

```bash
python3 proposed_method_valcal.py \
    --val-csv  outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/val_cascade_predictions.csv \
    --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/test_cascade_predictions.csv \
    --out-dir  outputs_proposed_locked_rate_promoted \
    --rate-rules-csv signature_merged_predictions.csv \
    --calibrate-isotonic
```

The headline test-set metrics are written to:

```
outputs_proposed_locked_rate_promoted/overall_metrics.json
```

### Stage 4 — Baselines on the same protocol

```bash
# Random Forest + Random Forest + Conformal (reuses cascade RF scores; <1 min)
python3 rf_baseline_valcal.py \
    --val-csv  outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/val_cascade_predictions.csv \
    --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/test_cascade_predictions.csv \
    --out-dir  outputs_rf_baseline_valcal

# Rate-rule-only ablation
python3 rate_rules_baseline_valcal.py \
    --val-csv  outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/val_cascade_predictions.csv \
    --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/test_cascade_predictions.csv \
    --out-dir  outputs_rate_rules_baseline_valcal

# OCSVM / Isolation Forest / LSTM-AE under val-calibrated tau* (~30-50 min CPU)
python3 compare_anomaly_baselines_valcal.py \
    --data-dir ../csv_CIC_IDS2017 \
    --out-dir  outputs_baselines_temporal_by_file_valcal_iso \
    --split-strategy temporal_by_file --seed 42 \
    --headline-operating-point val_accuracy_calibrated --lstm-device cpu
```

### Stage 5 — Recent IEEE journal baseline: CLAD

CLAD = Contrastive Learning for Anomaly Detection (Wilkie et al.,
IEEE TNSM 2026). The model + loss come from the original authors'
public repository — we keep our adapter (`closr_baseline_valcal.py`)
in this repo but the upstream code must be cloned alongside:

```bash
# Original CLAD authors' repo — required to import ContrastiveMLP + CLADLoss
git clone https://github.com/jackwilkie/CLOSR.git ../CLOSR

# Our adapter applies CLAD on top of the locked val-cal protocol
python3 closr_baseline_valcal.py \
    --data-dir ../csv_CIC_IDS2017 --closr-repo ../CLOSR \
    --out-dir  outputs_closr_baseline_temporal \
    --split-strategy temporal_by_file --seed 42

# Apply the same val-calibrated tau* protocol
python3 proposed_method_valcal.py \
    --val-csv  outputs_closr_baseline_temporal/val_closr_predictions.csv \
    --test-csv outputs_closr_baseline_temporal/test_closr_predictions.csv \
    --out-dir  outputs_closr_baseline_temporal_valcal \
    --calibrate-isotonic --method-name "CLAD"
```

GPU recommended (~90 min). CPU is possible but very slow.

### Stage 6 — Cascade component ablation (Reviewer 2 response)

Five variants demonstrating that the *ordering* of the cascade is what
matters, not the strength of any single tier:

```bash
for variant in no_signature no_rate_rules no_conformal no_gate; do
    python3 hybrid_cascade_splitcal_fastsnort.py \
        --data-dir ../csv_CIC_IDS2017 \
        --snort-predictions ../snort/outputs_snort_eval_v4a/snort_signature_predictions.csv \
        --output-dir outputs_abl_${variant} \
        --alpha-conformal 0.05 --alpha-escalate 0.20 --gate-threshold 0.50 \
        --calibration-fraction 0.50 --split-strategy temporal_by_file \
        --${variant//_/-}     # toggles --no-signature / --no-rate-rules / etc.
    python3 proposed_method_valcal.py \
        --val-csv  outputs_abl_${variant}/val_cascade_predictions.csv \
        --test-csv outputs_abl_${variant}/test_cascade_predictions.csv \
        --out-dir  outputs_abl_${variant}_valcal --calibrate-isotonic \
        --method-name "Ablation: ${variant}"
done
```

### Cross-dataset runs

The same Stage 2 + 3 commands work on the other two datasets — just
change the data directory:

```bash
# Lycos2017 (Rosay et al. 2021 — corrected CIC-IDS2017)
python3 hybrid_cascade_splitcal_fastsnort.py \
    --data-dir ../csv_Lycos2017 \
    --output-dir outputs_lycos_hybrid_cascade_a20_g50 \
    --split-strategy random_stratified \
    --alpha-conformal 0.05 --alpha-escalate 0.20 --gate-threshold 0.50
# (no --snort-predictions: Lycos has no matching pcaps)

# UNSW-NB15
python3 hybrid_cascade_splitcal_fastsnort.py \
    --data-dir ../csv_UNSW_NB15 \
    --output-dir outputs_unsw_hybrid_cascade_a20_g50 \
    --split-strategy random_stratified \
    --alpha-conformal 0.05 --alpha-escalate 0.20 --gate-threshold 0.50
```

---

## 6. Expected results (test-set accuracy at val-calibrated τ*)

The numbers reported in the IMC 2026 camera-ready, for reference
during reproduction:

| Method | CIC-IDS2017 | Lycos2017 | UNSW-NB15 |
|---|---|---|---|
| Rate-rules only | 0.74 | 0.78 | 0.61 |
| Isolation Forest | 0.81 | 0.83 | 0.55 |
| One-Class SVM | 0.79 | 0.85 | 0.58 |
| LSTM autoencoder | 0.85 | 0.86 | 0.51 |
| Random Forest (sup.) | 0.94 | 0.97 | 0.91 |
| RF + Conformal | 0.94 | 0.97 | 0.91 |
| CLAD (Wilkie 2026) | 0.93 | 0.96 | 0.89 |
| **Hybrid-Cascade (ours)** | **0.96** | **0.9999** | **0.93** |

Variation up to ±0.01 across runs is normal (different sklearn
versions, BLAS implementations). All numbers reported here are with
seed 42 and the requirements pinned in `requirements.txt`.

---

## 7. Running the SaaS prototype

The prototype is a Docker Compose stack that mirrors the architecture
figure in the paper: multi-tenant ingestion → Kafka → streaming
cascade → per-tenant webhooks.

```bash
cd prototype
cp .env.example .env       # then edit secrets (see comments inside)
./scripts/quickstart.sh
```

In another terminal, fire a synthetic attack:

```bash
./scripts/demo_attack.sh
```

Expected: a JSON alert ending with `"tier": "tier1_rate"` arrives at
the tenant's webhook within a few seconds.

Full details — services, ports, ingestion modes, load test driver —
are in `prototype/README.md`.

---

## 8. Where to look in the paper

### 8.1 SaaS pipeline (architecture, deployment, throughput)

| Paper element | Code path |
|---|---|
| Figure 1 (system architecture) | `prototype/docker-compose.yml` defines the topology; per-block mapping in §1.3 above |
| Edge deduplication (drop-trim-forward) | `prototype/gateway/` |
| OAuth2.0 multi-tenant ingestion | `prototype/gateway/` (`/ingest`, `/ingest_pcap`) |
| Per-tenant Kafka topics (`tenant.{u}.*`) | `prototype/init/` (KRaft bootstrap, topic creation) |
| Spark Structured Streaming preprocessing | `prototype/spark_preprocessor/` (schema-norm → validation → feature-stage → windowed-aggr) |
| Flow extraction (packet → flow) | `prototype/flow_extractor/` (CICFlowMeter v4 sidecar) |
| Online cascade detector | `prototype/streaming_worker/` (loads cascade joblibs, applies τ\*) |
| Alert fan-out + delivery accounting | `prototype/alert_fanout/` + `prototype/webhook_receiver/` |
| Tenant simulator (load gen, attack injection) | `prototype/tenant_simulator/` |
| Load test driver + throughput / latency results | `prototype/loadtest/driver.py`, `prototype/loadtest/plot_figures.py` |
| Kafka vs Direct-HTTP comparison | `prototype/loadtest/` (per-tenant delivery JSONs) |
| Offline retraining benchmark (sklearn vs Spark MLlib) | `prototype/spark_experiment/mllib/` (raw timings → `results.csv`) |
| Figure 5 plot (sklearn vs Spark training time) | `scripts/plot_training_time.py` + `scripts/plot_training_time_line.py` |
| Environment precheck (verifies all deps before training) | `scripts/precheck_detection.sh` |

### 8.2 Detection algorithm (cascade, calibration, ablation)

| Paper element | Code path |
|---|---|
| Algorithm 1 (Hybrid-Cascade) | `pipeline/hybrid_cascade_splitcal_fastsnort.py` |
| Tier 1 signature engine | `snort/` + `pipeline/signature_rate_rules.py` |
| Tier 2 rate rules | `pipeline/signature_rate_rules.py` + `pipeline/rate_rules_baseline_valcal.py` |
| Tier 3 self-supervised RF | `pipeline/rf_anomaly.py` |
| Tier 3 split-conformal calibration | `pipeline/conformal_wrapper.py` |
| Tier 4 GBDT escalation gate | `pipeline/escalation_gate_fastsnort.py` |
| Val-accuracy-calibrated τ\* protocol | `pipeline/proposed_method_valcal.py` |
| Table II (main results, 3 datasets × 8 methods) | Stages 3–5 above |
| Table III (component ablation) | Stage 6 above |
| CLAD baseline (Wilkie et al., IEEE TNSM 2026) | `pipeline/closr_baseline_valcal.py` |
| Cross-dataset loader (CIC / Lycos / UNSW auto-detect) | `pipeline/load_data.py` |

---

## 9. What's NOT in this repository (external dependencies)

This repository contains all code we wrote. A few components are
external and must be obtained from their authors / upstreams:

| External dependency | What it provides | How to obtain |
|---|---|---|
| **CIC-IDS2017 dataset** | Daily flow CSVs + raw pcaps | https://www.unb.ca/cic/datasets/ids-2017.html |
| **Lycos2017 dataset** | Corrected CIC-IDS2017 (Rosay et al. 2021) | https://gitlab.inria.fr/rosay/lycos-ids2017 |
| **UNSW-NB15 dataset** | 4 daily CSVs (no header) | https://research.unsw.edu.au/projects/unsw-nb15-dataset |
| **CLAD reference implementation** (Wilkie et al., IEEE TNSM 2026) | `ContrastiveMLP` model + `CLADLoss` — imported by our adapter `pipeline/closr_baseline_valcal.py` | `git clone https://github.com/jackwilkie/CLOSR.git` |
| **Snort 3** + **libdaq** + **Snort community rules** | Signature detection engine (Tier 1). Recipe pinned in `prototype/snort_sidecar/Dockerfile` | Either run that Dockerfile, or install on host from https://github.com/snort3/snort3 |
| **CICFlowMeter v4** | Packet → flow extraction inside the prototype | Built via Gradle inside `prototype/flow_extractor/Dockerfile` (no manual install) |
| **Apache Kafka 3.9** (KRaft mode) | Multi-tenant streaming bus | Pulled automatically by `prototype/docker-compose.yml` |
| **Apache Spark** | Structured Streaming preprocessing + MLlib retraining | Same: pulled by Docker Compose |

All other code in the paper — Random Forest, Isolation Forest,
One-Class SVM, LSTM autoencoder, rate rules, split-conformal
calibration, the GBDT escalation gate, and the SaaS pipeline glue —
is our own implementation and lives in this repository.

---

## 10. Contact

Chaiyapat Anantarasuchart — `chaipatanantarasuchart@gmail.com`

Issues with reproduction are easiest to debug when accompanied by:
- the exact command you ran,
- the contents of the script's `outputs_*/overall_metrics.json`, and
- your Python version (`python3 -V`) and `pip freeze` output.
