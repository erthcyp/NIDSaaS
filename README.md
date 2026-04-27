# NIDSaaS — Network Intrusion Detection as a Service

Reference implementation, benchmark scripts, and reproduction artefacts
for the Bachelor's-thesis paper:

> **NIDSaaS: A Lightweight Multi-Tenant Network Intrusion Detection
> System with Apache Spark Cold-Path Retraining**

The repository contains everything needed to (a) bring the proposed
system up on a single Linux server, (b) reproduce the two experiments
reported in the paper, and (c) inspect or extend the underlying
research code.

---

## What's in the box

```
git_submission/
├── README.md                      ← this file
├── REPRODUCE.md                   ← step-by-step reproduction guide
├── LICENSE                        ← MIT
├── .gitignore
│
├── prototype/                     ← THE NIDSaaS PROTOTYPE (Docker stack)
│   │
│   ├── docker-compose.yml                ← base 8-service stack (no Spark)
│   ├── docker-compose.spark.prod.yml     ← overlay that inserts Spark on the hot path
│   │
│   ├── gateway/                  ← FastAPI ingestion + OAuth2 + Kafka producer
│   ├── streaming_worker/         ← Detector — Hybrid Cascade + signature joiner
│   ├── flow_extractor/           ← CICFlowMeter sidecar (PCAP → flows)
│   ├── snort_sidecar/            ← Snort 3 sidecar (signature stage)
│   ├── alert_fanout/             ← consumes .alerts → POST tenant webhooks
│   ├── webhook_receiver/         ← test SIEM endpoint
│   ├── tenant_simulator/         ← optional CSV / PCAP traffic source
│   ├── spark_preprocessor/       ← Spark Structured Streaming preprocessor
│   │                                (Schema-Normalise + Validate + Feature-Stage)
│   │
│   ├── spark_experiment/
│   │   ├── streaming_app.py      ← latency-probe Spark app (Section IV.C of paper)
│   │   ├── probe.py              ← Kafka direct vs Kafka-through-Spark probe
│   │   ├── docker-compose.spark.yml
│   │   ├── Dockerfile
│   │   └── mllib/                ← COLD-PATH RETRAINING BENCHMARK
│   │       ├── synth_dataset_gen.py     ← synthetic CIC-IDS2017-style generator
│   │       ├── train_sklearn.py         ← sklearn baseline
│   │       ├── train_spark_mllib.py     ← Spark MLlib counterpart
│   │       ├── run_benchmark.sh         ← orchestrates the full sweep
│   │       ├── plot_results.py          ← renders the comparison figures
│   │       ├── plot_throughput.py       ← renders the throughput figure
│   │       └── REPRODUCE.md             ← detailed reproduction guide
│   │
│   ├── loadtest/                 ← load harness (synthetic flow producer)
│   │   ├── driver.py
│   │   ├── run_experiment.py     ← E1 / E2 / E5 scenarios from the paper
│   │   └── plot_figures.py
│   │
│   └── scripts/                  ← demo + automation
│       ├── warmup_demo.sh
│       ├── demo_pcap.sh
│       ├── demo_sequential.sh    ← step-by-step advisor demo (1 terminal)
│       ├── demo_for_advisor.sh   ← multi-pane tmux dashboard demo
│       ├── inspect_alerts.sh
│       └── precheck_detection.sh
│
├── pipeline/                     ← RESEARCH PIPELINE (offline training)
│   │                                Mounted into the detector container
│   │                                at /opt/pipeline.
│   ├── cascade_export_patch.py
│   ├── conformal_wrapper.py
│   ├── proposed_method_valcal.py
│   ├── rate_rules_baseline_valcal.py
│   ├── rf_anomaly.py
│   ├── signature_rate_rules.py
│   ├── ...                       ← other training & ablation scripts
│   └── outputs_proposed_locked_rate_promoted/  ← run config (.json)
│                                                 only — heavy CSVs are
│                                                 excluded from the repo
│                                                 (regenerable from the
│                                                 training scripts)
│
├── snort/                        ← SNORT 3 EVALUATION HARNESS (ablation)
│   ├── snort_runner.py                  ← replays PCAP through Snort
│   ├── snort_eval_fixed_v3_splitstrategy.py   ← evaluation pipeline
│   ├── parse_fast_alerts.py             ← parses Snort fast.log
│   ├── filter_policy_snort.py           ← v4a-FTP-only filter policy
│   ├── rules/                           ← Snort 3 community ruleset
│   └── README_SNORT_updated.md          ← Snort-specific notes
│                                          Eval outputs (~720 MB) are
│                                          excluded — regenerable.
│
└── paper/                        ← LATEX SECTIONS for the thesis
    ├── 01_retraining_benchmark.tex          ← Experiment 6 write-up
    ├── 02_throughput_validation.tex         ← Architecture validation +
    │                                          throughput sweep
    ├── 02b_throughput_results_focused.tex   ← Alternative Results paragraph
    └── sklearn_vs_spark_results.csv         ← raw measurements
```

---

## Architecture

```
                    ┌──────────────────────────────────────────┐
                    │                Tenants                   │
                    │  (acme, globex, initech — multi-tenant)  │
                    └────────────────────┬─────────────────────┘
                                         │  HTTPS POST /ingest (OAuth2)
                                         ▼
              ┌──────────────────────────────────────────────────┐
              │   Ingestion Gateway   (FastAPI + edge dedup)     │
              └────────────────────┬─────────────────────────────┘
                                   │  produce → tenant.{u}.raw
                                   ▼
                       ┌──────── Apache Kafka 3.9 ────────┐
                       │     KRaft single-node broker     │
                       └──┬─────────────────────────────┬─┘
                          │                             │
       tenant.{u}.raw     │                             │  tenant.{u}.signature
                          ▼                             │
              ┌────────────────────────────┐            │
              │  Spark Preprocessor        │            │
              │  (Schema Normalise +       │            │
              │   Validate + Stage)        │            │
              └────────────┬───────────────┘            │
                           │  tenant.{u}.preprocessed   │
                           ▼                            │
              ┌──────────────────────────────┐          │
              │   Hybrid Cascade Detector    │◀─────────┘
              │   • Tier-0 signature pass    │   ⤺  CICFlowMeter sidecar
              │   • Tier-1 rate rules        │      Snort 3 sidecar
              │   • Tier-2 RF + Conformal    │
              │              + GBDT gate     │
              └────────────┬─────────────────┘
                           │  tenant.{u}.alerts
                           ▼
              ┌──────────────────────────────┐
              │  Alert Fan-out  →  Webhook   │ → tenant SIEM
              └──────────────────────────────┘
```

**Cold-path retraining** (offline, periodic) runs in a separate
Spark MLlib pipeline that consumes the accumulated `.raw` /
`.preprocessed` history and emits an updated detector model. The
benchmark in `prototype/spark_experiment/mllib/` quantifies why
Spark MLlib is the right engine for this workload at production scale.

---

## Quick start (existing server)

If the SIIT server already has the code unpacked under
`~/NIDSaaS-Earth/`:

```bash
ssh siit@10.10.11.96
cd ~/NIDSaaS-Earth/prototype

# bring up the with-Spark stack
docker compose -f docker-compose.yml -f docker-compose.spark.prod.yml \
    up -d
sleep 60

# step-by-step demo (recommended for advisor walkthroughs — single terminal)
bash scripts/demo_sequential.sh
```

For a fresh install, see [`REPRODUCE.md`](./REPRODUCE.md).

### Two demo flavours

`prototype/scripts/` ships two demos — pick whichever matches your
audience:

| Script | Layout | When to use |
|--------|--------|-------------|
| `demo_sequential.sh` | **Single terminal**, 6 numbered steps with `press ENTER to continue` between each | Advisor walkthroughs, defense rehearsals — easy to narrate |
| `demo_for_advisor.sh` | tmux 4-pane dashboard (Spark / Detector / Webhook / Control) streaming in real time | Live system inspection, deep-dive sessions |

---

## The three experiments in the paper

| # | Name | Where to find code | Where to find figure |
|---|------|--------------------|----------------------|
| E1, E2 | Streaming speed + tenant isolation | `prototype/loadtest/run_experiment.py` (scenarios `e1`, `e2`) | `prototype/loadtest/figures/` |
| E6 | Cold-path retraining (sklearn vs Spark MLlib) | `prototype/spark_experiment/mllib/run_benchmark.sh` | `fig_sklearn_vs_spark_train.pdf`, `fig_sklearn_vs_spark_speedup.pdf` |
| E7 | Architecture throughput validation | `bash scripts/demo_sequential.sh` (single point) or manual rate sweep | `fig_throughput_sweep.pdf` |

---

## Hardware target

The numbers reported in the paper were collected on a single
SIIT-lab Linux server:

| Component | Spec |
|-----------|------|
| CPU | 12 logical cores |
| RAM | 16 GB physical + 4 GB swap |
| Disk | 548 GB SSD-backed |
| OS | Ubuntu 22.04 / kernel 5.15 |
| Docker | 28.2 + Compose v2 |

Lower-spec hardware can run the prototype but will hit the OOM
ceilings reported in the paper sooner (the cold-path benchmark
saturates a 16 GB box at 1 GB on-disk dataset size).

---

## License

MIT — see [`LICENSE`](./LICENSE).

---

## Contact

Earth (Nerdeye) — chaipatanantarasuchart@gmail.com
SIIT, Thammasat University
