# compare_anomaly_baselines

## What it does

Legacy variant of `compare_anomaly_baselines_valcal.py`. Trains anomaly-detection baselines (RF, Isolation Forest, One-Class SVM, LSTM-Autoencoder) and selects operating points by F1 optimization directly on the test set. **Not recommended for publication** because test-optimal thresholding leaks test information; use `compare_anomaly_baselines_valcal.py` instead for proper validation-calibrated comparison.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` from repo root
- CIC-IDS2017 CSVs in the folder specified by `--data-dir`
- PyTorch (for LSTM)

## Inputs

| Flag | Default | Meaning |
|------|---------|---------|
| `--data-dir` | *required* | Path to CIC-IDS2017 CSV folder |
| `--out-dir` | *required* | Output directory for metrics and LaTeX |
| `--split-strategy` | `temporal_by_file` | How to split: `temporal_by_file` or `stratified` |
| `--seed` | `42` | Random seed for stratified splits |
| `--test-size` | `0.20` | Fraction for test set |
| `--val-size-from-train` | `0.20` | Fraction of training data for validation |
| `--iforest-n-estimators` | `200` | Isolation Forest tree count |
| `--ocsvm-train-size` | `20000` | Max rows for One-Class SVM training |
| `--ocsvm-nu` | `0.01` | One-Class SVM nu parameter |
| `--ocsvm-gamma` | `scale` | One-Class SVM kernel gamma |
| `--skip-ocsvm` | *flag* | Skip One-Class SVM |
| `--skip-lstm` | *flag* | Skip LSTM-Autoencoder |
| `--lstm-seq-len` | `10` | Sequence length |
| `--lstm-hidden` | `64` | Hidden size |
| `--lstm-latent` | `32` | Latent dimension |
| `--lstm-epochs` | `8` | Training epochs |
| `--lstm-batch` | `256` | Batch size |
| `--lstm-lr` | `1e-3` | Learning rate |
| `--lstm-train-size` | `200000` | Max benign training rows |
| `--lstm-device` | `cpu` | PyTorch device |

## How to run (from scratch)

```bash
# Complete run (all baselines)
python3 compare_anomaly_baselines.py \
  --data-dir ../csv_CIC_IDS2017 \
  --out-dir outputs_baselines_temporal

# Quick test (skip slow methods)
python3 compare_anomaly_baselines.py \
  --data-dir ../csv_CIC_IDS2017 \
  --out-dir outputs_baselines_temporal_quick \
  --skip-ocsvm --skip-lstm
```

## Outputs

`outputs_baselines_temporal/`

```
overall_metrics_baselines.csv
  # Metrics for each baseline at test-F1-optimal and test-FAR-matched thresholds

baselines_table_fragment.tex
  # LaTeX rows (benchmarks only, should not appear in final paper)

run_config.json
  # Metadata
```

## How to interpret the output

This script uses **test-optimal** thresholding, which means thresholds are selected on the test set itself. This inflates performance estimates and should not be used in the paper. Refer to the reported metrics as **upper bounds only** for internal development.

## Relationship to valcal variant

| Feature | `compare_anomaly_baselines.py` | `compare_anomaly_baselines_valcal.py` |
|---------|--------------------------------|---------------------------------------|
| Threshold selection | On test set (test-optimal) | On validation set (frozen for test) |
| Suitable for publication | No (leaks test info) | Yes |
| Performance | Inflated | Conservative, generalizable |
| Use case | Development / debugging | Final paper comparison |

## Common problems

Same as `compare_anomaly_baselines_valcal.py` (FileNotFoundError on data-dir, PyTorch issues, memory, etc.).

## Recommendation

Do not use this script for paper results. Always use `compare_anomaly_baselines_valcal.py` instead.
