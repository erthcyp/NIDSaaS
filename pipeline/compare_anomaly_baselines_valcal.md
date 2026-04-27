# compare_anomaly_baselines_valcal

## What it does

Trains Random Forest, Isolation Forest, One-Class SVM, and LSTM-Autoencoder anomaly detectors on CIC-IDS2017 using a fixed temporal split, then applies validation-calibrated thresholds (accuracy and FAR optimized on validation, frozen for test). Outputs metrics and LaTeX fragments suitable for comparison-table inclusion.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` from repo root
- CIC-IDS2017 CSVs in the folder specified by `--data-dir`
- PyTorch (for LSTM) installed via requirements

## Inputs

| Flag | Default | Meaning |
|------|---------|---------|
| `--data-dir` | *required* | Path to CIC-IDS2017 CSV folder |
| `--out-dir` | *required* | Output directory for metrics and LaTeX |
| `--split-strategy` | `temporal_by_file` | How to split: `temporal_by_file` or `stratified` |
| `--seed` | `42` | Random seed for stratified splits (ignored for temporal) |
| `--test-size` | `0.20` | Fraction of data for test (temporal: last 20% by date) |
| `--val-size-from-train` | `0.20` | Fraction of training data for validation |
| `--iforest-n-estimators` | `200` | Number of trees in Isolation Forest |
| `--ocsvm-train-size` | `20000` | Max training rows for One-Class SVM (for speed) |
| `--ocsvm-nu` | `0.01` | Nu parameter for One-Class SVM |
| `--ocsvm-gamma` | `scale` | Gamma parameter for One-Class SVM kernel |
| `--skip-ocsvm` | *flag* | Skip One-Class SVM (slow) |
| `--skip-lstm` | *flag* | Skip LSTM-Autoencoder (requires PyTorch) |
| `--lstm-seq-len` | `10` | Sequence length for LSTM sliding windows |
| `--lstm-hidden` | `64` | Hidden size of LSTM encoder/decoder |
| `--lstm-latent` | `32` | Latent dimension |
| `--lstm-epochs` | `8` | Number of epochs for LSTM training |
| `--lstm-batch` | `256` | Batch size |
| `--lstm-lr` | `1e-3` | Learning rate |
| `--lstm-train-size` | `200000` | Max rows for LSTM training (from benign only) |
| `--lstm-device` | `cpu` | PyTorch device (`cpu` or `cuda`) |
| `--target-far` | `8.1e-4` | Target false-alarm rate for secondary row |
| `--include-val-f1` | *flag* | Emit F1-optimal row (not included by default) |
| `--include-test-optimistic` | *flag* | Emit test-optimal rows (benchmark only, not for paper) |

## How to run (from scratch)

From `pipeline/` directory:

```bash
# Complete run (all baselines, temporal split, defaults)
python3 compare_anomaly_baselines_valcal.py \
  --data-dir ../csv_CIC_IDS2017 \
  --out-dir outputs_baselines_valcal
```

Skip slow methods (for quick test):

```bash
python3 compare_anomaly_baselines_valcal.py \
  --data-dir ../csv_CIC_IDS2017 \
  --out-dir outputs_baselines_valcal \
  --skip-ocsvm \
  --skip-lstm
```

With LSTM on GPU:

```bash
python3 compare_anomaly_baselines_valcal.py \
  --data-dir ../csv_CIC_IDS2017 \
  --out-dir outputs_baselines_valcal \
  --lstm-device cuda
```

## Outputs

`outputs_baselines_valcal/`

```
overall_metrics_baselines.csv
  # One row per (method, operating_point) pair. Columns:
  #   method, operating_point, threshold_source, threshold, accuracy, precision,
  #   recall, f1, far, roc_auc, pr_auc, tp, fp, tn, fn, fit_seconds, score_seconds

baselines_table_fragment.tex
  # LaTeX rows ready for the main comparison table (one per method at headline operating_point)

run_config.json
  # Metadata: split strategy, feature dimensions, training hyperparameters, sample counts
```

## How to interpret the output

Open `overall_metrics_baselines.csv`. Each method appears twice:
- One row at `operating_point = val_f1_calibrated` (F1 optimized on validation)
- One row at `operating_point = val_far_calibrated_<TARGET_FAR>` (FAR-constrained on validation)

For the main paper, use the row matching the selected operating point (typically `val_f1_calibrated` or the FAR-matching row). Compare `accuracy`, `recall`, and `far` columns against the Hybrid-Cascade rows.

The `fit_seconds` and `score_seconds` columns show wall-clock training and scoring time.

## Reusing artifacts

The output CSVs contain row indices and can be merged with external audit trails. The metrics rows can be directly concatenated with rows from other baseline scripts to create a unified comparison table.

## Common problems

1. **FileNotFoundError on data-dir**: Ensure the path contains the CIC-IDS2017 CSV files (e.g., `Monday.csv`, `Tuesday.csv`, etc.). The script reads all CSVs in that folder.

2. **LSTM requires PyTorch**: If you skip `--skip-lstm` but PyTorch is not installed, the script will fail. Either install PyTorch (`pip install torch`) or add `--skip-lstm`.

3. **LSTM out-of-memory**: The LSTM fits only on the benign training fold (sliced to `--lstm-train-size` rows). If you hit memory limits, reduce `--lstm-train-size` or `--lstm-hidden`.

4. **One-Class SVM is slow**: Training and scoring times grow quadratically with training set size. The script caps it at `--ocsvm-train-size=20000` by default. To skip: add `--skip-ocsvm`.

5. **Split strategy mismatch**: Use `--split-strategy temporal_by_file` to match the cascade's temporal split. Stratified splits will not align with other scripts' results.
