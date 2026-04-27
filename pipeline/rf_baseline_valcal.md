# rf_baseline_valcal

## What it does

Emits the "Random Forest" and "RF + Conformal" rows of the main comparison table without retraining. Reuses RF scores already saved by the cascade run, applies validation-calibrated threshold selection (accuracy and balanced-accuracy optimized on validation, frozen for test), and optionally applies Mondrian conformal prediction for uncertainty quantification.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` from repo root
- CSVs from a prior cascade run (typically `hybrid_cascade_splitcal_fastsnort.py`)

## Inputs

| Flag | Default | Meaning |
|------|---------|---------|
| `--val-csv` | `outputs_hybrid_cascade_splitcal_fastsnort_temporal/val_cascade_predictions.csv` | Validation predictions CSV (must have `rf_score`, `rf_pvalue`, `binary_label`) |
| `--test-csv` | `outputs_hybrid_cascade_splitcal_fastsnort_temporal/test_cascade_predictions.csv` | Test predictions CSV (same schema as val) |
| `--out-dir` | `outputs_rf_baseline_valcal` | Output directory for metrics and LaTeX |
| `--label-col` | `binary_label` | Name of ground-truth label column (0=benign, 1=attack) |
| `--rf-score-col` | `rf_score` | Name of RF score column (typically anomaly score in [0,1]) |
| `--rf-pvalue-col` | `rf_pvalue` | Name of RF p-value column (for conformal prediction) |
| `--skip-conformal` | *flag* | Skip the RF + Conformal row (useful if p-value column is absent) |
| `--calibrate-isotonic` | *flag* | Fit isotonic regression on val scores; emit `*_isotonic` rows for ablation |
| `--skip-val-balanced-accuracy` | *flag* | Skip the balanced-accuracy row (emitted by default) |
| `--headline-operating-point` | `val_accuracy_calibrated` | Which operating point row goes in the LaTeX headline (choices: `val_accuracy_calibrated`, `val_balanced_accuracy_calibrated`, or `*_isotonic` variants) |

## How to run (from scratch)

From `pipeline/` directory:

```bash
# Simplest: uses defaults matching the headline cascade run
python3 rf_baseline_valcal.py
```

Explicit paths:

```bash
python3 rf_baseline_valcal.py \
  --val-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/val_cascade_predictions.csv \
  --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/test_cascade_predictions.csv \
  --out-dir outputs_rf_baseline_valcal
```

With isotonic calibration (ablation):

```bash
python3 rf_baseline_valcal.py \
  --calibrate-isotonic \
  --out-dir outputs_rf_baseline_valcal_isotonic
```

Skip conformal (if p-value column is missing):

```bash
python3 rf_baseline_valcal.py \
  --skip-conformal \
  --out-dir outputs_rf_baseline_valcal_no_conformal
```

## Outputs

`outputs_rf_baseline_valcal/`

```
overall_metrics_rf_valcal.csv
  # One row per (method, operating_point) pair. Columns:
  #   method, operating_point, threshold_source, threshold, accuracy, precision,
  #   recall, f1, far, roc_auc, pr_auc, tp, fp, tn, fn

rf_table_fragment.tex
  # LaTeX rows for the main comparison table (headlines the selected operating point)

run_config.json
  # Metadata: thresholds, row counts, protocol description, conformal enable/disable
```

## How to interpret the output

Open `overall_metrics_rf_valcal.csv`. Look for two methods:
- **"Random Forest"** at `operating_point = val_accuracy_calibrated`: the RF baseline at the headline accuracy-optimized threshold
- **"RF + Conformal"** at the same operating point (if not `--skip-conformal`): same threshold, but with conformal prediction uncertainty quantification applied

The `val_balanced_accuracy_calibrated` row (prior-invariant alternative) is emitted but typically not used in the paper unless specifically requested.

The `threshold` column shows tau* (selected on validation, applied to test). Compare the `accuracy`, `recall`, and `far` columns with the Hybrid-Cascade rows in the main table.

## Reusing artifacts

The output metrics can be directly concatenated with outputs from other baseline scripts (e.g., `compare_anomaly_baselines_valcal.py`) to build a unified comparison table. The LaTeX fragment is ready to paste into the paper without modification.

## Common problems

1. **FileNotFoundError on CSVs**: Verify the cascade output directory exists. Default paths assume a temporal cascade run in `outputs_hybrid_cascade_splitcal_fastsnort_temporal/`.

2. **Missing columns in input CSV**: The CSVs must have `binary_label`, `rf_score`, and (unless `--skip-conformal`) `rf_pvalue` columns. The error message will list what is missing.

3. **Skip conformal if p-value unavailable**: If the cascade run did not output p-values, add `--skip-conformal` to avoid a ValueError. This emits only the "Random Forest" row.

4. **Isotonic calibration requires sufficient validation data**: If the validation set is very small (<1000 rows), isotonic regression may be poorly estimated. The script still runs but may not improve generalization.

5. **Threshold selection on all-benign or all-attack validation set**: If the validation fold contains only one class, the accuracy-optimal threshold will degenerate. The script logs a warning if this is detected.
