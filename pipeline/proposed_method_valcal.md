# proposed_method_valcal

## What it does

Applies a validation-calibrated low-FAR operating point to the Hybrid-Cascade detector. Takes pre-computed validation and test predictions (including Snort hits, gate probabilities, and optional rate-rule indicators), builds a composite final-score function, selects a decision threshold on validation that maximizes accuracy, and evaluates on the frozen test set. Outputs metrics and prediction tables suitable for inclusion in the main comparison table.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` from repo root
- Validation and test CSVs from a prior cascade run (e.g., from `hybrid_cascade_splitcal_fastsnort.py`)

## Inputs

| Flag | Default | Meaning |
|------|---------|---------|
| `--val-csv` | *required* | Path to validation predictions CSV (must have `binary_label`, `snort_pred`, `gate_prob`) |
| `--test-csv` | *required* | Path to test predictions CSV (same schema as val) |
| `--out-dir` | *required* | Output directory for metrics and predictions |
| `--target-far` | `8.1e-4` | Target false-alarm rate for the secondary FAR-calibrated row |
| `--label-col` | `binary_label` | Name of ground-truth label column (0=benign, 1=attack) |
| `--snort-col` | `snort_pred` | Name of Snort binary prediction column |
| `--gate-prob-col` | `gate_prob` | Name of gate posterior probability column in [0,1] |
| `--row-id-col` | `row_id` | Name of row identifier for merging rate-rule hits |
| `--rate-rules-csv` | `None` | Optional CSV with `rate_*` indicator columns (e.g., `signature_merged_predictions.csv`) |
| `--rate-rules-include` | `rate_V,rate_S,rate_P` | Comma-separated rate-rule column names to promote to Tier-1 fast-path |
| `--include-val-f1` | *flag* | Emit an additional F1-optimal row (not emitted by default) |
| `--include-test-optimistic` | *flag* | Emit test-optimal rows for benchmarking only (not for paper) |
| `--calibrate-isotonic` | *flag* | Fit isotonic regression on val scores and emit `*_isotonic` rows |
| `--skip-val-balanced-accuracy` | *flag* | Skip the balanced-accuracy row (emitted by default) |

## How to run (from scratch)

From `pipeline/` directory:

```bash
# Simplest: assumes val/test CSVs from the headline cascade run
python3 proposed_method_valcal.py \
  --val-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/val_cascade_predictions.csv \
  --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/test_cascade_predictions.csv \
  --out-dir outputs_proposed_method_valcal
```

With rate-rule promotion (widening the Tier-1 fast-path):

```bash
python3 proposed_method_valcal.py \
  --val-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/val_cascade_predictions.csv \
  --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/test_cascade_predictions.csv \
  --rate-rules-csv signature_merged_predictions.csv \
  --rate-rules-include "rate_V,rate_S,rate_P" \
  --out-dir outputs_proposed_method_valcal
```

With isotonic calibration (for ablation):

```bash
python3 proposed_method_valcal.py \
  --val-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/val_cascade_predictions.csv \
  --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/test_cascade_predictions.csv \
  --calibrate-isotonic \
  --out-dir outputs_proposed_method_valcal
```

## Outputs

`outputs_proposed_method_valcal/`

```
val_scores_with_predictions.csv
  # Validation set with columns: binary_label, snort_pred, gate_prob, 
  #   final_score, y_pred_val_accuracy_calibrated, 
  #   y_pred_val_balanced_accuracy_calibrated (and isotonic variants if --calibrate-isotonic)

test_scores_with_predictions.csv
  # Test set with same schema as val

overall_metrics_proposed_valcal.csv
  # One row per (method, operating_point) pair. Columns:
  #   method, operating_point, threshold_source, threshold, accuracy, precision,
  #   recall, f1, far, roc_auc, pr_auc, tp, fp, tn, fn

run_config.json
  # Metadata: thresholds, sample counts, rate-rule promotions, protocol description
```

## How to interpret the output

Open `overall_metrics_proposed_valcal.csv`. The headline row is `operating_point = val_accuracy_calibrated` (or the choice set by `--headline-operating-point`). Read:
- `accuracy`: main metric (threshold maximizes this on validation)
- `recall`: catch rate on test attacks
- `far`: false-alarm rate on test benigns
- `threshold`: the tau* selected on validation and frozen for test

The `val_balanced_accuracy_calibrated` row (prior-invariant alternative) and `*_isotonic` rows (if enabled) are ablations for the paper. Use the `val_accuracy_calibrated` row for the main table unless specified otherwise.

## Reusing artifacts

The output CSVs can be fed back into other analysis scripts:
- `val_scores_with_predictions.csv` + `test_scores_with_predictions.csv` can be used by any threshold-study or per-class breakdown script
- The final scores and predictions can be exported to create confusion matrices or attack-class breakdowns

## Common problems

1. **FileNotFoundError on CSVs**: Verify paths are absolute or relative to `pipeline/`. The cascade outputs are usually under `outputs_hybrid_cascade_splitcal_fastsnort_temporal/`.

2. **Missing columns in input CSV**: The validation and test CSVs must have `binary_label`, `snort_pred`, and `gate_prob` columns. If any are missing, the script will fail with a clear error message listing required columns.

3. **out-of-range gate_prob values**: If `gate_prob` contains values outside [0,1], the script raises a ValueError. Check the source cascade run for bugs in gate probability output.

4. **Rate-rule CSV mismatch**: If using `--rate-rules-csv`, ensure the CSV has a `row_id` column and the requested rate-rule columns (e.g., `rate_V`, `rate_S`, `rate_P`). Rows must align by `row_id` with the validation/test CSVs.

5. **Memory on large CSVs**: If validation or test sets are >1M rows, ensure sufficient RAM. Isotonic calibration fits a regressor to val scores, which is fast; the bottleneck is CSV I/O and merges.
