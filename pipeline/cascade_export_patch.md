# cascade_export_patch

## What it does

Helper module (not a standalone script) that exports validation and test prediction tables from the hybrid cascade pipeline. Packs RF scores, Snort predictions, gate probabilities, escalation flags, and final cascade decisions into two CSV files for downstream validation-calibrated thresholding by `proposed_method_valcal.py`.

**This is a library, not a runnable script.** Its functions are copied into (or imported by) the main cascade script to export predictions after training.

## Prerequisites

- Part of the cascade pipeline; requires Python 3.10+ and numpy/pandas
- Intended for use within the cascade run (e.g., `hybrid_cascade_splitcal_fastsnort.py`)

## Function interface

```python
def export_cascade_split_predictions(
    out_dir,
    val_df: pd.DataFrame,
    test_df: pd.DataFrame,
    val_rf_score,
    val_rf_pvalue,
    val_snort_pred,
    val_snort_score,
    val_gate_prob,
    val_escalated,
    val_cascade_pred,
    val_cascade_score,
    test_rf_score,
    test_rf_pvalue,
    test_snort_pred,
    test_snort_score,
    test_gate_prob,
    test_escalated,
    test_cascade_pred,
    test_cascade_score,
)
```

## Inputs

Call with the following arrays/DataFrames from the cascade run:

| Parameter | Shape | Meaning |
|-----------|-------|---------|
| `val_df` | (n_val, *) | Validation DataFrame (retained as-is in output) |
| `test_df` | (n_test, *) | Test DataFrame (retained as-is in output) |
| `val_rf_score` | (n_val,) | RF anomaly scores on validation |
| `val_rf_pvalue` | (n_val,) | RF p-values on validation (for conformal) |
| `val_snort_pred` | (n_val,) | Snort binary predictions (1=hit, 0=miss) |
| `val_snort_score` | (n_val,) | Snort confidence scores |
| `val_gate_prob` | (n_val,) | Gate posterior P(attack \| x) in [0,1] |
| `val_escalated` | (n_val,) | Binary: 1 if RF escalated to gate, 0 if non-escalated |
| `val_cascade_pred` | (n_val,) | Final cascade prediction (1=attack, 0=benign) |
| `val_cascade_score` | (n_val,) | Final cascade score/confidence |
| `test_rf_score` | (n_test,) | RF anomaly scores on test |
| `test_rf_pvalue` | (n_test,) | RF p-values on test |
| `test_snort_pred` | (n_test,) | Snort predictions on test |
| `test_snort_score` | (n_test,) | Snort scores on test |
| `test_gate_prob` | (n_test,) | Gate probabilities on test |
| `test_escalated` | (n_test,) | Escalation flags on test |
| `test_cascade_pred` | (n_test,) | Cascade predictions on test |
| `test_cascade_score` | (n_test,) | Cascade scores on test |

## How to use

1. **Copy** the functions from `cascade_export_patch.py` into your cascade script (or import the module if vendored into the cascade).

2. **After all cascade components have been computed** (RF, Snort, gate, escalation, final predictions), call:

```python
from cascade_export_patch import export_cascade_split_predictions

export_cascade_split_predictions(
    out_dir=out_dir,
    val_df=splits.val_all,
    test_df=splits.test_all,
    val_rf_score=val_scores,
    val_rf_pvalue=val_pvals,
    val_snort_pred=val_snort_pred,
    val_snort_score=val_snort_score,
    val_gate_prob=val_gate_prob,
    val_escalated=val_escalated,
    val_cascade_pred=val_cascade_pred,
    val_cascade_score=val_cascade_score,
    test_rf_score=test_scores,
    test_rf_pvalue=test_pvals,
    test_snort_pred=test_snort_pred,
    test_snort_score=test_snort_score,
    test_gate_prob=test_gate_prob,
    test_escalated=test_escalated,
    test_cascade_pred=test_cascade_pred,
    test_cascade_score=test_cascade_score,
)
```

3. **Use the output CSVs** with `proposed_method_valcal.py`:

```bash
python3 proposed_method_valcal.py \
  --val-csv <out_dir>/val_cascade_predictions.csv \
  --test-csv <out_dir>/test_cascade_predictions.csv \
  --out-dir outputs_proposed_method_valcal
```

## Outputs

Writes two CSV files to `out_dir`:

### val_cascade_predictions.csv

```
[columns from val_df]  # All original columns preserved
rf_score, rf_pvalue, snort_pred, snort_score, gate_prob, escalated, cascade_pred, cascade_score, split
  # Cascade component outputs. split = "validation"
```

### test_cascade_predictions.csv

Same schema as val, with split = "test".

Example schema:

```
binary_label, row_id, source_file, ..., 
rf_score, rf_pvalue, snort_pred, snort_score, gate_prob, escalated, cascade_pred, cascade_score, split
```

## How to interpret the outputs

- Use `gate_prob` as the primary anomaly score for threshold selection
- `snort_pred` (binary 0/1) indicates fast-path signature hits
- `escalated` (binary) shows which rows were escalated from RF to gate
- `cascade_pred` and `cascade_score` are the cascade's final decision and confidence
- `binary_label` (if present) is the ground truth for evaluation

## Relationship to the cascade

This module is part of the cascade's internal machinery. It ensures that:
1. **Val/test alignment**: Predictions are row-aligned with the original DataFrames
2. **Completeness**: All intermediate scores (RF, Snort, gate) are exported for post-hoc analysis
3. **Auditability**: Downstream scripts can inspect escalation decisions and verify the cascade logic

## Common problems

1. **Shape mismatch**: If any input array has a different length than `val_df` or `test_df`, the export will fail with a ValueError listing the mismatch. Ensure all arrays are computed over the exact same rows.

2. **NaN or inf values**: Arrays with NaN or infinity are exported as-is. The cascade script should not produce these; if it does, investigate upstream computation.

3. **Missing columns in base DataFrames**: Ensure `val_df` and `test_df` have at least a row index or row_id column for joining. The export preserves all original columns.

4. **File already exists**: The function overwrites existing CSV files. Back up prior runs if needed.

## Reusing artifacts

The exported CSVs are the gateway to downstream analysis:
- `proposed_method_valcal.py` uses them to apply new operating points
- `rate_rules_baseline_valcal.py` and `rf_baseline_valcal.py` reuse the scores
- Any external audit or ablation study can read these CSVs directly
