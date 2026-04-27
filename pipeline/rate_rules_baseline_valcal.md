# rate_rules_baseline_valcal

## What it does

Rate-rule-only ablation for the comparison table. Evaluates hand-coded rate-based signatures (volumetric, SYN-flood, port-scan, brute-force) with and without Snort, using validation-calibrated thresholds. Emits four rows showing the ceiling of what signature-based detection alone can achieve, directly comparable to the Hybrid-Cascade at the same operating point.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` from repo root
- Cascade val/test CSVs from a prior run (e.g., `hybrid_cascade_splitcal_fastsnort.py`)
- Rate-rule predictions from `signature_merged_predictions.csv` (or equivalent)

## Inputs

| Flag | Default | Meaning |
|------|---------|---------|
| `--val-csv` | `outputs_hybrid_cascade_splitcal_fastsnort_temporal/val_cascade_predictions.csv` | Validation predictions from cascade (must have `binary_label`, `snort_pred`) |
| `--test-csv` | `outputs_hybrid_cascade_splitcal_fastsnort_temporal/test_cascade_predictions.csv` | Test predictions from cascade (same schema as val) |
| `--rate-csv` | `signature_merged_predictions.csv` | Rate-rule fires CSV (must have `rate_V`, `rate_S`, `rate_P`, `rate_B`, `rate_L`, `rate_R`, and optionally `signature_pred`) |
| `--out-dir` | `outputs_rate_rules_baseline_valcal` | Output directory for metrics and LaTeX |

## How to run (from scratch)

From `pipeline/` directory:

```bash
# Simplest: uses defaults matching the headline cascade run
python3 rate_rules_baseline_valcal.py
```

Explicit paths:

```bash
python3 rate_rules_baseline_valcal.py \
  --val-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/val_cascade_predictions.csv \
  --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal/test_cascade_predictions.csv \
  --rate-csv signature_merged_predictions.csv \
  --out-dir outputs_rate_rules_baseline_valcal
```

## Outputs

`outputs_rate_rules_baseline_valcal/`

```
overall_metrics_rate_rules_valcal.csv   # one row per (method, operating_point)
rate_rules_table_fragment.tex           # LaTeX rows ready for the master table
per_class_rate_VSP.csv                  # which attack classes V|S|P catches/misses
per_class_rate_OR.csv                   # same, for the full OR
per_class_rate_count.csv                # same, for the count score
per_class_snort_plus_rates.csv          # signature stack vs each attack class
run_config.json                         # provenance + protocol blurb
```

## What to do with the results

1. **Open `overall_metrics_rate_rules_valcal.csv`.** The four `method`
   rows at `operating_point = val_accuracy_calibrated` go into the master
   comparison table directly under the existing baselines.

2. **Open the per-class CSVs.** This is where the ablation story lives:
   you should see *high* detection rate on DoS/DDoS/PortScan/Patator and
   *near-zero* detection on Web Attack / Infiltration / Bot / Heartbleed.
   That contrast is the headline figure for "why the gate is necessary."

3. **Add the LaTeX fragment** at `rate_rules_table_fragment.tex` to the
   ablation block of the paper. Same column schema as the existing
   baseline fragments, so it lines up with no edits.

## How to interpret the output

Open `overall_metrics_rate_rules_valcal.csv`. Four methods will be present:

| Method | Score | Interpretation |
|--------|-------|-----------------|
| Rate Rules (V\|S\|P) | rate_V \| rate_S \| rate_P | Tier-1 promotion set used in cascade |
| Rate Rules (all 6, OR) | rate_V \| ... \| rate_R | Full union of all six rules |
| Rate Rules (count) | sum of rate_* fires | Treated as anomaly score, threshold selected on val |
| Snort + Rate Rules | snort_pred \| any rate rule | Full signature stack |

Each row shows `val_accuracy_calibrated` metrics. Compare `accuracy`, `recall`, and `far` columns with the Hybrid-Cascade row to understand the coverage and false-alarm tradeoff of signature-based detection alone.

## Relationship to the cascade

These rows show the ceiling of what hand-coded signatures can achieve on CIC-IDS2017:
- High recall on volumetric attacks (DoS, DDoS) but near-zero on application-layer exploits
- The Hybrid-Cascade's gate is trained to catch what signatures miss

## Why the protocol matches the headline run

- Same split strategy (`temporal_by_file`, seed 42, 64/16/20)
- Same labels (binary_label: 1=attack, 0=benign)
- Same threshold-selection rule (val-accuracy-calibrated on validation, frozen for test)
- Test set used exactly once per method, never for tuning

So the rate-rule-only rows are directly comparable to the RF, RF+Conformal, HistGB, and full Hybrid-Cascade rows in the master table.

## Common problems

1. **FileNotFoundError on CSVs**: Ensure the cascade output directory exists and the rate-rule CSV is in the current directory or specified with an absolute path. Default path is `signature_merged_predictions.csv`.

2. **Missing rate_* columns in rate-csv**: The rate-rule CSV must have columns `rate_V`, `rate_S`, `rate_P`, `rate_B`, `rate_L`, `rate_R` (one for each rule). Generate it via `signature_rate_rules.py` if missing.

3. **Row count mismatch between CSVs**: If val/test and rate-rule CSVs have different row counts, the merge will fail. Ensure all three CSVs are generated from the same underlying flow dataset and temporal split.

4. **Missing snort_pred column**: The cascade val/test CSVs must have a `snort_pred` column for the "Snort + Rate Rules" row. If absent, edit the script to skip that row.

5. **All zeros or all ones**: If all test rows are predicted as benign (or all as attack), metrics like recall or precision will be degenerate (NaN or 0). This indicates the rate rules are not firing or are firing on everything. Verify the rate-rule thresholds are appropriate for your dataset.
