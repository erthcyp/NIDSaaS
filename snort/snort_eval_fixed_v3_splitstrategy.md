# snort_eval_fixed_v3_splitstrategy.py

## What it does

Matches Snort alerts to flows in the CIC-IDS2017 test split, computes binary classification metrics (TP, FP, FN, TN, accuracy, precision, recall, F1, FAR), and exports prediction CSVs. This is the final step in the Snort evaluation pipeline: snort_runner.py → parse_fast_alerts.py → **snort_eval_fixed_v3_splitstrategy.py**.

## Prerequisites

- **Python 3.10+**
- **pandas, numpy, scikit-learn** (see requirements.txt)
- **CIC-IDS2017 CSV data** with labeled flows at `--data-dir` (typically `../csv_CIC_IDS2017/`)
- **Parsed Snort alerts CSV** from parse_fast_alerts.py (e.g. `/home/$USER/snort_alerts.csv` or filtered version)
- **load_data.py** in the pipeline directory (auto-imported, defines `load_and_prepare_detection_data`)

## Inputs

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--data-dir` | Path | Yes | Path to CIC-IDS2017 CSV folder with labeled flows |
| `--snort-alerts` | Path | Yes | Path to parsed Snort alerts CSV (output from parse_fast_alerts.py or filter_policy_snort.py) |
| `--output-dir` | Path | Yes | Directory to save metrics and prediction CSVs |
| `--split-row-ids` | Path | No | JSON file with `test_row_ids` list to override default test split (e.g. `split_row_ids.json`) |
| `--split-strategy` | str | No | How to split data: `"random"` (default), `"temporal"`, or `"temporal_by_file"` |
| `--time-window-seconds` | float | No | Time window for matching alerts to flows (default: `2.0` seconds) |
| `--ignore-time` | flag | No | Match alerts to flows by (pcap, proto, IPs, ports) only, ignoring timestamps |
| `--max-missing-fraction` | float | No | Drop flows with >this fraction missing values (default: `0.20`) |
| `--test-size` | float | No | Fraction of data reserved for test (default: `0.20`) |
| `--val-size-from-train` | float | No | Fraction of training data reserved for validation (default: `0.20`) |
| `--random-state` | int | No | Random seed for reproducibility (default: `42`) |
| `--drop-unknown-labels` | flag | No | Drop flows with unknown/BENIGN labels before evaluation |

## How to run (from scratch)

This script is step 3 in the chain:

```
snort_runner.py → parse_fast_alerts.py → snort_eval_fixed_v3_splitstrategy.py
```

After **parse_fast_alerts.py** completes, run:

### Option A: Simple evaluation (standard test split, ignore timestamps)

```bash
python3 snort/snort_eval_fixed_v3_splitstrategy.py \
  --data-dir "csv_CIC_IDS2017" \
  --snort-alerts "/home/$USER/snort_alerts.csv" \
  --output-dir "outputs_snort_eval" \
  --ignore-time
```

### Option B: With filtered alerts (e.g., v4a policy)

```bash
python3 snort/snort_eval_fixed_v3_splitstrategy.py \
  --data-dir "csv_CIC_IDS2017" \
  --snort-alerts "/home/$USER/snort_alerts_v4a.csv" \
  --output-dir "outputs_snort_eval_v4a" \
  --ignore-time \
  --time-window-seconds 2.0
```

### Option C: With custom test split (e.g., from ML experiment)

```bash
python3 snort/snort_eval_fixed_v3_splitstrategy.py \
  --data-dir "csv_CIC_IDS2017" \
  --snort-alerts "/home/$USER/snort_alerts_v4a.csv" \
  --output-dir "outputs_snort_eval_v4a" \
  --split-row-ids "path/to/split_row_ids.json" \
  --ignore-time
```

## Outputs

```
outputs_snort_eval/
├── snort_signature_metrics.csv          # Single-row table with overall metrics
└── snort_signature_predictions.csv      # Full test set with predictions and scores
```

### snort_signature_metrics.csv

Single row with these columns:

| Column | Description |
|--------|-------------|
| `paper_model` | Always `"Signature-Snort"` |
| `model` | Always `"snort_signature"` |
| `accuracy` | (TP + TN) / (TP + TN + FP + FN) |
| `precision` | TP / (TP + FP) — of detected flows, how many were truly malicious |
| `recall` | TP / (TP + FN) — of malicious flows, how many were detected |
| `f1` | 2 × (precision × recall) / (precision + recall) |
| `far` | FP / (FP + TN) — False Alarm Rate, proportion of benign flows wrongly flagged |
| `roc_auc` | Area under ROC curve (NaN if not computed) |
| `pr_auc` | Area under Precision-Recall curve (NaN if not computed) |
| `tp` | True Positives (malicious flows correctly detected) |
| `tn` | True Negatives (benign flows correctly not detected) |
| `fp` | False Positives (benign flows wrongly flagged) |
| `fn` | False Negatives (malicious flows missed) |
| `alerts_input_rows` | Total rows in input Snort alerts CSV |
| `alerts_deduped_rows` | Alerts after deduplication |
| `matched_test_rows` | Number of test flows matched to at least one alert |
| `time_window_seconds` | Time window used for matching |
| `ignore_time` | Whether timestamps were ignored |

Example:

```csv
paper_model,model,accuracy,precision,recall,f1,far,tp,tn,fp,fn,roc_auc,pr_auc,...
Signature-Snort,snort_signature,0.987,0.65,0.72,0.68,0.005,456,8234,45,176,NaN,NaN,...
```

### snort_signature_predictions.csv

Full test set with columns from CIC-IDS2017 plus prediction columns:

| Column | Description |
|--------|-------------|
| (all original test flow columns) | `timestamp`, `protocol`, `source_ip`, `source_port`, `destination_ip`, `destination_port`, `label`, etc. |
| `signature_pred` | Binary prediction: `1` = malicious (alert matched), `0` = benign (no alert) |
| `signature_score` | Confidence score: `1.0` if matched, `0.0` if not |

Use this for downstream fusion (e.g., combining with RF predictions in a hybrid model).

## How to interpret the output

Key number: **Recall** (column `recall` in metrics.csv)

Recall = TP / (TP + FN) = proportion of attacks actually caught by Snort

- Recall = 0.75 → Snort detected 75% of attack flows
- Recall < 0.5 → Signature rules are not suitable for this dataset
- Recall > 0.8 → Signatures are good coverage

Secondary metric: **False Alarm Rate (FAR)**

FAR = FP / (FP + TN) = proportion of benign flows wrongly flagged

- FAR = 0.01 → 1% of benign traffic is wrongly flagged
- FAR > 0.05 → Too many false alarms, tighten filter policy

F1 score balances both: F1 = 0.68 in example above is moderate.

### Interpreting matched_test_rows

If `matched_test_rows` is very low (e.g., <10):
- Either very few Snort alerts were generated
- Or timestamps don't align well with test flows
- Try `--ignore-time` to match on (pcap, proto, IPs, ports) only

## Reusing outputs

**snort_signature_predictions.csv** is the input for downstream hybrid fusion:

```bash
# Example: combine Snort predictions with RF model predictions
python3 pipeline/combine_predictions.py \
  --snort-preds "outputs_snort_eval_v4a/snort_signature_predictions.csv" \
  --rf-preds "rf_predictions.csv" \
  --output "hybrid_predictions.csv"
```

Both `metrics.csv` and `predictions.csv` are also archived for the paper results.

## Common problems

1. **"Could not resolve required columns in test dataframe"**
   - CIC-IDS2017 CSV columns don't match expected names. Script looks for:
     - `timestamp` or `Timestamp`
     - `protocol` or `Protocol`
     - `source_ip`, `Source IP`, or `src_ip`
     - etc.
   - Print available columns:
     ```bash
     python3 -c "import pandas as pd; df = pd.read_csv('csv_CIC_IDS2017/Monday-WorkingHours.csv'); print(df.columns.tolist())"
     ```

2. **"The supplied Snort alerts CSV is empty"**
   - No alerts in the parsed CSV. Either:
     - PCAP traffic was benign (expected)
     - Rule file didn't load in snort_runner.py
   - Check: `wc -l /home/$USER/snort_alerts.csv`

3. **"matched_test_rows" is very low (0–10)**
   - Time window too tight. Try `--ignore-time` or increase `--time-window-seconds` (e.g., `5.0`).
   - Or PCAP names in alerts don't match CSV `source_file` column. Inspect both:
     ```bash
     cut -d, -f13 /home/$USER/snort_alerts.csv | head -5
     cut -d, -f13 csv_CIC_IDS2017/Monday-WorkingHours.csv | head -5
     ```

4. **"Could not find binary_label or label column"**
   - CIC-IDS2017 CSV is missing a label column. Add one:
     ```bash
     python3 -c "
     import pandas as pd
     df = pd.read_csv('csv_CIC_IDS2017/Monday-WorkingHours.csv')
     df['binary_label'] = (df['Label'].str.upper() != 'BENIGN').astype(int)
     df.to_csv('csv_CIC_IDS2017/Monday-WorkingHours.csv', index=False)
     "
     ```

5. **Recall is 0, FAR is NaN**
   - No alerts matched test rows. Ensure:
     - `--snort-alerts` file is not empty
     - PCAP names align (Monday-WorkingHours.pcap → "Monday-WorkingHours" in CSV)
     - Try `--ignore-time` to relax matching constraints
