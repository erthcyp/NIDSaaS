# signature_rate_rules

## What it does

Flow-level rate-based signature prefilter. Detects volumetric attacks, SYN floods, port scans, brute-force attempts, and slow-HTTP exploits using hand-coded rules on CIC-IDS2017 canonical flow features. Outputs a CSV with per-flow rule fires and anomaly indicators, optionally merged with existing Snort predictions. Acts as a drop-in replacement or complement to signature-based detection.

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt` from repo root
- CIC-IDS2017 CSVs in the folder specified by `--data-dir`

## Inputs

| Flag | Default | Meaning |
|------|---------|---------|
| `--data-dir` | *required* | Path to CIC-IDS2017 CSV folder (e.g., `../csv_CIC_IDS2017`) |
| `--output-csv` | *required* | Output CSV path (will contain row_id, signature_pred, signature_score, rule_fired, rate_* columns) |
| `--merge-snort-csv` | `None` | Optional Snort predictions CSV to OR-merge into the output |
| `--max-missing-fraction` | `0.30` | Tolerate up to this fraction of missing values per feature before dropping a row |
| `--vol-pkt-s` | `None` | Override volumetric packets/s threshold (default from config) |
| `--vol-byte-s` | `None` | Override volumetric bytes/s threshold |
| `--portscan-unique-ports` | `None` | Override port-scan unique-port threshold |
| `--bruteforce-attempts` | `None` | Override brute-force attempt threshold |

## How to run (from scratch)

From `pipeline/` directory:

```bash
# Generate rate-rule predictions only
python3 signature_rate_rules.py \
  --data-dir ../csv_CIC_IDS2017 \
  --output-csv signature_rate_predictions.csv
```

Merge with existing Snort predictions (OR the two sets together):

```bash
python3 signature_rate_rules.py \
  --data-dir ../csv_CIC_IDS2017 \
  --merge-snort-csv ../snort/outputs_snort_eval_v4a/snort_signature_predictions.csv \
  --output-csv signature_merged_predictions.csv
```

Override a threshold (e.g., for ablation):

```bash
python3 signature_rate_rules.py \
  --data-dir ../csv_CIC_IDS2017 \
  --vol-pkt-s 50000 \
  --output-csv signature_rate_predictions_ablated.csv
```

## Outputs

`signature_rate_predictions.csv` (or user-specified path)

```
row_id
  # Unique identifier for each flow (0-indexed across the entire CIC-IDS2017 dataset)

signature_pred
  # Binary prediction: 1 if ANY rule fires, 0 otherwise
  # (or 1 if the rule OR'd with Snort when --merge-snort-csv is set)

signature_score
  # Number of rules that fired (0–6); treated as an anomaly score

rule_fired
  # String indicating which rules fired (e.g., "VPS", "V", "P", "")
  # Letters: V=Volumetric, L=Slow-HTTP, S=SYN-flood, R=RST-anomaly, P=PortScan, B=Brute-force

rate_V, rate_S, rate_P, rate_B, rate_L, rate_R
  # Binary indicators (0 or 1) for each individual rule
  # Can be used to selectively promote rules into the cascade's Tier-1 fast-path
  # via proposed_method_valcal.py --rate-rules-include
```

## Rules and thresholds

All thresholds are calibrated on the benign training fold (CIC-IDS2017, ~99–99.9th percentile) to achieve ~1% false-alarm rate per rule.

| Rule | Acronym | Condition | Why it matters |
|------|---------|-----------|----------------|
| Volumetric | V | ≥20 packets AND (packets/s > 40k OR bytes/s > 15M) | Detects large-volume DoS/DDoS |
| Slow-HTTP | L | Port in {80,443,8080}, duration > 60s, <20 packets, <100 bytes/s | Slow-HTTP POST exploits |
| SYN-flood | S | ≥3 SYN flags, ≤10 total packets | Classic SYN floods |
| RST-anomaly | R | ≥5 RST flags, ≤20 total packets | Abnormal RST behavior |
| PortScan | P | (src_ip, 2s window): ≥200 unique dst_ports using only 'scan-like' flows (≤10 pkt, <10ms) | Horizontal port sweeps |
| Brute-force | B | (src_ip, dst_port, 2s window): ≥10 attempts on ports {21,22,23} | SSH/FTP credential attacks |

## How to interpret the output

Use `signature_pred` (binary) and `signature_score` (count) directly for anomaly detection. Or extract individual `rate_*` columns and promote selected rules into the cascade via `proposed_method_valcal.py --rate-rules-csv ... --rate-rules-include rate_V,rate_S,rate_P` to widen the Tier-1 fast-path.

## Relationship to the cascade

The output CSV is consumed by:
1. **`proposed_method_valcal.py`**: Via `--rate-rules-csv` flag, to optionally widen the Tier-1 fast-path
2. **`rate_rules_baseline_valcal.py`**: As the `--rate-csv` input to evaluate rate-rules-only performance

## Common problems

1. **FileNotFoundError on data-dir**: Ensure the path points to the CIC-IDS2017 CSV folder. The script expects files like `Monday.csv`, `Tuesday.csv`, etc.

2. **All rows filtered out**: If `--max-missing-fraction` is too small, rows with any missing features may be dropped. Increase to 0.5 or higher to be more permissive.

3. **Output CSV is empty or has very few fires**: Rate-rule thresholds are conservatively set for ~1% false-alarm rate on benign data. If no attacks are being caught, verify the dataset contains actual attacks (e.g., open the CSV and check the ground-truth labels).

4. **Merge with Snort fails (row count mismatch)**: If `--merge-snort-csv` is used, the Snort CSV must have the same row count and row-id alignment as the rate-rule CSV. Check that both were generated from the same underlying flow dataset.

5. **Performance on unlabeled data**: This script only checks rules; it does not read ground-truth labels. It can be applied to any flow dataset with the expected feature columns. Evaluation against ground truth is done downstream.

## Reusing artifacts

The output `signature_rate_predictions.csv` (or merged version) can be:
- Fed into `proposed_method_valcal.py` to promote selected rate rules into Tier-1
- Fed into `rate_rules_baseline_valcal.py` to evaluate rate-only performance
- Used independently as a fast prefilter upstream of a ML detector
