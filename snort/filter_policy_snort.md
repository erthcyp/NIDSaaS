# filter_policy_snort.py

## What it does

Filters a parsed Snort alerts CSV to keep only alerts matching a policy SID (Signature ID) list. This optional step allows testing different rule subsets without re-running Snort. Can be inserted between parse_fast_alerts.py and snort_eval_fixed_v3_splitstrategy.py in the pipeline:

```
snort_runner.py → parse_fast_alerts.py → filter_policy_snort.py → snort_eval_fixed_v3_splitstrategy.py
```

## Prerequisites

- **Python 3.10+**
- **pandas** (see requirements.txt)
- **Parsed Snort alerts CSV** from parse_fast_alerts.py (e.g. `/home/$USER/snort_alerts.csv`)
- **SID policy file** — a text file with one SID per line (see example below)

## Inputs

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--input-csv` | Path | Yes | Parsed alerts CSV from parse_fast_alerts.py |
| `--policy-file` | Path | Yes | Text file with one SID per line (lines starting with `#` are comments) |
| `--output-csv` | Path | Yes | Path to save filtered alerts CSV |

## How to run (from scratch)

### Step 1: Create a policy file (optional if one exists)

Policy files are simple text files listing SIDs to keep:

```bash
cat > snort/rules/policy/my_policy.txt <<'EOF'
# My custom policy: keep only FTP-related rules
491   # FTP Bad Command
553   # FTP Anonymous Login
1234  # Custom FTP rule (if applicable)
EOF
```

Lines starting with `#` are ignored. Empty lines are OK.

### Step 2: Run the filter

```bash
python3 snort/filter_policy_snort.py \
  --input-csv "/home/$USER/snort_alerts.csv" \
  --policy-file "snort/rules/policy/v4a_keep_sids.txt" \
  --output-csv "/home/$USER/snort_alerts_v4a.csv"
```

This keeps only alerts with SID 491 or 553 (the v4a policy).

### Step 3: Feed filtered alerts to evaluation

```bash
python3 snort/snort_eval_fixed_v3_splitstrategy.py \
  --data-dir "csv_CIC_IDS2017" \
  --snort-alerts "/home/$USER/snort_alerts_v4a.csv" \
  --output-dir "outputs_snort_eval_v4a" \
  --ignore-time
```

## Outputs

```
snort_alerts_v4a.csv
```

Same structure as the input CSV, but with only rows where `sid` is in the policy file:

| Column | Type | Example |
|--------|------|---------|
| `timestamp` | str | `07/03-18:55:58.598308` |
| `gid` | int | `1` |
| `sid` | int | `491` |
| `rev` | int | `5` |
| `message` | str | `FTP Bad Command` |
| `priority` | int | `3` |
| `proto` | str | `TCP` |
| `src_ip` | str | `10.0.0.5` |
| `src_port` | int | `60123` |
| `dst_ip` | str | `192.168.1.1` |
| `dst_port` | int | `21` |
| `source_file` | str | `/home/user/snort_outputs/Monday/alert_fast.txt` |
| `pcap_name` | str | `Monday-WorkingHours` |

### Console output

The script also prints a summary and the top SIDs in the filtered set:

```
[filter_policy_snort] loading alerts: /home/user/snort_alerts.csv
[filter_policy_snort] loaded 18 SIDs from policy: snort/rules/policy/v4a_keep_sids.txt
[filter_policy_snort] input rows   = 12345
[filter_policy_snort] output rows  = 2418
[filter_policy_snort] saved to     = /home/user/snort_alerts_v4a.csv

Top SID/message counts:
491 FTP Bad Command           1823
553 FTP Anonymous Login        595
```

## How to interpret the output

Key number: **Output rows / Input rows = Selectivity**

- If output rows = 0.1 × input rows → policy keeps 10% of alerts (aggressive filtering)
- If output rows = 0.5 × input rows → policy keeps 50% (moderate filtering)
- If output rows = 0.9 × input rows → policy is too loose (keeps almost everything)

Check the "Top SID/message counts" to ensure the right rules are kept:

```
491 FTP Bad Command           1823
553 FTP Anonymous Login        595
```

If unexpected SIDs appear, check the policy file format.

## Reusing outputs

The filtered CSV is fed directly to **snort_eval_fixed_v3_splitstrategy.py**:

```bash
python3 snort/snort_eval_fixed_v3_splitstrategy.py \
  --snort-alerts "/home/$USER/snort_alerts_v4a.csv" \
  ...
```

Example workflow for testing multiple policies:

```bash
# Policy v4a: FTP only
python3 snort/filter_policy_snort.py \
  --input-csv snort_alerts.csv \
  --policy-file snort/rules/policy/v4a_keep_sids.txt \
  --output-csv snort_alerts_v4a.csv

# Policy v4b: FTP + other
python3 snort/filter_policy_snort.py \
  --input-csv snort_alerts.csv \
  --policy-file snort/rules/policy/v4b_keep_sids.txt \
  --output-csv snort_alerts_v4b.csv

# Evaluate both
python3 snort/snort_eval_fixed_v3_splitstrategy.py \
  --snort-alerts snort_alerts_v4a.csv \
  --output-dir outputs_v4a \
  --ignore-time

python3 snort/snort_eval_fixed_v3_splitstrategy.py \
  --snort-alerts snort_alerts_v4b.csv \
  --output-dir outputs_v4b \
  --ignore-time

# Compare metrics
echo "v4a metrics:"; cat outputs_v4a/snort_signature_metrics.csv
echo "v4b metrics:"; cat outputs_v4b/snort_signature_metrics.csv
```

## Common problems

1. **"Policy file not found"**
   - Check the policy file path:
     ```bash
     ls -l snort/rules/policy/v4a_keep_sids.txt
     ```

2. **"No valid SIDs found in policy file"**
   - The policy file is empty or all lines are comments. Add at least one SID:
     ```bash
     echo "491" >> snort/rules/policy/my_policy.txt
     ```

3. **"Invalid SID in policy file: abc"**
   - A line contains non-integer text. SIDs must be numbers. Fix:
     ```bash
     # Before (wrong):
     cat snort/rules/policy/bad.txt
     491 FTP Bad Command
     553
     # After (correct):
     cat snort/rules/policy/fixed.txt
     491
     553
     ```

4. **"Input CSV must contain a 'sid' column"**
   - The input CSV is malformed or from a different source. Verify:
     ```bash
     head -1 /home/$USER/snort_alerts.csv | tr ',' '\n' | grep -i sid
     ```
   - If missing, regenerate with parse_fast_alerts.py.

5. **Output rows = 0 (empty filtered CSV)**
   - No alerts in the input matched any SID in the policy. Either:
     - The input CSV is empty (check: `wc -l /home/$USER/snort_alerts.csv`)
     - The policy SIDs don't exist in the input (check: `cut -d, -f3 /home/$USER/snort_alerts.csv | sort -u`)
     - Examples:
       ```bash
       # Show all unique SIDs in input
       tail -n +2 /home/$USER/snort_alerts.csv | cut -d, -f3 | sort -u
       # Compare with policy
       cat snort/rules/policy/v4a_keep_sids.txt | grep -v "^#"
       ```
