# parse_fast_alerts.py

## What it does

Parses raw Snort `alert_fast.txt` files (output from snort_runner.py) into a single structured CSV with columns for timestamp, SID, message, protocol, IPs, and ports. This is the second step in the pipeline: snort_runner.py → **parse_fast_alerts.py** → snort_eval_fixed_v3_splitstrategy.py.

## Prerequisites

- **Python 3.10+**
- **pandas** (typically already installed, see requirements.txt)
- **alert_fast.txt files** produced by snort_runner.py in the structure:
  ```
  snort_outputs/
  ├── pcap_1_name/alert_fast.txt
  ├── pcap_2_name/alert_fast.txt
  └── ...
  ```

## Inputs

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--input-dir` | Path | Yes | Directory containing the raw snort outputs (recursively searches for `alert_fast.txt`) |
| `--output-csv` | Path | Yes | Path to save the parsed CSV (e.g. `/home/$USER/snort_alerts.csv`) |

## How to run (from scratch)

This script is step 2 in the chain:

```
snort_runner.py → parse_fast_alerts.py → snort_eval_fixed_v3_splitstrategy.py
```

After **snort_runner.py** completes, run:

```bash
python3 snort/parse_fast_alerts.py \
  --input-dir "/home/$USER/snort_outputs" \
  --output-csv "/home/$USER/snort_alerts.csv"
```

The script recursively finds all `alert_fast.txt` files under `--input-dir` and merges them into one CSV.

## Outputs

```
snort_alerts.csv
```

Columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | str | Alert timestamp in format `MM/DD-HH:MM:SS.microseconds` |
| `gid` | int | Generator ID (almost always 1 for rule-based alerts) |
| `sid` | int | Signature ID (unique per rule) |
| `rev` | int | Rule revision number |
| `message` | str | Alert message text |
| `priority` | int | Alert priority (0–3, lower = more critical) |
| `proto` | str | Protocol: `TCP`, `UDP`, `ICMP`, etc. |
| `src_ip` | str | Source IP address |
| `src_port` | int or NaN | Source port (NaN if not applicable) |
| `dst_ip` | str | Destination IP address |
| `dst_port` | int or NaN | Destination port (NaN if not applicable) |
| `source_file` | str | Full path to the `alert_fast.txt` file |
| `pcap_name` | str | Parent directory name (usually the PCAP filename without extension) |

Example rows (first 2):

```
timestamp,gid,sid,rev,message,priority,proto,src_ip,src_port,dst_ip,dst_port,source_file,pcap_name
07/03-18:55:58.598308,1,491,5,FTP Bad Command,3,TCP,10.0.0.5,60123,192.168.1.1,21,/home/user/snort_outputs/Monday-WorkingHours/alert_fast.txt,Monday-WorkingHours
07/03-18:56:12.124567,1,553,3,FTP Anonymous Login,2,TCP,172.16.0.10,50000,192.168.1.2,21,/home/user/snort_outputs/Monday-WorkingHours/alert_fast.txt,Monday-WorkingHours
```

## How to interpret the output

Check the parsed alert count and breakdown:

```bash
python3 -c "
import pandas as pd
df = pd.read_csv('/home/\$USER/snort_alerts.csv')
print(f'Total alerts: {len(df):,}')
print(f'\nTop 10 SIDs:')
print(df['sid'].value_counts().head(10))
print(f'\nProtocol distribution:')
print(df['proto'].value_counts())
"
```

Key metrics:
- **Total rows** = number of alerts triggered
- **Unique SIDs** = how many different rules fired
- **NULL counts** in `src_port` and `dst_port` = protocols without ports (e.g. ICMP)

If the CSV is empty or has < 100 rows, either:
- No rules matched the traffic (benign PCAP)
- The rules file didn't load correctly in snort_runner.py

## Reusing outputs

This CSV is consumed directly by **filter_policy_snort.py** (to filter by SID list) and **snort_eval_fixed_v3_splitstrategy.py** (to evaluate against test labels).

Typical workflow:
1. Generate `snort_alerts.csv` here
2. (Optional) Filter with `filter_policy_snort.py` → `snort_alerts_v4a.csv`
3. Evaluate with `snort_eval_fixed_v3_splitstrategy.py` → `snort_signature_metrics.csv`

## Common problems

1. **"No alert_fast files found under: /path/to/dir"**
   - Check that snort_runner.py completed successfully and the output directory contains subdirectories with `alert_fast.txt` files.
   - Verify: `find /home/$USER/snort_outputs -name "alert_fast.txt" | head -5`

2. **"FileNotFoundError: [Errno 2] No such file or directory"**
   - The input directory doesn't exist. Double-check the `--input-dir` path.

3. **CSV is empty or has very few rows**
   - This is usually OK if traffic was benign. Check raw `alert_fast.txt` file:
     ```bash
     wc -l /home/$USER/snort_outputs/*/alert_fast.txt
     head /home/$USER/snort_outputs/*/alert_fast.txt
     ```
   - If `alert_fast.txt` is empty, no rules triggered (check Snort config and rule file were loaded).

4. **"MemoryError" on very large PCAP sets**
   - parse_fast_alerts loads all alerts into memory before writing CSV. For massive datasets (>10M alerts), edit the script to use chunked pandas writing.

5. **Timestamps are incorrect or malformed**
   - This is normal if PCAP was captured on a different date. The inferred date is based on the PCAP filename (e.g., "Monday-WorkingHours" → 2017-07-03). Check README_SNORT_updated.md for the date mapping.
