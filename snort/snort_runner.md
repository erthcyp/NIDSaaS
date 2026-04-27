# snort_runner.py

## What it does

Replays PCAP files through Snort 3 in offline mode, generating `alert_fast.txt` output files for each PCAP. This is the first step in the Snort evaluation pipeline: **snort_runner.py** → parse_fast_alerts.py → snort_eval_fixed_v3_splitstrategy.py.

## Prerequisites

- **Python 3.10+**
- **Snort 3** installed and working (verify with `/opt/snort3/bin/snort -V`)
- **LibDAQ** compiled and library path registered (see README_SNORT_updated.md for full install)
- **PCAP files** (`.pcap`, `.pcapng`, or `.cap`) in a single directory
- **Snort config** (`snort.lua`, typically at `/opt/snort3/etc/snort/snort.lua`)
- **Community rules** (optional but recommended, typically at `snort/rules/community/snort3-community.rules`)

## Inputs

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--snort-exe` | Path | Yes | Path to Snort binary, e.g. `/opt/snort3/bin/snort` |
| `--pcap-dir` | Path | Yes | Directory containing `.pcap` / `.pcapng` files to replay |
| `--rules` | Path | Yes | Path to `snort.lua` config, e.g. `/opt/snort3/etc/snort/snort.lua` |
| `--out-dir` | Path | Yes | Output directory where `alert_fast.txt` files will be written (one per PCAP) |
| `--extra-rules` | Path | No | Optional extra rules file (e.g. `rules/community/snort3-community.rules`) |
| `--packet-limit` | Int | No | Optional packet limit per PCAP for faster testing (e.g. `10000`) |

## How to run (from scratch)

### Step 1: Verify Snort installation

```bash
/opt/snort3/bin/snort -V
```

You should see output like: `Snort 3.x.x.x`.

### Step 2: Prepare PCAP files

Copy or mount PCAP files to a working directory:

```bash
mkdir -p ~/pcap_CIC_IDS2017
cp /path/to/pcaps/*.pcap ~/pcap_CIC_IDS2017/
```

For WSL users: copy from Windows into Linux filesystem for better performance.

### Step 3: Run Snort over all PCAPs

From the repo root (or the `snort/` directory):

```bash
python3 snort/snort_runner.py \
  --snort-exe "/opt/snort3/bin/snort" \
  --pcap-dir "/home/$USER/pcap_CIC_IDS2017" \
  --rules "/opt/snort3/etc/snort/snort.lua" \
  --extra-rules "snort/rules/community/snort3-community.rules" \
  --out-dir "/home/$USER/snort_outputs"
```

With an optional packet limit (for testing):

```bash
python3 snort/snort_runner.py \
  --snort-exe "/opt/snort3/bin/snort" \
  --pcap-dir "/home/$USER/pcap_CIC_IDS2017" \
  --rules "/opt/snort3/etc/snort/snort.lua" \
  --extra-rules "snort/rules/community/snort3-community.rules" \
  --out-dir "/home/$USER/snort_outputs" \
  --packet-limit 50000
```

The script will discover all PCAPs automatically and run Snort on each one.

## Outputs

```
snort_outputs/
├── pcap_1_name/
│   ├── alert_fast.txt          # Snort raw alerts in fast format
│   └── pcap_1_name_snort_stdout.txt  # Snort console log
├── pcap_2_name/
│   ├── alert_fast.txt
│   └── pcap_2_name_snort_stdout.txt
└── ...
```

**alert_fast.txt format (one alert per line):**

```
07/03-18:55:58.598308 [**] [1:1000009:1] "TEST ANY IP" [**] [Priority: 0] {TCP} 8.8.8.8:80 -> 192.168.1.2:50000
```

Fields (in order):
- Timestamp: `MM/DD-HH:MM:SS.microseconds`
- GID:SID:REV: `[gid:sid:rev]` (Generator ID : Signature ID : Revision)
- Message: quoted text in `[**] "..." [**]`
- Priority: `[Priority: 0-3]`
- Protocol: `{TCP|UDP|ICMP}`
- Endpoints: `SRC_IP[:SRC_PORT] -> DST_IP[:DST_PORT]`

## How to interpret the output

Check the console logs to ensure all PCAPs processed successfully:

```bash
grep "return_code=0" /home/$USER/snort_outputs/*/pcap*_snort_stdout.txt
```

Return code `0` = success. Anything else indicates an error in that PCAP.

For each PCAP, check the alert file size to ensure it's not empty:

```bash
du -h /home/$USER/snort_outputs/*/alert_fast.txt
```

If any PCAP produced 0 bytes, no alerts were triggered (benign traffic or no matching rules).

## Reusing outputs

The output `alert_fast.txt` files are consumed by **parse_fast_alerts.py**, which aggregates them into a single CSV. Do not edit them manually.

## Common problems

1. **"snort: command not found"**
   - Snort is not installed or the binary path is wrong. Verify with `ls -l /opt/snort3/bin/snort`.

2. **"error opening DAQ library"**
   - LibDAQ is not built or the library path is not registered. Run:
     ```bash
     echo '/usr/local/lib/daq_s3/lib/' | sudo tee /etc/ld.so.conf.d/libdaq3.conf
     sudo ldconfig
     ```

3. **"No PCAP files found under: /path/to/pcaps"**
   - The directory is empty or contains files with wrong extension. Check extensions are `.pcap`, `.pcapng`, or `.cap` (case-sensitive).

4. **"rule file not found"**
   - The path to `snort.lua` or `extra-rules` is wrong. Verify with `ls -l /opt/snort3/etc/snort/snort.lua`.

5. **Slow replay in WSL**
   - PCAP files are on the Windows filesystem. Copy them to the Linux side first (`/home/$USER/pcap_...`) for 10x+ speedup.
