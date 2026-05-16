#!/usr/bin/env bash
# precheck_detection.sh
# ----------------------------------------------------------------------
# Sanity-check before running the detection experiment from
# DETECTION_QUICKSTART.md.
#
# Verifies:
#   - CSV dataset on Windows fs (or wherever the repo lives)
#   - PCAP dataset placed inside WSL Linux fs (~/pcap_CIC_IDS2017/)
#   - Python deps importable
#   - Snort 3 binary installed
#   - Snort rules + policy files present
#   - Pre-generated escape-hatch artifacts present (for §10 fast path)
#
# Soft warnings (yellow) do not block — they tell you which optional
# steps you can or cannot run yet. Hard failures (red) exit non-zero.
#
# Run from the repo root:
#   bash scripts/precheck_detection.sh
# ----------------------------------------------------------------------
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

green()  { printf "  \033[32m%s\033[0m\n" "$*"; }
red()    { printf "  \033[31m%s\033[0m\n" "$*"; }
yellow() { printf "  \033[33m%s\033[0m\n" "$*"; }

EXIT=0
SNORT_PATH_OK=1
ML_PATH_OK=1
FAST_PATH_OK=1   # §10 — skip Snort, use committed CSV

echo "== precheck_detection =="
echo "repo: $REPO_ROOT"

# ----------------------------------------------------------------------
# 1. CSV dataset (required)
# ----------------------------------------------------------------------
echo
echo "[1/6] CSV dataset (csv_CIC_IDS2017/)"
EXPECTED_CSVS=(
  "Monday-WorkingHours.pcap_ISCX.csv"
  "Tuesday-WorkingHours.pcap_ISCX.csv"
  "Wednesday-workingHours.pcap_ISCX.csv"
  "Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv"
  "Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv"
  "Friday-WorkingHours-Morning.pcap_ISCX.csv"
  "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"
  "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv"
)
if [[ ! -d csv_CIC_IDS2017 ]]; then
  red "MISSING dir: csv_CIC_IDS2017/"
  red "  see DETECTION_QUICKSTART.md §2.1"
  EXIT=1
  ML_PATH_OK=0
  SNORT_PATH_OK=0
  FAST_PATH_OK=0
else
  MISSING=0
  for f in "${EXPECTED_CSVS[@]}"; do
    if [[ ! -f "csv_CIC_IDS2017/$f" ]]; then
      red "MISSING file: csv_CIC_IDS2017/$f"
      MISSING=$((MISSING+1))
    fi
  done
  if [[ $MISSING -eq 0 ]]; then
    green "OK — 8 expected CSVs present"
  else
    red "$MISSING CSV(s) missing — see DETECTION_QUICKSTART.md §2.1"
    EXIT=1
    ML_PATH_OK=0
    SNORT_PATH_OK=0
    FAST_PATH_OK=0
  fi
fi

# ----------------------------------------------------------------------
# 2. PCAP dataset — required for Snort, must be on Linux fs (~/pcap_CIC_IDS2017)
# ----------------------------------------------------------------------
echo
echo "[2/6] PCAP dataset (~/pcap_CIC_IDS2017/)"
PCAP_DIR="${HOME}/pcap_CIC_IDS2017"
EXPECTED_PCAPS=(
  "Monday-WorkingHours.pcap"
  "Tuesday-WorkingHours.pcap"
  "Wednesday-workingHours.pcap"
  "Thursday-WorkingHours.pcap"
  "Friday-WorkingHours.pcap"
)
if [[ ! -d "$PCAP_DIR" ]]; then
  yellow "no $PCAP_DIR/ — Snort path NOT runnable yet"
  yellow "  copy pcaps in: cp /mnt/c/.../PCAPs/*.pcap $PCAP_DIR/"
  yellow "  see DETECTION_QUICKSTART.md §2.2"
  SNORT_PATH_OK=0
else
  # Check we're not on /mnt/c/ — that would defeat the purpose
  REAL_PCAP_DIR="$(readlink -f "$PCAP_DIR" 2>/dev/null || echo "$PCAP_DIR")"
  case "$REAL_PCAP_DIR" in
    /mnt/c/*|/mnt/d/*|/mnt/e/*)
      yellow "$PCAP_DIR points at $REAL_PCAP_DIR (Windows fs)"
      yellow "  Snort will run 5-10x slower. Copy to native Linux fs:"
      yellow "  rm $PCAP_DIR && mkdir -p $PCAP_DIR"
      yellow "  cp $REAL_PCAP_DIR/*.pcap $PCAP_DIR/"
      ;;
  esac
  MISSING=0
  for f in "${EXPECTED_PCAPS[@]}"; do
    if [[ ! -f "$PCAP_DIR/$f" ]]; then
      yellow "MISSING pcap: $PCAP_DIR/$f"
      MISSING=$((MISSING+1))
    fi
  done
  if [[ $MISSING -eq 0 ]]; then
    green "OK — 5 expected PCAPs present in Linux fs"
  else
    yellow "$MISSING PCAP(s) missing — Snort path NOT runnable yet"
    SNORT_PATH_OK=0
  fi
fi

# ----------------------------------------------------------------------
# 3. Python interpreter + deps
# ----------------------------------------------------------------------
echo
echo "[3/6] Python deps"
if ! command -v python3 >/dev/null 2>&1; then
  red "python3 not found — install Python 3.10 or 3.11"
  EXIT=1
  ML_PATH_OK=0
  FAST_PATH_OK=0
  SNORT_PATH_OK=0
else
  PYV="$(python3 -c 'import sys; print(".".join(map(str,sys.version_info[:2])))')"
  green "python3 = $PYV  ($(which python3))"
  if ! python3 -c "import numpy, pandas, sklearn, torch, joblib" 2>/dev/null; then
    red "missing python deps — run: pip install -r requirements.txt"
    red "  (any env works: venv / system / conda — see DETECTION_QUICKSTART.md §3)"
    EXIT=1
    ML_PATH_OK=0
    FAST_PATH_OK=0
  else
    green "OK — numpy / pandas / sklearn / torch / joblib import cleanly"
  fi
fi

# ----------------------------------------------------------------------
# 4. Snort 3 binary
# ----------------------------------------------------------------------
echo
echo "[4/6] Snort 3 binary"
SNORT_BIN=""
for cand in /opt/snort3/bin/snort "$(command -v snort 2>/dev/null || true)"; do
  if [[ -n "$cand" && -x "$cand" ]]; then
    SNORT_BIN="$cand"
    break
  fi
done
if [[ -z "$SNORT_BIN" ]]; then
  yellow "snort not installed — Snort path NOT runnable yet"
  yellow "  install per DETECTION_QUICKSTART.md §4 (~30 min one-time)"
  SNORT_PATH_OK=0
else
  SNORT_VER="$($SNORT_BIN -V 2>&1 | grep -Eo 'Version [0-9.]+' | head -1)"
  green "OK — $SNORT_BIN ($SNORT_VER)"
fi

# ----------------------------------------------------------------------
# 5. Snort rules + policy
# ----------------------------------------------------------------------
echo
echo "[5/6] Snort rules + policy"
RULES_OK=1
for f in \
  snort/rules/community/snort3-community.rules \
  snort/rules/community/sid-msg.map \
  snort/rules/policy/v4a_keep_sids.txt; do
  if [[ ! -f "$f" ]]; then
    red "MISSING $f"
    RULES_OK=0
  fi
done
if [[ $RULES_OK -eq 1 ]]; then
  green "OK — community rules + v4a policy committed"
else
  red "rules incomplete — git pull or re-clone"
  SNORT_PATH_OK=0
fi

# ----------------------------------------------------------------------
# 6. Pre-generated Snort artifacts (§10 escape hatch)
# ----------------------------------------------------------------------
echo
echo "[6/6] Pre-generated Snort artifacts (§10 fast path)"
ART_OK=1
for f in \
  snort/outputs_snort_eval_v4a_temporal/snort_signature_predictions.csv \
  snort/outputs_snort_eval_v4a_temporal/snort_signature_metrics.csv; do
  if [[ ! -f "$f" ]]; then
    yellow "missing $f"
    ART_OK=0
  fi
done
if [[ $ART_OK -eq 1 ]]; then
  green "OK — committed snort_signature_predictions.csv ready for §10 fast path"
else
  yellow "no committed predictions — fast path (§10) unavailable until Snort runs"
  FAST_PATH_OK=0
fi

# ----------------------------------------------------------------------
# Disk + RAM heads-up
# ----------------------------------------------------------------------
echo
echo "[+] disk + RAM"
FREE_GB="$(df -BG --output=avail . 2>/dev/null | tail -1 | tr -dc '0-9')"
if [[ -n "${FREE_GB:-}" && $FREE_GB -lt 30 ]]; then
  yellow "only ${FREE_GB} GB free here — Snort outputs + ML artifacts need ~30 GB"
else
  green "disk: ${FREE_GB:-?} GB free here"
fi
MEM_MB="$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo 2>/dev/null || echo 0)"
if [[ $MEM_MB -lt 11000 ]]; then
  yellow "RAM: ${MEM_MB} MiB — anomaly baselines may OOM"
  yellow "  raise WSL2 mem cap to >= 12 GB (DETECTION_QUICKSTART.md §1)"
else
  green "RAM: ${MEM_MB} MiB"
fi

# ----------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------
echo
echo "== summary =="
[[ $FAST_PATH_OK  -eq 1 ]] && green "FAST PATH (§10, skip Snort, ~1.5 h):           READY" \
                          ||  yellow "FAST PATH (§10, skip Snort, ~1.5 h):           blocked"
[[ $SNORT_PATH_OK -eq 1 ]] && green "FULL PATH (§4–§6, fresh Snort run, ~4 h):      READY" \
                          ||  yellow "FULL PATH (§4–§6, fresh Snort run, ~4 h):      blocked"

if [[ $EXIT -ne 0 ]]; then
  echo
  red "precheck FAILED — fix the items in red above and re-run."
  exit $EXIT
fi
echo
green "precheck OK"
