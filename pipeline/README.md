# pipeline — Detection scripts

The detection logic of NIDSaaS, organised as a chain of scripts that
read each other's output. Each entry-point script has a matching `.md`
documenting flags and outputs in detail.

## Stage order

Run in this order when reproducing the paper from scratch. Stages 4-7
are independent baselines and may run in any order (or be skipped) once
Stage 2 has produced the cascade prediction CSVs.

| Stage | Script | Output dir | Purpose |
|---|---|---|---|
| 1 | `signature_rate_rules.py` | writes back into `pipeline/` | Run rate-rule engine + merge with Snort predictions to produce `signature_merged_predictions.csv` |
| 2 | `hybrid_cascade_splitcal_fastsnort.py` | `outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/` | Main cascade: train RF + conformal + gate; emit val_/test_cascade_predictions.csv |
| 3 | `proposed_method_valcal.py` | `outputs_proposed_locked_rate_promoted/` | Apply val-accuracy-calibrated τ* to cascade scores → headline metrics |
| 4 | `rf_baseline_valcal.py` | `outputs_rf_baseline_valcal/` | RF and RF+Conformal baseline rows (reuses cascade RF scores, no retraining) |
| 5 | `rate_rules_baseline_valcal.py` | `outputs_rate_rules_baseline_valcal/` | Rate-rule-only ablation |
| 6 | `compare_anomaly_baselines.py` | (per-run) | RF / Conformal / LSTM AE / IF / OCSVM at default thresholds |
| 7 | `compare_anomaly_baselines_valcal.py` | `outputs_baselines_temporal_by_file_valcal_iso/` | Same baselines under val-accuracy-calibrated protocol |
| 8 | `cascade_export_patch.py` | writes `.joblib` bundle | Package the cascade joblibs for the streaming prototype |
| 9 | `closr_baseline_valcal.py` | `outputs_closr_baseline_temporal/` → `outputs_closr_baseline_temporal_valcal/` | Train + score CLAD (Wilkie et al., IEEE TNSM 2026) on the locked split |

## Library modules (imported by the entry scripts above; do not rename)

| Module | Role |
|---|---|
| `config.py` | `RFConfig` and other dataclasses shared across detectors |
| `utils.py` | Seeding, JSON IO, canonical column / label normalisation |
| `features.py` | Numeric/categorical preprocessing for sklearn detectors |
| `load_data.py` | CIC-IDS2017 loader + locked split (`temporal_by_file`) |
| `metrics.py` | Binary classification metrics including FAR |
| `rf_anomaly.py` | `SelfSupervisedRFAnomaly` model class |
| `conformal_wrapper.py` | Split-conformal calibration |
| `escalation_gate_fastsnort.py` | HistGB gate + preprocessor |
| `lstm_autoencoder_baseline.py` | LSTM AE baseline (also used as a library by `compare_anomaly_baselines.py`) |

These modules are imported via `from X import …` and their filenames
must not change.

## Utility scripts

- `fix_closr_score_range.py` — one-shot rescale of an older CLAD
  prediction CSV from [-1,1] to [0,1]. Only needed for CSVs produced
  before 2026-04-28; the current `closr_baseline_valcal.py` writes the
  [0,1] range directly.

## Locked run commands

```bash
cd /mnt/c/Users/user/Downloads/NIDSaaS_Experiment/pipeline

# Stage 2 — train the cascade (longest step; ~30-45 min)
python3 hybrid_cascade_splitcal_fastsnort.py \
  --data-dir ../csv_CIC_IDS2017 \
  --snort-predictions ../snort/outputs_snort_eval_v4a/snort_signature_predictions.csv \
  --output-dir outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50 \
  --alpha-conformal 0.05 --alpha-escalate 0.20 --gate-threshold 0.50 \
  --calibration-fraction 0.50 --split-strategy temporal_by_file

# Stage 3 — headline metrics
python3 proposed_method_valcal.py \
  --val-csv  outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/val_cascade_predictions.csv \
  --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/test_cascade_predictions.csv \
  --out-dir outputs_proposed_locked_rate_promoted \
  --rate-rules-csv signature_merged_predictions.csv \
  --calibrate-isotonic

# Stage 4 — RF + RF-Conformal baseline rows (sub-minute; reuses scores from Stage 2)
python3 rf_baseline_valcal.py \
  --val-csv  outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/val_cascade_predictions.csv \
  --test-csv outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/test_cascade_predictions.csv \
  --out-dir outputs_rf_baseline_valcal

# Stage 7 — anomaly baselines (~30-50 min on CPU)
python3 compare_anomaly_baselines_valcal.py \
  --data-dir ../csv_CIC_IDS2017 \
  --out-dir outputs_baselines_temporal_by_file_valcal_iso \
  --split-strategy temporal_by_file --seed 42 \
  --headline-operating-point val_accuracy_calibrated --lstm-device cpu

# Stage 9 — CLAD baseline (~90 min on GPU)
python3 closr_baseline_valcal.py \
  --data-dir ../csv_CIC_IDS2017 --closr-repo ../CLOSR \
  --out-dir outputs_closr_baseline_temporal \
  --split-strategy temporal_by_file --seed 42

python3 proposed_method_valcal.py \
  --val-csv  outputs_closr_baseline_temporal/val_closr_predictions.csv \
  --test-csv outputs_closr_baseline_temporal/test_closr_predictions.csv \
  --out-dir  outputs_closr_baseline_temporal_valcal \
  --calibrate-isotonic --method-name "CLAD"
```

See each script's `.md` companion for full flag documentation.

## Output index

See `OUTPUTS_INDEX.md` in this same directory for a per-folder
description of every `outputs_*/` produced by the scripts above.
