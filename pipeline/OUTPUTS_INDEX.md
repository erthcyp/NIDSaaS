# pipeline output directory index

Each `outputs_*/` directory under `pipeline/` was produced by exactly
one stage script in `README.md`. This index maps directory → producing
script → contents → downstream consumers.

| Directory | Producing stage | Approx. size | Key contents | Downstream consumers |
|---|---|---|---|---|
| `outputs_hybrid_cascade_splitcal_fastsnort_temporal_a20_g50/` | Stage 2 | ~1.8 GB | `rf_anomaly.joblib`, `conformal_wrapper.joblib`, `escalation_gate_fastsnort.joblib`, `val_cascade_predictions.csv`, `test_cascade_predictions.csv`, `cascade_predictions.csv`, `overall_metrics.csv`, `cascade_summary.json` | Stage 3, Stage 4, Stage 8 |
| `outputs_proposed_locked_rate_promoted/` | Stage 3 | ~600 MB | `overall_metrics_proposed_valcal.csv` (headline), `val_scores_with_predictions.csv`, `test_scores_with_predictions.csv`, `run_config.json` | Paper Table I (Hybrid-Cascade row) |
| `outputs_rf_baseline_valcal/` | Stage 4 | < 10 KB | `overall_metrics_rf_valcal.csv`, `rf_table_fragment.tex`, `run_config.json` | Paper Table I (RF + RF-Conformal rows) |
| `outputs_closr_baseline_temporal/` | Stage 9a | ~600 MB | `clad_baseline.pt`, `val_closr_predictions.csv`, `test_closr_predictions.csv`, `closr_baseline_summary.json` | Stage 9b |
| `outputs_closr_baseline_temporal_valcal/` | Stage 9b | ~640 MB | `overall_metrics_proposed_valcal.csv` (method label = "CLAD"), `val_scores_with_predictions.csv`, `test_scores_with_predictions.csv`, `run_config.json` | Paper Table I (CLAD row) |

## Optional outputs

These directories are produced only when the corresponding stage is
explicitly run. They are not part of the headline reproduction:

- `outputs_rate_rules_baseline_valcal/` — Stage 5 (rate-rule ablation)
- `outputs_baselines_temporal_by_file_valcal_iso/` — Stage 7 (OCSVM, IF,
  LSTM AE, RF, RF+Conformal under val-cal)

## Re-creating from scratch

Deleting any of these directories is safe — the producing script will
recreate them on the next run. Wall-clock cost:

- Stage 2 cascade: ~30-45 min
- Stage 7 baselines: ~30-50 min
- Stage 9 CLAD: ~90 min on a single laptop GPU

Prefer reusing existing artefacts when hyperparameters are unchanged.

## Headline metrics location

The Hybrid-Cascade and CLAD headline rows both live in files named
`overall_metrics_proposed_valcal.csv` — one under
`outputs_proposed_locked_rate_promoted/` (Hybrid-Cascade) and one under
`outputs_closr_baseline_temporal_valcal/` (CLAD). They share the same
schema because both are emitted by `proposed_method_valcal.py`; they
differ only in the `method` column (`Hybrid-Cascade (ours)` vs `CLAD`).

The RF and RF-Conformal rows live in
`outputs_rf_baseline_valcal/overall_metrics_rf_valcal.csv`.
