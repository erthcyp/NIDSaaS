# snort — Signature engine

Snort 3 runner that processes the CIC-IDS2017 packet captures and
emits per-flow signature predictions. The output CSV is consumed by
the detection cascade in `pipeline/`.

## Files

| File | Purpose |
|---|---|
| `snort_runner.py` (+ `.md`) | Replay PCAPs through Snort 3 with the locked rule set |
| `snort_eval_fixed_v3_splitstrategy.py` (+ `.md`) | Per-day Snort evaluation under the locked temporal split |
| `parse_fast_alerts.py` (+ `.md`) | Convert Snort fast-alert output → per-row CSV |
| `filter_policy_snort.py` (+ `.md`) | Apply the configured rule-policy filter |
| `rules/community/` | Talos community ruleset |
| `rules/local/` | Project-local custom rules |
| `rules/policy/` | Active policy includes |
| `outputs_snort_eval_v4a/` | Final signature predictions consumed by the cascade |
| `outputs_snort_eval_v4a_temporal/` | Per-day temporal-split signature predictions |
| `README_SNORT_updated.md` | Detailed history of Snort version + rule changes |

## Key output

The file consumed by the detection cascade:

```
snort/outputs_snort_eval_v4a/snort_signature_predictions.csv
```

passed to `pipeline/hybrid_cascade_splitcal_fastsnort.py` via the
`--snort-predictions` flag.

## Re-running Snort

Snort is not part of the daily workflow — its output is frozen at the
above path. Re-running is only required if:

- the rule set changes (`rules/`),
- the pcap input changes (`pcap_CIC_IDS2017/`), or
- you need to validate a corrupted output CSV.

See `snort_runner.md` for the full command and Snort 3 installation
prerequisites.
