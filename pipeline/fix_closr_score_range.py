"""One-shot fix-up: rescale CLAD scores from [-1, +1] to [0, 1].

The first version of closr_baseline_valcal.py wrote gate_prob = -cosine_sim,
which is in [-1, +1]. proposed_method_valcal.py validates that gate_prob is
in [0, 1] and aborts otherwise.

This script reads the existing val_/test_closr_predictions.csv, applies the
monotone transform gate_prob' = (gate_prob + 1) / 2 (and the same to
cascade_score), and overwrites the CSV in place. No retraining required.

The transform is monotone, so the val_accuracy_calibrated tau* found by
proposed_method_valcal.py downstream is exactly the same operating point
as before the rescale -- just expressed at a different numerical threshold.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def rescale_csv(path: Path) -> None:
    df = pd.read_csv(path)
    for col in ("gate_prob", "cascade_score"):
        if col not in df.columns:
            print(f"  ! {path.name}: column '{col}' missing — skipped")
            continue
        raw = df[col].to_numpy(dtype=np.float64)
        before_lo, before_hi = float(raw.min()), float(raw.max())
        new = ((raw + 1.0) * 0.5).clip(0.0, 1.0).astype(np.float32)
        after_lo, after_hi = float(new.min()), float(new.max())
        df[col] = new
        print(
            f"  {col}: [{before_lo:+.4f}, {before_hi:+.4f}]  ->  "
            f"[{after_lo:.4f}, {after_hi:.4f}]"
        )
    df.to_csv(path, index=False)
    print(f"  rewrote {path}  rows={len(df):,}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--out-dir",
        default="outputs_closr_baseline_temporal",
        help="Directory containing val_closr_predictions.csv and "
        "test_closr_predictions.csv",
    )
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    for name in ("val_closr_predictions.csv", "test_closr_predictions.csv"):
        path = out_dir / name
        if not path.exists():
            print(f"missing: {path}")
            continue
        print(f"fixing: {path}")
        rescale_csv(path)
    print("done.")


if __name__ == "__main__":
    main()
