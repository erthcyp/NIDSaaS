"""sklearn baseline trainer for the Spark MLlib comparison.

Loads a parquet dataset (produced by synth_dataset_gen.py), trains a
sklearn RandomForestClassifier on it, and reports:
    * load_time_sec
    * train_time_sec
    * predict_time_sec
    * peak_rss_mb
    * accuracy / f1 / precision / recall on a held-out 20% test split

The same hyperparameters are mirrored in train_spark_mllib.py so the
comparison is apples-to-apples (same forest size, same depth, same
features). The only thing that differs is the *engine*.

Usage::

    python3 train_sklearn.py --input /tmp/synth/sweep/1gb.parquet \
        --n-estimators 100 --max-depth 20 --n-jobs 8 \
        --result-csv /tmp/synth/results.csv
"""
from __future__ import annotations

import argparse
import json
import os
import resource
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, f1_score, precision_score, recall_score,
)
from sklearn.model_selection import train_test_split


def _peak_rss_mb() -> float:
    """Peak resident set size in MB (Linux returns kB from getrusage)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _emit_result(result: dict, result_csv: str | None) -> None:
    """Print as JSON line + optionally append to CSV for benchmark sweep."""
    print("RESULT " + json.dumps(result, sort_keys=True), flush=True)

    if result_csv:
        out = Path(result_csv)
        write_header = not out.exists()
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "a") as f:
            if write_header:
                f.write(",".join(result.keys()) + "\n")
            f.write(",".join(str(v) for v in result.values()) + "\n")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True,
                   help="Path to .parquet dataset.")
    p.add_argument("--n-estimators", type=int, default=100,
                   help="RandomForest n_estimators (default: 100).")
    p.add_argument("--max-depth", type=int, default=20,
                   help="RandomForest max_depth (default: 20).")
    p.add_argument("--n-jobs", type=int, default=-1,
                   help="sklearn n_jobs (-1 = all cores; default: -1).")
    p.add_argument("--test-size", type=float, default=0.20,
                   help="Held-out fraction for test eval (default: 0.20).")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--result-csv", default=None,
                   help="Optional path to append a CSV row for sweep summary.")
    args = p.parse_args()

    print(f"[sklearn] reading {args.input}")
    t0 = time.time()
    table = pq.read_table(args.input)
    df = table.to_pandas()
    load_time = time.time() - t0
    n_rows, n_cols = df.shape
    print(f"[sklearn] loaded {n_rows:,} rows × {n_cols} cols "
          f"in {load_time:.1f}s")

    y = df["binary_label"].values
    X = df.drop(columns=["binary_label"]).values
    print(f"[sklearn] X={X.shape}  y={y.shape}  "
          f"label balance: {np.bincount(y)}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size,
        random_state=args.seed, stratify=y)

    print(f"[sklearn] train={X_train.shape[0]:,}  test={X_test.shape[0]:,}")
    print(f"[sklearn] training RF "
          f"(n_estimators={args.n_estimators}, max_depth={args.max_depth}, "
          f"n_jobs={args.n_jobs}) ...")

    t0 = time.time()
    clf = RandomForestClassifier(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        n_jobs=args.n_jobs,
        random_state=args.seed,
    )
    clf.fit(X_train, y_train)
    train_time = time.time() - t0
    print(f"[sklearn] train done in {train_time:.1f}s")

    t0 = time.time()
    y_pred = clf.predict(X_test)
    predict_time = time.time() - t0

    acc = accuracy_score(y_test, y_pred)
    f1  = f1_score(y_test, y_pred, average="binary", zero_division=0)
    prec = precision_score(y_test, y_pred, average="binary", zero_division=0)
    rec = recall_score(y_test, y_pred, average="binary", zero_division=0)

    peak_rss = _peak_rss_mb()
    on_disk_mb = Path(args.input).stat().st_size / (1024 ** 2)

    result = {
        "engine": "sklearn",
        "input_file": Path(args.input).name,
        "n_rows": n_rows,
        "n_cols": n_cols,
        "on_disk_mb": round(on_disk_mb, 1),
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "n_jobs": args.n_jobs,
        "load_time_sec": round(load_time, 2),
        "train_time_sec": round(train_time, 2),
        "predict_time_sec": round(predict_time, 2),
        "peak_rss_mb": round(peak_rss, 1),
        "accuracy": round(acc, 4),
        "precision": round(prec, 4),
        "recall": round(rec, 4),
        "f1": round(f1, 4),
    }

    print(f"[sklearn] eval: acc={acc:.4f}  f1={f1:.4f}  "
          f"prec={prec:.4f}  rec={rec:.4f}")
    print(f"[sklearn] peak RSS = {peak_rss:,.0f} MB")

    _emit_result(result, args.result_csv)


if __name__ == "__main__":
    main()
