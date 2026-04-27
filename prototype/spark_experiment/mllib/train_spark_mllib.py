"""Spark MLlib RandomForest trainer for the sklearn comparison.

Reads the same parquet dataset that train_sklearn.py uses and trains a
PySpark MLlib RandomForestClassifier with **matched hyperparameters**
(same n_estimators, same max_depth, same train/test split fraction).

Reports the same fields as train_sklearn.py so the result CSVs from
both scripts can be concatenated and plotted side-by-side:

    engine,input_file,n_rows,n_cols,on_disk_mb,n_estimators,max_depth,
    load_time_sec,train_time_sec,predict_time_sec,peak_rss_mb,
    accuracy,precision,recall,f1

The script is meant to be launched via spark-submit so Spark's
distributed evaluator + JVM are properly initialised. We use
``--master local[*]`` so the same script works on a single 12-core
server (no cluster needed for this benchmark).

Usage::

    /opt/spark/bin/spark-submit \
        --master local[*] \
        --conf spark.driver.memory=8g \
        --conf spark.executor.memory=8g \
        train_spark_mllib.py \
            --input /tmp/synth/sweep/1gb.parquet \
            --n-estimators 100 --max-depth 20 \
            --result-csv /tmp/synth/results.csv
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


def _emit_result(result: dict, result_csv: str | None) -> None:
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
                   help="Spark RF numTrees (default: 100).")
    p.add_argument("--max-depth", type=int, default=20,
                   help="Spark RF maxDepth (default: 20). "
                        "Note: Spark caps maxDepth at 30.")
    p.add_argument("--test-size", type=float, default=0.20)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--shuffle-partitions", type=int, default=12,
                   help="spark.sql.shuffle.partitions (default: 12 = 1 per core).")
    p.add_argument("--result-csv", default=None)
    args = p.parse_args()

    spark = (SparkSession.builder
        .appName("nidsaas-spark-mllib-bench")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        # Faster local read: skip schema merge across files
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print(f"[spark] reading {args.input}")
    t0 = time.time()
    df = spark.read.parquet(args.input)
    n_rows = df.count()       # forces a full read
    load_time = time.time() - t0
    n_cols = len(df.columns)
    print(f"[spark] loaded {n_rows:,} rows × {n_cols} cols in {load_time:.1f}s")

    # Class balance — for sanity check
    print("[spark] label distribution:")
    df.groupBy("binary_label").count().show()

    # Cast label to double + assemble features into Vector column
    feature_cols = [c for c in df.columns if c != "binary_label"]
    df = df.withColumn("label", F.col("binary_label").cast("double"))
    assembler = VectorAssembler(inputCols=feature_cols,
                                outputCol="features",
                                handleInvalid="keep")
    data = assembler.transform(df).select("features", "label")

    # Persist with MEMORY_AND_DISK so Spark spills serialised partitions
    # to disk when heap is tight. Default cache() is MEMORY_ONLY which OOMs
    # the JVM when the dataset exceeds driver heap (~8 GB). In PySpark
    # MEMORY_AND_DISK is already serialised (deserialized=False), unlike
    # the Scala API where you'd want MEMORY_AND_DISK_SER explicitly.
    data = data.persist(StorageLevel.MEMORY_AND_DISK)

    train, test = data.randomSplit([1.0 - args.test_size, args.test_size],
                                   seed=args.seed)
    train_count = train.count()
    test_count  = test.count()
    print(f"[spark] split: train={train_count:,}  test={test_count:,}")

    print(f"[spark] training RF (numTrees={args.n_estimators}, "
          f"maxDepth={args.max_depth}) ...")

    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features",
        numTrees=args.n_estimators,
        maxDepth=min(args.max_depth, 30),    # Spark cap
        seed=args.seed,
    )

    t0 = time.time()
    model = rf.fit(train)
    train_time = time.time() - t0
    print(f"[spark] train done in {train_time:.1f}s")

    t0 = time.time()
    pred = model.transform(test)
    pred.cache()
    pred.count()                              # materialise
    predict_time = time.time() - t0

    eval_acc = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    eval_f1 = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1")
    eval_prec = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    eval_rec = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedRecall")

    acc  = eval_acc.evaluate(pred)
    f1   = eval_f1.evaluate(pred)
    prec = eval_prec.evaluate(pred)
    rec  = eval_rec.evaluate(pred)

    on_disk_mb = Path(args.input).stat().st_size / (1024 ** 2)
    # Spark doesn't expose Python peak RSS easily — leave 0 for now;
    # the bench script measures wall-clock + uses `time -v` for memory.
    peak_rss = 0.0

    result = {
        "engine": "spark_mllib",
        "input_file": Path(args.input).name,
        "n_rows": n_rows,
        "n_cols": n_cols,
        "on_disk_mb": round(on_disk_mb, 1),
        "n_estimators": args.n_estimators,
        "max_depth": args.max_depth,
        "n_jobs": args.shuffle_partitions,
        "load_time_sec": round(load_time, 2),
        "train_time_sec": round(train_time, 2),
        "predict_time_sec": round(predict_time, 2),
        "peak_rss_mb": peak_rss,
        "accuracy": round(acc, 4),
        "precision": round(prec, 4),
        "recall": round(rec, 4),
        "f1": round(f1, 4),
    }

    print(f"[spark] eval: acc={acc:.4f}  f1={f1:.4f}  "
          f"prec={prec:.4f}  rec={rec:.4f}")

    _emit_result(result, args.result_csv)
    spark.stop()


if __name__ == "__main__":
    main()
