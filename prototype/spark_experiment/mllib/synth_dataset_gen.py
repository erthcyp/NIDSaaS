"""Synthetic CIC-IDS2017-style flow dataset generator.

Produces .parquet files with ~80 numeric features + a binary label
column at controllable size (rows or GB). Reproducible (fixed seed).
Stream-writes in batches so 50 GB outputs do not blow up RAM.

Why this exists
---------------
The Spark MLlib vs. sklearn retraining benchmark needs *labelled
training data* sweepable across orders of magnitude (0.5 GB → 50 GB)
to find the crossover point where Spark wins. Shipping ~50 GB of real
CIC-IDS2017 PCAP/CSV to the SIIT server every time we re-experiment
is impractical, and the advisor has approved synthetic data for the
benchmark.

Feature distributions are calibrated *roughly* against publicly
documented CIC-IDS2017 statistics — enough that downstream models
behave qualitatively similarly to training on real data (RandomForest
fits in similar time, hits comparable accuracy regimes), but no claim
is made that the absolute numbers transfer 1:1. The point of the
benchmark is to compare Spark MLlib vs. sklearn under the **same**
synthetic load, so absolute calibration is not required.

Usage
-----
    # By row count
    python3 synth_dataset_gen.py --rows 2_500_000 --output /tmp/synth/1gb.parquet

    # By target uncompressed size in GB
    python3 synth_dataset_gen.py --size-gb 5 --output /tmp/synth/5gb.parquet

    # Sweep helper (creates ~6 files for benchmark)
    for sz in 0.5 1 2 5 10 20; do
        python3 synth_dataset_gen.py --size-gb $sz \
                --output /tmp/synth/${sz}gb.parquet
    done

Notes on file format
--------------------
Parquet is preferred over CSV because:
  * Spark reads parquet ~10× faster than CSV (columnar + zero-parse)
  * sklearn (via pandas.read_parquet) handles it fine
  * On-disk size is ~3-5× smaller than CSV for the same data
"""
from __future__ import annotations

import argparse
import os
import random
import time
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# CIC-IDS2017 canonical feature list (78 features, matching the canonicalized
# names used by pipeline/load_data.py + signature_rate_rules.py).
#
# For each feature we hold:
#   (mean_benign, std_benign, mean_attack, std_attack)
# Distributions are intentionally simple (Gaussian, clipped to >= 0) — they
# are NOT meant to be a perfect simulation of the real dataset, just enough
# variation that ML models train non-trivially.
# ---------------------------------------------------------------------------
FEATURE_DISTS: Dict[str, Tuple[float, float, float, float]] = {
    # Flow geometry
    "flow_duration":               (1.5e6,  3e6,  5e4,   2e5),
    "total_fwd_packets":           (10,     30,    50,    150),
    "total_backward_packets":      (8,      25,    20,    80),
    "total_length_of_fwd_packets": (1500,   5000,  3000,  12000),
    "total_length_of_bwd_packets": (2000,   6000,  500,   2000),

    # Per-direction packet length stats
    "fwd_packet_length_max":       (500,   600,   1500,  500),
    "fwd_packet_length_min":       (40,    60,    40,    50),
    "fwd_packet_length_mean":      (150,   200,   400,   300),
    "fwd_packet_length_std":       (100,   150,   200,   200),
    "bwd_packet_length_max":       (700,   500,   400,   400),
    "bwd_packet_length_min":       (40,    60,    40,    50),
    "bwd_packet_length_mean":      (250,   200,   100,   150),
    "bwd_packet_length_std":       (150,   200,   100,   150),

    # Flow rates — attacks have *much* higher rates (DoS-ish bias)
    "flow_bytes_s":                (5e4,   2e5,   3e7,   5e7),
    "flow_packets_s":              (50,    200,   8e4,   2e5),

    # Inter-arrival time (IAT) stats — attacks are bursty (low IAT)
    "flow_iat_mean":               (5e4,   1.5e5, 50,    300),
    "flow_iat_std":                (8e4,   2e5,   500,   2000),
    "flow_iat_max":                (5e5,   1.5e6, 2000,  8000),
    "flow_iat_min":                (50,    200,   1,     5),
    "fwd_iat_total":               (1e6,   3e6,   5e3,   2e4),
    "fwd_iat_mean":                (8e4,   2e5,   80,    300),
    "fwd_iat_std":                 (1e5,   2e5,   500,   2000),
    "fwd_iat_max":                 (5e5,   1.5e6, 3000,  10000),
    "fwd_iat_min":                 (60,    200,   2,     8),
    "bwd_iat_total":               (8e5,   2e6,   3e3,   1e4),
    "bwd_iat_mean":                (8e4,   2e5,   100,   400),
    "bwd_iat_std":                 (1e5,   2e5,   400,   1500),
    "bwd_iat_max":                 (5e5,   1.5e6, 2000,  8000),
    "bwd_iat_min":                 (60,    200,   2,     8),

    # TCP flags — attacks tend to have more SYN/RST anomalies
    "fwd_psh_flags":               (0.1,   0.4,   0.5,   1.0),
    "bwd_psh_flags":               (0.1,   0.3,   0.2,   0.5),
    "fwd_urg_flags":               (0.0,   0.1,   0.05,  0.2),
    "bwd_urg_flags":               (0.0,   0.1,   0.05,  0.2),
    "fwd_header_length":           (200,   400,   600,   1500),
    "bwd_header_length":           (160,   300,   200,   600),

    # Per-direction packet rates
    "fwd_packets_s":               (30,    100,   5e4,   1e5),
    "bwd_packets_s":               (20,    80,    1e4,   3e4),

    # Aggregate packet length stats
    "min_packet_length":           (40,    50,    40,    40),
    "max_packet_length":           (700,   500,   1500,  500),
    "packet_length_mean":          (200,   200,   300,   300),
    "packet_length_std":           (150,   200,   200,   250),
    "packet_length_variance":      (3e4,   8e4,   8e4,   2e5),

    # Per-flow flag totals
    "fin_flag_count":              (1.0,   1.5,   0.5,   1.0),
    "syn_flag_count":              (1.0,   1.0,   3.5,   3.0),
    "rst_flag_count":              (0.2,   0.6,   2.0,   3.0),
    "psh_flag_count":              (1.5,   2.5,   2.0,   3.0),
    "ack_flag_count":              (10,    25,    8,     20),
    "urg_flag_count":              (0.0,   0.1,   0.1,   0.4),
    "cwe_flag_count":              (0.0,   0.05,  0.0,   0.05),
    "ece_flag_count":              (0.0,   0.05,  0.0,   0.05),

    # Ratios + sub-flow stats
    "down_up_ratio":               (1.2,   1.0,   0.4,   0.8),
    "average_packet_size":         (200,   150,   300,   250),
    "avg_fwd_segment_size":        (150,   200,   400,   300),
    "avg_bwd_segment_size":        (250,   200,   100,   150),
    "fwd_header_length_1":         (200,   400,   600,   1500),

    # Bulk-transfer features
    "fwd_avg_bytes_bulk":          (0.0,   500,   0.0,   100),
    "fwd_avg_packets_bulk":        (0.0,   1.0,   0.0,   0.5),
    "fwd_avg_bulk_rate":           (0.0,   2e4,   0.0,   5e3),
    "bwd_avg_bytes_bulk":          (0.0,   500,   0.0,   100),
    "bwd_avg_packets_bulk":        (0.0,   1.0,   0.0,   0.5),
    "bwd_avg_bulk_rate":           (0.0,   2e4,   0.0,   5e3),

    # Sub-flow counts/sizes
    "subflow_fwd_packets":         (10,    30,    50,    150),
    "subflow_fwd_bytes":           (1500,  5000,  3000,  12000),
    "subflow_bwd_packets":         (8,     25,    20,    80),
    "subflow_bwd_bytes":           (2000,  6000,  500,   2000),

    # TCP window sizes
    "init_win_bytes_forward":      (8000,  5000,  3000,  4000),
    "init_win_bytes_backward":     (4000,  3000,  500,   2000),
    "act_data_pkt_fwd":            (5,     15,    20,    80),
    "min_seg_size_forward":        (20,    10,    20,    10),

    # Idle/active timing
    "active_mean":                 (5e4,   1e5,   200,   1000),
    "active_std":                  (3e4,   8e4,   100,   500),
    "active_max":                  (3e5,   8e5,   2000,  6000),
    "active_min":                  (1e3,   5e3,   10,    50),
    "idle_mean":                   (8e5,   2e6,   1e4,   5e4),
    "idle_std":                    (5e5,   1e6,   8e3,   3e4),
    "idle_max":                    (3e6,   8e6,   5e4,   2e5),
    "idle_min":                    (5e4,   2e5,   500,   2000),

    # Ports + protocol — discrete, sampled separately below
    # (kept as features but sampled differently from the gaussian path)
    "destination_port":            (-1,    -1,    -1,    -1),
    "protocol":                    (-1,    -1,    -1,    -1),
}


# Discrete features — sampled by class, not by gaussian
DISCRETE_BENIGN_PORTS = [80, 443, 8080, 8443, 22, 53, 25, 110, 143, 993, 995]
DISCRETE_ATTACK_PORTS = [22, 23, 21, 80, 443, 3389, 445, 139, 1433, 3306]
PROTOCOLS = [6, 17, 1]   # TCP, UDP, ICMP


def _gen_batch(n_rows: int,
               attack_ratio: float,
               rng: np.random.Generator) -> Dict[str, np.ndarray]:
    """Generate one batch of rows. Returns dict[col_name -> np.ndarray]."""
    n_attack = int(round(n_rows * attack_ratio))
    n_benign = n_rows - n_attack

    # Label vector: 0 = benign, 1 = attack. Shuffle so order is mixed.
    labels = np.concatenate([
        np.zeros(n_benign, dtype=np.int8),
        np.ones(n_attack,  dtype=np.int8),
    ])
    perm = rng.permutation(n_rows)
    labels = labels[perm]

    cols: Dict[str, np.ndarray] = {}

    for feat, (mb, sb, ma, sa) in FEATURE_DISTS.items():
        if mb == -1:   # discrete features handled below
            continue

        # Gaussian per class, vectorized
        benign_vals = rng.normal(mb, sb, n_benign).astype(np.float32)
        attack_vals = rng.normal(ma, sa, n_attack).astype(np.float32)
        merged = np.concatenate([benign_vals, attack_vals])[perm]

        # CIC features are >= 0 in practice
        np.clip(merged, 0, None, out=merged)
        cols[feat] = merged

    # Ports — sampled from class-specific set
    benign_ports = rng.choice(DISCRETE_BENIGN_PORTS, n_benign).astype(np.int32)
    attack_ports = rng.choice(DISCRETE_ATTACK_PORTS, n_attack).astype(np.int32)
    cols["destination_port"] = np.concatenate([benign_ports, attack_ports])[perm]

    cols["protocol"] = rng.choice(PROTOCOLS, n_rows).astype(np.int32)

    cols["binary_label"] = labels

    return cols


def _estimate_rows_for_size(target_gb: float) -> int:
    """Rough conversion from target uncompressed size → row count.

    78 numeric features ~ 78 * 4 bytes (float32) = 312 B + label byte ≈ 320 B.
    Parquet usually compresses 2-4× so disk is smaller than this estimate;
    we deliberately overshoot the row count slightly so the on-disk file
    hits at least the requested size.
    """
    bytes_per_row = 320
    # parquet compression ~3× → over-shoot rows so on-disk gets close
    overshoot = 2.5
    target_bytes = target_gb * (1024 ** 3)
    return int(target_bytes * overshoot / bytes_per_row)


def generate(output_path: str,
             n_rows: int,
             attack_ratio: float,
             seed: int,
             batch_rows: int) -> None:
    """Stream-write a synthetic parquet dataset."""
    rng = np.random.default_rng(seed)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Build schema once (so streaming write is consistent)
    feature_cols = [k for k in FEATURE_DISTS.keys()]
    schema_fields = []
    for c in feature_cols:
        if c in ("destination_port", "protocol"):
            schema_fields.append(pa.field(c, pa.int32()))
        else:
            schema_fields.append(pa.field(c, pa.float32()))
    schema_fields.append(pa.field("binary_label", pa.int8()))
    schema = pa.schema(schema_fields)

    print(f"[synth] writing {n_rows:,} rows → {out}")
    print(f"[synth] schema: {len(schema_fields)} columns "
          f"(78 features + binary_label)")
    print(f"[synth] attack_ratio={attack_ratio}  seed={seed}  "
          f"batch_rows={batch_rows:,}")

    t0 = time.time()
    written = 0
    with pq.ParquetWriter(out, schema, compression="snappy") as writer:
        while written < n_rows:
            this_batch = min(batch_rows, n_rows - written)
            cols = _gen_batch(this_batch, attack_ratio, rng)

            arrays = []
            for f in feature_cols:
                arrays.append(pa.array(cols[f]))
            arrays.append(pa.array(cols["binary_label"]))

            tbl = pa.Table.from_arrays(arrays, schema=schema)
            writer.write_table(tbl)
            written += this_batch
            elapsed = time.time() - t0
            rate = written / max(elapsed, 1e-9)
            print(f"  [synth] {written:,}/{n_rows:,}  "
                  f"({100 * written / n_rows:5.1f}%)  "
                  f"{rate:,.0f} rows/s  "
                  f"elapsed={elapsed:.1f}s",
                  flush=True)

    elapsed = time.time() - t0
    on_disk_mb = out.stat().st_size / (1024 ** 2)
    print(f"[synth] done — {n_rows:,} rows, "
          f"{on_disk_mb:,.1f} MB on disk, "
          f"{elapsed:.1f}s wall clock "
          f"({n_rows/max(elapsed,1e-9):,.0f} rows/s avg)")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--rows", type=int,
                   help="Exact number of rows to generate.")
    g.add_argument("--size-gb", type=float,
                   help="Approximate target uncompressed size in GB. "
                        "On-disk parquet will be ~3× smaller after compression.")
    p.add_argument("--output", type=str, required=True,
                   help="Output .parquet path.")
    p.add_argument("--seed", type=int, default=42,
                   help="RNG seed (default: 42 for reproducibility).")
    p.add_argument("--attack-ratio", type=float, default=0.20,
                   help="Fraction of rows labelled as attack (default: 0.20).")
    p.add_argument("--batch-rows", type=int, default=500_000,
                   help="Rows per pyarrow write batch (default: 500k). "
                        "Lower if RAM-constrained.")
    args = p.parse_args()

    n_rows = args.rows if args.rows is not None \
             else _estimate_rows_for_size(args.size_gb)

    if not 0.0 < args.attack_ratio < 1.0:
        raise SystemExit("--attack-ratio must be in (0, 1)")

    generate(args.output, n_rows,
             attack_ratio=args.attack_ratio,
             seed=args.seed,
             batch_rows=args.batch_rows)


if __name__ == "__main__":
    main()
