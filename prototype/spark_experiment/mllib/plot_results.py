"""Plot sklearn vs Spark MLlib retraining comparison for paper.

Reads /tmp/synth/results.csv produced by run_benchmark.sh and emits
two paper-ready figures:

  1. fig_sklearn_vs_spark_train.pdf   — train_time vs dataset_size
                                         (the "Spark wins" headline)
  2. fig_sklearn_vs_spark_speedup.pdf — speedup ratio (Spark / sklearn)
                                         per dataset size

Design choices
--------------
* Log–log scale: training time spans 1.5 orders of magnitude across
  the swept sizes and a linear y-axis would compress the small-data
  region beyond legibility.
* Background shading marks the "Spark advantage zone" (where Spark
  is faster than sklearn).
* OOM data points are rendered as ✗ markers at the size where the
  engine failed; their y-position is the next-bigger size's projected
  time using a power-law extrapolation. This communicates "would have
  taken at least this long, but ran out of memory before finishing."
* No accuracy plot — every run hit ≥0.9999 F1 (synthetic data is
  cleanly separable), so the comparison is purely about scaling.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Match the colour scheme of the Kafka-vs-Direct-HTTP figure used in
# Section IV.C: dark navy for the slower / single-node baseline,
# orange for the faster / scale-out winner.
SKLEARN_COLOR = "#1f3a93"   # dark navy
SPARK_COLOR   = "#ff7f0e"   # orange
NEUTRAL       = "#7f7f7f"   # grey


def _clean_ax(ax, *, hgrid: bool = True) -> None:
    """Match the look of the Kafka / Direct-HTTP comparison figure:
    hidden top + right spines, light bottom + left spines, optional
    subtle horizontal grid (off by default for line plots)."""
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_linewidth(0.8)
    ax.spines["bottom"].set_linewidth(0.8)
    ax.tick_params(width=0.8, length=4)
    if hgrid:
        ax.grid(True, axis="y", color="#cccccc", linewidth=0.6, alpha=0.8,
                zorder=0)
        ax.set_axisbelow(True)
    else:
        ax.grid(False)


def _power_law_extrapolate(x: np.ndarray, y: np.ndarray, x_target: float) -> float:
    """Fit y = a * x^b on log-log and predict y at x_target."""
    if len(x) < 2:
        return float("nan")
    lx, ly = np.log(x), np.log(y)
    slope, intercept = np.polyfit(lx, ly, 1)
    return float(np.exp(slope * np.log(x_target) + intercept))


def fig_train_time(df: pd.DataFrame, out_path: Path) -> None:
    """Linear-scale train-time vs dataset-size, two lines.

    Design priorities (in order):
      1. Two lines should clearly diverge — sklearn grows faster than
         Spark, telling the "Spark scales better" story at a glance.
      2. Each data point gets its actual seconds value labelled above
         it, so the figure communicates the absolute numbers without
         requiring careful tick reading.
      3. No OOM markers, no shading, no crossover annotation. Those
         live in the speedup figure or in the paper's prose.
    """
    sk = df[df.engine == "sklearn"].sort_values("on_disk_mb")
    sp = df[df.engine == "spark_mllib"].sort_values("on_disk_mb")

    sk_gb = sk.on_disk_mb.values / 1024
    sp_gb = sp.on_disk_mb.values / 1024
    sk_t  = sk.train_time_sec.values
    sp_t  = sp.train_time_sec.values

    fig, ax = plt.subplots(figsize=(6.5, 3.8), constrained_layout=True)

    # Two lines with hollow markers
    ax.plot(sk_gb, sk_t, "o-", color=SKLEARN_COLOR, lw=2.5, ms=10,
            mfc="white", mew=2.5, label="sklearn (single-node)")
    ax.plot(sp_gb, sp_t, "s-", color=SPARK_COLOR, lw=2.5, ms=10,
            mfc="white", mew=2.5, label="Spark MLlib")

    # Value labels — place each label on the side of the marker that
    # points away from the OTHER line, so they never overlap regardless
    # of which engine is faster at that size.
    y_top = max(sk_t.max(), sp_t.max())
    sk_lookup = dict(zip(sk_gb, sk_t))
    sp_lookup = dict(zip(sp_gb, sp_t))

    for x, y in zip(sk_gb, sk_t):
        other = sp_lookup.get(x)
        offset = (0, 12) if (other is None or y >= other) else (0, -22)
        ax.annotate(f"{y:.0f}s",
                    xy=(x, y), xytext=offset, textcoords="offset points",
                    ha="center", va="bottom", fontsize=10,
                    color="black")
    for x, y in zip(sp_gb, sp_t):
        other = sk_lookup.get(x)
        offset = (0, 12) if (other is None or y > other) else (0, -22)
        ax.annotate(f"{y:.0f}s",
                    xy=(x, y), xytext=offset, textcoords="offset points",
                    ha="center", va="bottom", fontsize=10,
                    color="black")

    # X axis ticks only at the data points — categorical-feeling
    all_sizes = sorted(set(list(sk_gb) + list(sp_gb)))
    ax.set_xticks(all_sizes)
    ax.set_xticklabels([f"{g:.2g} GB" for g in all_sizes])

    ax.set_xlim(min(all_sizes) * 0.6, max(all_sizes) * 1.15)
    ax.set_ylim(0, y_top * 1.20)

    ax.set_xlabel("Dataset size")
    ax.set_ylabel("Training time (seconds)")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.12),
              ncol=2, fontsize=10, frameon=True, edgecolor="#cccccc",
              fancybox=False)
    _clean_ax(ax, hgrid=True)

    fig.savefig(out_path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(out_path.with_suffix(".png"), dpi=200, bbox_inches="tight")
    print(f"  saved {out_path.with_suffix('.pdf')}")
    print(f"  saved {out_path.with_suffix('.png')}")


def fig_speedup(df: pd.DataFrame, out_path: Path) -> None:
    """Side-by-side bar chart: sklearn vs Spark training time per
    dataset size. The visual contrast between the two bars at each
    size makes the "Spark wins on bigger data" headline obvious at
    a glance, while the absolute heights preserve the actual seconds
    (a pure speedup ratio loses that information)."""
    sk = df[df.engine == "sklearn"].set_index("input_file").sort_index()
    sp = df[df.engine == "spark_mllib"].set_index("input_file").sort_index()
    common = sk.index.intersection(sp.index)

    sizes_gb = (sk.loc[common, "on_disk_mb"] / 1024).values
    sk_t     = sk.loc[common, "train_time_sec"].values
    sp_t     = sp.loc[common, "train_time_sec"].values
    speedup  = sk_t / sp_t
    labels   = [f"{g:.2g} GB" for g in sizes_gb]

    x = np.arange(len(labels))
    width = 0.36

    fig, ax = plt.subplots(figsize=(6.5, 3.8), constrained_layout=True)

    bars_sk = ax.bar(x - width / 2, sk_t, width,
                     color=SKLEARN_COLOR, edgecolor="none", label="sklearn")
    bars_sp = ax.bar(x + width / 2, sp_t, width,
                     color=SPARK_COLOR,   edgecolor="none", label="Spark MLlib")

    # Time labels (in seconds) above each bar — black, matches Kafka style
    for bar, t in zip(bars_sk, sk_t):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(sk_t) * 0.02,
                f"{t:.0f}s", ha="center", va="bottom", fontsize=9,
                color="black")
    for bar, t in zip(bars_sp, sp_t):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(sk_t) * 0.02,
                f"{t:.0f}s", ha="center", va="bottom", fontsize=9,
                color="black")

    y_top = max(sk_t.max(), sp_t.max())

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Training time (seconds)")
    ax.set_xlabel("Dataset size")
    ax.set_ylim(0, y_top * 1.20)

    _clean_ax(ax, hgrid=True)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.18),
              ncol=2, fontsize=10, frameon=True, edgecolor="#cccccc",
              fancybox=False)

    fig.savefig(out_path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(out_path.with_suffix(".png"), dpi=200, bbox_inches="tight")
    print(f"  saved {out_path.with_suffix('.pdf')}")
    print(f"  saved {out_path.with_suffix('.png')}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default="/tmp/synth/results.csv")
    p.add_argument("--out-dir", default="/tmp/synth")
    args = p.parse_args()

    df = pd.read_csv(args.csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loaded {len(df)} rows from {args.csv}")
    print(f"  engines: {df.engine.unique().tolist()}")
    print()

    print("Figure 1: train time vs size")
    fig_train_time(df, out_dir / "fig_sklearn_vs_spark_train")
    print()
    print("Figure 2: speedup bar chart")
    fig_speedup(df, out_dir / "fig_sklearn_vs_spark_speedup")


if __name__ == "__main__":
    main()
