"""Plot throughput-sweep figure for the Spark-augmented architecture.

Reads the manually-collected results from the throughput sweep
(50, 100, 200, 500, 1000 req/s) and produces a 2-axis figure:

  - Left axis:  delivery rate (%, bar)
  - Right axis: p50 / p99 latency (ms, lines on log-y)

The bar collapses to a small value at 500 / 1000 req/s, while the
latency lines spike — together they tell the "system saturates around
300 req/s" story in one panel.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


# Match the Kafka figure: navy bars, warm-coloured latency lines,
# black axis lines + labels.
SPARK_COLOR     = "#1f3a93"   # navy — delivery bars
LATENCY_COLOR   = "#ff7f0e"   # orange — p50 latency
LATENCY99_COLOR = "#d62728"   # red   — p99 latency (worse-case)
NEUTRAL         = "#7f7f7f"


# Hard-coded measurements (from /tmp/synth/throughput_sweep/*.log)
DATA = [
    {"rate": 50,   "sent": 1500, "delivered": 1500, "p50": 157.2, "p99": 427.6},
    {"rate": 100,  "sent": 3000, "delivered": 3000, "p50": 143.8, "p99": 255.5},
    {"rate": 200,  "sent": 6000, "delivered": 6000, "p50": 3142.0, "p99": 6340.2},
    {"rate": 500,  "sent": 4494, "delivered": 1053, "p50": 2223.1, "p99": 605469.1},
    {"rate": 1000, "sent": 3827, "delivered":  568, "p50": 243103.6, "p99": 547386.0},
]


def _clean_ax(ax) -> None:
    ax.spines["top"].set_visible(False)
    ax.tick_params(width=0.8, length=4)
    # Light horizontal grid (Kafka-figure style)
    ax.grid(True, axis="y", color="#cccccc", linewidth=0.6, alpha=0.8, zorder=0)
    ax.set_axisbelow(True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default=".")
    args = p.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rates = np.array([d["rate"] for d in DATA])
    delivered_pct = np.array([100.0 * d["delivered"] / d["sent"] for d in DATA])
    p50 = np.array([d["p50"] for d in DATA])
    p99 = np.array([d["p99"] for d in DATA])

    fig, ax1 = plt.subplots(figsize=(7, 4.2), constrained_layout=True)

    # -- Bars: delivery rate (left axis) --
    bars = ax1.bar([str(r) for r in rates], delivered_pct,
                   color=SPARK_COLOR, alpha=0.55, edgecolor="none", width=0.55,
                   label="Delivery rate (%)")
    ax1.set_ylim(0, 110)
    ax1.set_ylabel("Delivery rate (%)")
    ax1.spines["left"].set_color("black")
    ax1.spines["left"].set_linewidth(0.8)
    ax1.spines["bottom"].set_linewidth(0.8)
    ax1.spines["right"].set_visible(False)
    ax1.spines["top"].set_visible(False)
    ax1.grid(False)
    ax1.set_xlabel("Offered load (requests/second)")

    # Annotate delivery % above each bar — black, regular weight (Kafka style)
    for bar, pct in zip(bars, delivered_pct):
        ax1.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + 2,
                 f"{pct:.0f}%", ha="center", va="bottom", fontsize=9,
                 color="black")

    # -- Lines: latency (right axis, log scale) --
    ax2 = ax1.twinx()
    x_pos = np.arange(len(rates))
    ax2.plot(x_pos, p50, "o-", color=LATENCY_COLOR, lw=1.8, ms=5,
             mfc="white", mew=1.5, label="p50 latency")
    ax2.plot(x_pos, p99, "s--", color=LATENCY99_COLOR, lw=1.8, ms=5,
             mfc="white", mew=1.5, label="p99 latency")
    ax2.set_yscale("log")
    ax2.set_ylim(50, 1e6)
    ax2.set_ylabel("Latency (ms, log scale)")
    ax2.spines["right"].set_color("black")
    ax2.spines["right"].set_linewidth(0.8)
    ax2.spines["top"].set_visible(False)
    ax2.spines["left"].set_visible(False)
    ax2.spines["bottom"].set_visible(False)
    ax2.grid(False)

    # Combined legend at top center, light gray frame (Kafka style)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               loc="upper center", bbox_to_anchor=(0.5, 1.15),
               ncol=3, fontsize=9, frameon=True, edgecolor="#cccccc",
               fancybox=False)

    # Saturation annotation between 200 and 500
    ax1.axvspan(2.5, 4.5, alpha=0.06, color="red", zorder=0)
    ax1.text(3.5, 110, "saturation",
             ha="center", va="bottom", fontsize=10,
             color="#a00000")

    fig.savefig(out_dir / "fig_throughput_sweep.pdf", bbox_inches="tight")
    fig.savefig(out_dir / "fig_throughput_sweep.png", dpi=200, bbox_inches="tight")
    print(f"  saved {out_dir / 'fig_throughput_sweep.pdf'}")
    print(f"  saved {out_dir / 'fig_throughput_sweep.png'}")


if __name__ == "__main__":
    main()
