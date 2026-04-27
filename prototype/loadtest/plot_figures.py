"""Generate paper-quality PDF figures from outputs/*.json runs.

Usage::

    pip install matplotlib --break-system-packages   # one-time
    python3 plot_figures.py outputs/*.json
    python3 plot_figures.py outputs/*.json --output-dir paper_figs

Emits three vector PDFs into ``--output-dir`` (default ``figures/``):

  * ``fig_e1_frontier.pdf`` — 4-panel line chart, p50 / p95 / p99 / max
    latency vs target rate, kafka vs direct_http. 2-column wide.
    Highlights the crossover at rate=50 where Kafka maintains tail
    latency while Direct-HTTP collapses.

  * ``fig_e2_isolation.pdf`` — grouped bar of per-tenant delivery
    rate. 1-column wide. The "noisy-neighbour collateral damage"
    figure: Kafka preserves quiet tenant near 100%, Direct-HTTP drops
    both tenants to ~55%.

  * ``fig_e5_resources.pdf`` — grouped bar of mean CPU per container.
    1-column wide. Shows the Kafka broker as a fixed CPU premium and
    the Direct-HTTP gateway as a saturation bottleneck.

All three are PDF (vector) at 300 DPI fallback, 9pt serif text, ready
for ``\\includegraphics{...}`` in a 2-column IEEE/ACM template.

Color scheme is color-blind safe (blue + orange-red) and consistent
across all three figures so cross-figure comparisons stay obvious.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from collections import defaultdict
from typing import Any

# Lazy matplotlib import so the harness doesn't grow a hard dep
try:
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError as e:
    sys.stderr.write(
        f"plot_figures requires matplotlib + numpy: {e}\n"
        "Install:  pip install matplotlib --break-system-packages\n"
    )
    sys.exit(1)


# ---- style ---------------------------------------------------------------
KAFKA_COLOR  = "#1f77b4"   # blue (matplotlib tab:blue)
DIRECT_COLOR = "#d62728"   # red  (matplotlib tab:red)

plt.rcParams.update({
    "font.family":      "serif",
    "font.size":        9,
    "axes.titlesize":   9,
    "axes.labelsize":   9,
    "xtick.labelsize":  8,
    "ytick.labelsize":  8,
    "legend.fontsize":  8,
    "legend.frameon":   False,
    "axes.grid":        True,
    "grid.linewidth":   0.4,
    "grid.alpha":       0.4,
    "lines.linewidth":  1.5,
    "lines.markersize": 4,
    "savefig.dpi":      300,
    "savefig.bbox":     "tight",
    "pdf.fonttype":     42,    # embed TrueType (vector friendly)
    "ps.fonttype":      42,
})


# ---- helpers -------------------------------------------------------------
def _load_runs(paths: list[str]) -> list[dict[str, Any]]:
    runs = []
    for p in paths:
        with open(p) as f:
            d = json.load(f)
        d["_path"] = os.path.basename(p)
        runs.append(d)
    return runs


def _expand_globs(paths: list[str]) -> list[str]:
    out: list[str] = []
    for p in paths:
        if any(c in p for c in "*?[]"):
            out.extend(sorted(glob.glob(p)))
        else:
            out.append(p)
    return [p for p in out if os.path.isfile(p)]


def _save(fig, out_path: str) -> None:
    fig.savefig(out_path)
    plt.close(fig)
    sys.stderr.write(f"  wrote {out_path}\n")


# ---- Fig 2 — E1 throughput-latency frontier -----------------------------
def fig_e1_frontier(runs: list[dict[str, Any]], out_path: str) -> None:
    e1 = [r for r in runs
          if re.match(r"^e1_rate\d+$", r["summary"]["scenario"])]
    if not e1:
        sys.stderr.write("  E1 runs not found — skipping fig_e1_frontier\n")
        return

    # by_mode[mode][rate] = summary
    by_mode: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for r in e1:
        s = r["summary"]
        rate = int(re.match(r"e1_rate(\d+)", s["scenario"]).group(1))
        by_mode[s["mode"]][rate] = s

    rates = sorted({rt for d in by_mode.values() for rt in d.keys()})
    if "kafka" not in by_mode or "direct_http" not in by_mode:
        sys.stderr.write("  E1 missing one of kafka/direct_http — skipping\n")
        return

    fig, axes = plt.subplots(1, 4, figsize=(7.16, 2.2), constrained_layout=True)
    metrics = [("e2e_ms_p50", "p50"),
               ("e2e_ms_p95", "p95"),
               ("e2e_ms_p99", "p99"),
               ("e2e_ms_max", "max")]

    for ax, (key, label) in zip(axes, metrics):
        ky = [by_mode["kafka"].get(r, {}).get(key) for r in rates]
        dy = [by_mode["direct_http"].get(r, {}).get(key) for r in rates]
        ax.plot(rates, ky, marker="o", color=KAFKA_COLOR,  label="Kafka")
        ax.plot(rates, dy, marker="s", color=DIRECT_COLOR, label="Direct-HTTP",
                linestyle="--")
        ax.set_xlabel("Target rate (rps)")
        ax.set_ylabel(f"{label} latency (ms)")
        ax.set_xscale("log")
        ax.set_xticks(rates)
        ax.set_xticklabels([str(r) for r in rates])
        # log-y for max only — its range spans 0-2000+ ms
        if label == "max":
            ax.set_yscale("log")

    handles, labels_ = axes[0].get_legend_handles_labels()
    # Anchor legend's LOWER edge at figure top (y=1.0) so it grows UPWARD
    # outside the figure — bbox_inches='tight' captures it without ever
    # overlapping panel data, regardless of axis ranges.
    fig.legend(handles, labels_,
               loc="lower center", bbox_to_anchor=(0.5, 1.0),
               ncol=2, frameon=False)

    _save(fig, out_path)


# ---- Fig 3 — E2 noisy-neighbour per-tenant delivery rate ---------------
def fig_e2_isolation(runs: list[dict[str, Any]], out_path: str) -> None:
    e2 = [r for r in runs if r["summary"]["scenario"] == "e2_noisy_neighbour"]
    if not e2:
        sys.stderr.write("  E2 runs not found — skipping fig_e2_isolation\n")
        return

    by_mode = {r["summary"]["mode"]: r["summary"] for r in e2}
    if "kafka" not in by_mode or "direct_http" not in by_mode:
        sys.stderr.write("  E2 missing one of kafka/direct_http — skipping\n")
        return

    tenants = sorted(
        set(by_mode["kafka"].get("per_tenant", {}).keys())
        | set(by_mode["direct_http"].get("per_tenant", {}).keys())
    )
    kafka_d  = [by_mode["kafka"]      ["per_tenant"].get(t, {}).get("delivery_rate", 0) * 100
                for t in tenants]
    direct_d = [by_mode["direct_http"]["per_tenant"].get(t, {}).get("delivery_rate", 0) * 100
                for t in tenants]

    fig, ax = plt.subplots(figsize=(3.5, 2.4), constrained_layout=True)
    x = np.arange(len(tenants))
    w = 0.38
    bars_k = ax.bar(x - w / 2, kafka_d,  w, label="Kafka",       color=KAFKA_COLOR)
    bars_d = ax.bar(x + w / 2, direct_d, w, label="Direct-HTTP", color=DIRECT_COLOR)

    # value labels on top of each bar
    for bars in (bars_k, bars_d):
        for b in bars:
            h = b.get_height()
            ax.text(b.get_x() + b.get_width() / 2, h + 1.5, f"{h:.0f}%",
                    ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels(tenants)
    ax.set_ylabel("Delivery rate (%)")
    # extra headroom + legend above the plot so it never overlaps bars
    ax.set_ylim(0, 115)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.14),
              ncol=2, frameon=False)
    ax.grid(True, axis="y")
    ax.set_axisbelow(True)
    # hide vertical grid lines, only horizontal
    ax.xaxis.grid(False)

    _save(fig, out_path)


# ---- Fig 4 — E5 container CPU comparison --------------------------------
_PRETTY = {
    "nidsaas_gateway":          "gateway",
    "nidsaas_detector":         "detector",
    "nidsaas_alert_fanout":     "alert fanout",
    "nidsaas_kafka":            "kafka broker",
    "nidsaas_webhook_receiver": "webhook",
    "nidsaas_flow_extractor":   "flow ext.",
    "nidsaas_snort_sidecar":    "snort",
}


def fig_e5_resources(runs: list[dict[str, Any]], out_path: str) -> None:
    e5 = [r for r in runs
          if r["summary"]["scenario"] == "e5_resource_footprint"
          and r.get("resource_samples")]
    if not e5:
        sys.stderr.write("  E5 runs not found — skipping fig_e5_resources\n")
        return

    by_mode = {r["summary"]["mode"]: r["resource_samples"] for r in e5}
    if "kafka" not in by_mode or "direct_http" not in by_mode:
        sys.stderr.write("  E5 missing one of kafka/direct_http — skipping\n")
        return

    # Show only containers active in either mode (>=5% CPU avoids idle noise)
    all_c = sorted(set(by_mode["kafka"].keys()) | set(by_mode["direct_http"].keys()))
    active = [c for c in all_c
              if (by_mode["kafka"].get(c, {}).get("cpu_mean", 0) >= 5
                  or by_mode["direct_http"].get(c, {}).get("cpu_mean", 0) >= 5)]
    if not active:
        sys.stderr.write("  E5 no active containers — skipping\n")
        return

    labels = [_PRETTY.get(c, c.replace("nidsaas_", "")) for c in active]
    kafka_cpu  = [by_mode["kafka"]      .get(c, {}).get("cpu_mean", 0) for c in active]
    direct_cpu = [by_mode["direct_http"].get(c, {}).get("cpu_mean", 0) for c in active]

    fig, ax = plt.subplots(figsize=(3.5, 2.4), constrained_layout=True)
    x = np.arange(len(active))
    w = 0.38
    ax.bar(x - w / 2, kafka_cpu,  w, label="Kafka",       color=KAFKA_COLOR)
    ax.bar(x + w / 2, direct_cpu, w, label="Direct-HTTP", color=DIRECT_COLOR)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.set_ylabel("CPU mean (%)")
    # extra headroom + legend above the plot so it never overlaps bars
    ymax = max(max(kafka_cpu, default=0), max(direct_cpu, default=0))
    ax.set_ylim(0, ymax * 1.15)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.14),
              ncol=2, frameon=False)
    ax.grid(True, axis="y")
    ax.set_axisbelow(True)
    ax.xaxis.grid(False)

    _save(fig, out_path)


# ---- Fig E2-throughput — gateway saturation under noisy-neighbour load --
def fig_e2_throughput(runs: list[dict[str, Any]], out_path: str) -> None:
    """Two bars: how many requests each architecture actually sustained
    versus the target (acme 5 rps + globex 200 rps over 60 s = 12{,}300).
    Direct-HTTP saturates the gateway at ~41% of target, exposing a
    fundamental scalability gap that Kafka closes with async handoff."""
    e2 = [r for r in runs if r["summary"]["scenario"] == "e2_noisy_neighbour"]
    if not e2:
        sys.stderr.write("  E2 runs not found — skipping fig_e2_throughput\n")
        return
    by_mode = {r["summary"]["mode"]: r["summary"] for r in e2}
    if "kafka" not in by_mode or "direct_http" not in by_mode:
        sys.stderr.write("  E2 missing one of kafka/direct_http — skipping\n")
        return

    # target = sum of (rate × duration) across profiles
    target = max(by_mode["kafka"]["sent"], by_mode["direct_http"]["sent"])
    for s in by_mode.values():
        for p in s.get("profiles", []):
            t = int(p.get("rate_per_sec", 0) * p.get("duration_sec", 0))
            target = max(target, t)
    # if profiles not in JSON, fall back to kafka.sent (which equals target
    # when Kafka absorbs everything)
    target = target or by_mode["kafka"]["sent"]

    kafka_sent  = by_mode["kafka"]["sent"]
    direct_sent = by_mode["direct_http"]["sent"]

    fig, ax = plt.subplots(figsize=(3.2, 2.4), constrained_layout=True)
    bars = ax.bar(["Kafka", "Direct-HTTP"],
                  [kafka_sent, direct_sent],
                  color=[KAFKA_COLOR, DIRECT_COLOR], width=0.55)
    # target reference line
    ax.axhline(target, color="black", linestyle=":", linewidth=0.9,
               label=f"target ({target:,})")

    # labels: absolute count + % of target
    for b, v in zip(bars, [kafka_sent, direct_sent]):
        pct = 100 * v / target if target else 0
        ax.text(b.get_x() + b.get_width()/2, v + target * 0.03,
                f"{v:,}\n({pct:.0f}%)",
                ha="center", va="bottom", fontsize=8)

    ax.set_ylabel("Requests sustained in 60 s")
    ax.set_ylim(0, target * 1.18)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.14),
              ncol=2, frameon=False)
    ax.grid(True, axis="y", alpha=0.3)
    ax.set_axisbelow(True)
    ax.xaxis.grid(False)

    _save(fig, out_path)


# ---- Fig E1-tail — single-panel "Kafka wins" view (bar, log y) ----------
def fig_e1_tail_at_50rps(runs: list[dict[str, Any]], out_path: str) -> None:
    """One-panel bar chart at rate=50 rps showing all four percentiles.
    Log-y makes the max-latency gap (33 ms vs 1{,}087 ms = 32x) impossible
    to miss while still showing that p50/p95/p99 are comparable."""
    e1 = [r for r in runs
          if r["summary"]["scenario"] == "e1_rate50"]
    if len(e1) < 2:
        sys.stderr.write("  E1 rate=50 runs incomplete — skipping fig_e1_tail\n")
        return
    by_mode = {r["summary"]["mode"]: r["summary"] for r in e1}
    if "kafka" not in by_mode or "direct_http" not in by_mode:
        sys.stderr.write("  E1 rate=50 missing one of kafka/direct_http — skipping\n")
        return

    metrics = [("e2e_ms_p50", "p50"),
               ("e2e_ms_p95", "p95"),
               ("e2e_ms_p99", "p99"),
               ("e2e_ms_max", "max")]
    kafka_v  = [by_mode["kafka"][k]       for k, _ in metrics]
    direct_v = [by_mode["direct_http"][k] for k, _ in metrics]
    labels   = [lbl for _, lbl in metrics]

    fig, ax = plt.subplots(figsize=(3.5, 2.5), constrained_layout=True)
    x = np.arange(len(metrics))
    w = 0.38
    bars_k = ax.bar(x - w/2, kafka_v,  w, label="Kafka",       color=KAFKA_COLOR)
    bars_d = ax.bar(x + w/2, direct_v, w, label="Direct-HTTP", color=DIRECT_COLOR)

    # value labels on top of each bar
    for bars, vals in ((bars_k, kafka_v), (bars_d, direct_v)):
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width()/2, v * 1.10,
                    f"{v:.0f}", ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("Latency percentile")
    ax.set_ylabel("Latency (ms, log scale)")
    ax.set_yscale("log")
    ax.set_ylim(1, max(direct_v) * 4)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.14),
              ncol=2, frameon=False)
    ax.grid(True, axis="y", which="both", alpha=0.3)
    ax.set_axisbelow(True)
    ax.xaxis.grid(False)

    _save(fig, out_path)


# ---- entry point --------------------------------------------------------
def main() -> None:
    p = argparse.ArgumentParser(
        description="Generate paper-quality PDF figures from outputs/*.json runs.",
    )
    p.add_argument("paths", nargs="+",
                   help="Glob(s) of run JSONs (e.g. outputs/*.json).")
    p.add_argument("--output-dir", "-o", default="figures",
                   help="Output directory (default: figures/).")
    args = p.parse_args()

    files = _expand_globs(args.paths)
    if not files:
        sys.stderr.write("no matching files\n")
        sys.exit(2)

    runs = _load_runs(files)
    sys.stderr.write(f"loaded {len(runs)} runs\n")

    os.makedirs(args.output_dir, exist_ok=True)

    fig_e1_frontier     (runs, os.path.join(args.output_dir, "fig_e1_frontier.pdf"))
    fig_e1_tail_at_50rps(runs, os.path.join(args.output_dir, "fig_e1_tail.pdf"))
    fig_e2_isolation    (runs, os.path.join(args.output_dir, "fig_e2_isolation.pdf"))
    fig_e2_throughput   (runs, os.path.join(args.output_dir, "fig_e2_throughput.pdf"))
    fig_e5_resources    (runs, os.path.join(args.output_dir, "fig_e5_resources.pdf"))


if __name__ == "__main__":
    main()
