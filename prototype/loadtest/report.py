"""Pair / compare runs from outputs/.

Usage::

    python report.py outputs/e1_kafka_*.json outputs/e1_direct_http_*.json
    python report.py --csv outputs/*.json    # one row per run

The default mode prints a side-by-side table for each scenario that has
both transports represented. ``--csv`` dumps every run as a single row
so you can paste into a spreadsheet.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys
from typing import Any


def _load(path: str) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def _fmt(x: float | None, suffix: str = "") -> str:
    return f"{x:.1f}{suffix}" if isinstance(x, (int, float)) else "-"


def _delta(a: float | None, b: float | None) -> str:
    """b vs a: how much worse is b? Positive means b is slower."""
    if a is None or b is None or a == 0:
        return "-"
    pct = (b - a) / a * 100.0
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"


def render_table(runs_by_key: dict[tuple[str, str], dict[str, Any]]) -> str:
    out_lines: list[str] = []
    scenarios = sorted({k[0] for k in runs_by_key.keys()})
    for sc in scenarios:
        out_lines.append("")
        out_lines.append(f"== {sc} ==")
        kafka = runs_by_key.get((sc, "kafka"))
        direct = runs_by_key.get((sc, "direct_http"))
        if not kafka or not direct:
            present = "kafka" if kafka else "direct_http"
            other = "direct_http" if kafka else "kafka"
            out_lines.append(
                f"  only {present} run found — add a {other} run to compare."
            )
            continue
        s_k = kafka["summary"]
        s_d = direct["summary"]
        out_lines.append(
            f"  {'metric':<24} {'kafka':>14} {'direct_http':>14} {'Δ direct vs kafka':>22}"
        )
        out_lines.append("  " + "-" * 76)
        for key, label, fmt in [
            ("sent",            "sent",            ""),
            ("accepted",        "accepted",        ""),
            ("delivered_traces","delivered",       ""),
            ("e2e_ms_p50",      "p50 e2e (ms)",    "ms"),
            ("e2e_ms_p95",      "p95 e2e (ms)",    "ms"),
            ("e2e_ms_p99",      "p99 e2e (ms)",    "ms"),
            ("e2e_ms_max",      "max e2e (ms)",    "ms"),
        ]:
            kv = s_k.get(key)
            dv = s_d.get(key)
            out_lines.append(
                f"  {label:<24} {_fmt(kv):>14} {_fmt(dv):>14} {_delta(kv, dv):>22}"
            )
        # Per-tenant block
        tenants = sorted(set(s_k.get("per_tenant", {})) | set(s_d.get("per_tenant", {})))
        for t in tenants:
            kt = s_k.get("per_tenant", {}).get(t, {})
            dt = s_d.get("per_tenant", {}).get(t, {})
            out_lines.append(f"  -- tenant: {t} --")
            for key, label in [("p50", "p50"), ("p95", "p95"), ("p99", "p99")]:
                kv = (kt.get("latency_ms", {}) or {}).get(key)
                dv = (dt.get("latency_ms", {}) or {}).get(key)
                out_lines.append(
                    f"  {label:<24} {_fmt(kv):>14} {_fmt(dv):>14} {_delta(kv, dv):>22}"
                )
            kr = kt.get("delivery_rate")
            dr = dt.get("delivery_rate")
            kr_pct = kr * 100 if isinstance(kr, (int, float)) else None
            dr_pct = dr * 100 if isinstance(dr, (int, float)) else None
            out_lines.append(
                f"  {'delivery_rate':<24} {_fmt(kr_pct, '%'):>14} {_fmt(dr_pct, '%'):>14}"
            )
        # Resource block (only present for E5)
        if "resource_samples" in kafka and "resource_samples" in direct:
            out_lines.append("  -- resource (cpu mean / rss mean) --")
            names = sorted(set(kafka["resource_samples"]) | set(direct["resource_samples"]))
            for n in names:
                k = kafka["resource_samples"].get(n, {})
                d = direct["resource_samples"].get(n, {})
                k_cpu = k.get("cpu_mean")
                k_rss = k.get("rss_mean_mib")
                d_cpu = d.get("cpu_mean")
                d_rss = d.get("rss_mean_mib")
                k_label = (f"cpu={k_cpu:.1f}% rss={k_rss:.0f}MiB"
                           if k_cpu is not None and k_rss is not None else "-")
                d_label = (f"cpu={d_cpu:.1f}% rss={d_rss:.0f}MiB"
                           if d_cpu is not None and d_rss is not None else "-")
                out_lines.append(f"  {n:<24} {k_label:>30} {d_label:>30}")
    return "\n".join(out_lines)


def render_csv(runs: list[dict[str, Any]]) -> str:
    fields = ["scenario", "mode", "started_at", "duration_sec", "sent",
              "accepted", "deduped", "failed", "delivered_traces",
              "e2e_ms_p50", "e2e_ms_p95", "e2e_ms_p99", "e2e_ms_max", "notes"]
    out: list[str] = []
    out.append(",".join(fields))
    for r in runs:
        s = r["summary"]
        row = [str(s.get(f, "") or "") for f in fields]
        out.append(",".join(row).replace("\n", " "))
    return "\n".join(out)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("paths", nargs="+", help="Glob(s) of run JSONs to compare.")
    p.add_argument("--csv", action="store_true",
                   help="Emit one CSV row per run instead of a paired table.")
    args = p.parse_args()

    expanded: list[str] = []
    for path in args.paths:
        if any(ch in path for ch in "*?[]"):
            expanded.extend(sorted(glob.glob(path)))
        else:
            expanded.append(path)
    if not expanded:
        sys.stderr.write("no matching files\n")
        sys.exit(2)

    runs = [_load(p) for p in expanded if os.path.isfile(p)]
    if args.csv:
        print(render_csv(runs))
        return
    keyed = {(r["summary"]["scenario"], r["summary"]["mode"]): r for r in runs}
    print(render_table(keyed))


if __name__ == "__main__":
    main()
