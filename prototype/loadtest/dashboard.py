"""Build a self-contained static HTML dashboard from outputs/*.json runs.

Usage::

    python3 dashboard.py outputs/*.json
    python3 dashboard.py outputs/*.json --output dashboard.html
    python3 dashboard.py outputs/*.json -o dashboard.html && open dashboard.html

The output is a single HTML file (Chart.js loaded from CDN) that renders:

  * a sortable summary table — every run, one row, click headers to sort.
  * E1 throughput sweep — line charts of p50 / p95 / p99 / max vs target rate,
    one line per mode (kafka, direct_http).
  * E2 noisy-neighbour — grouped bar of delivery rate per tenant per mode,
    making the "quiet tenant collateral damage" claim immediately visual.
  * E5 resource footprint — grouped bar of mean CPU% and mean RSS (MiB)
    per container per mode, sourced from the ``resource_samples`` block.

Open the resulting file in any browser. No server, no Python at view time.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import glob
import json
import os
import re
import sys
from typing import Any


def _load(path: str) -> dict[str, Any]:
    with open(path) as f:
        d = json.load(f)
    d["_path"] = os.path.basename(path)
    return d


def _scenario_sort_key(scenario: str) -> tuple:
    """Stable order for scenarios: e1_rate5 < e1_rate20 < ... < e2_* < e5_*."""
    m = re.match(r"(e\d+)(?:_rate(\d+))?(?:_(.+))?", scenario)
    if not m:
        return (scenario, 0, "")
    family = m.group(1)
    rate = int(m.group(2)) if m.group(2) else 0
    rest = m.group(3) or ""
    return (family, rate, rest)


def build_dashboard(runs: list[dict[str, Any]]) -> str:
    """Return a self-contained HTML string."""
    runs = sorted(
        runs,
        key=lambda r: (
            _scenario_sort_key(r["summary"]["scenario"]),
            r["summary"]["mode"],
        ),
    )

    js_data: list[dict[str, Any]] = []
    for r in runs:
        s = r["summary"]
        js_data.append({
            "scenario":   s["scenario"],
            "mode":       s["mode"],
            "sent":       s.get("sent", 0),
            "accepted":   s.get("accepted", 0),
            "deduped":    s.get("deduped", 0),
            "delivered":  s.get("delivered_traces", 0),
            "p50":        s.get("e2e_ms_p50"),
            "p95":        s.get("e2e_ms_p95"),
            "p99":        s.get("e2e_ms_p99"),
            "max":        s.get("e2e_ms_max"),
            "per_tenant": s.get("per_tenant", {}),
            "resource":   r.get("resource_samples", {}),
            "started_at": s.get("started_at"),
            "notes":      s.get("notes", ""),
            "file":       r.get("_path", ""),
        })

    n_total  = len(js_data)
    n_kafka  = sum(1 for r in js_data if r["mode"] == "kafka")
    n_direct = sum(1 for r in js_data if r["mode"] == "direct_http")

    generated = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    js_payload = json.dumps(js_data, separators=(",", ":"), default=str)

    return (
        _HTML_TEMPLATE
        .replace("<<GENERATED>>", generated)
        .replace("<<N_TOTAL>>",   str(n_total))
        .replace("<<N_KAFKA>>",   str(n_kafka))
        .replace("<<N_DIRECT>>",  str(n_direct))
        .replace("<<JS_PAYLOAD>>", js_payload)
    )


_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>NIDSaaS Load Test Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0"></script>
<style>
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #f6f7f9; color: #222; margin: 0;
    padding: 24px 36px; max-width: 1400px;
    margin-left: auto; margin-right: auto;
  }
  h1 { margin: 0 0 6px 0; font-size: 26px; }
  h2 { margin: 36px 0 12px 0; font-size: 19px;
       border-bottom: 2px solid #e0e3e8; padding-bottom: 6px; }
  .meta { color: #5a6172; font-size: 13px; margin-bottom: 24px; }
  .meta strong { color: #222; }
  table { width: 100%; border-collapse: collapse; background: white;
          border-radius: 6px; overflow: hidden;
          box-shadow: 0 1px 3px rgba(0,0,0,0.06); font-size: 13px; }
  th { background: #2c3038; color: white; text-align: left;
       padding: 10px 12px; cursor: pointer; user-select: none;
       font-weight: 600; white-space: nowrap; }
  th:hover { background: #3a3f48; }
  th.sorted-asc::after  { content: " \25B2"; opacity: 0.7; }
  th.sorted-desc::after { content: " \25BC"; opacity: 0.7; }
  td { padding: 8px 12px; border-bottom: 1px solid #eef0f3; }
  tr:nth-child(even) td { background: #fafbfc; }
  tr:hover td { background: #eaf3ff; }
  .mode-kafka  { color: #1976d2; font-weight: 600; }
  .mode-direct { color: #c62828; font-weight: 600; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .pill { padding: 2px 8px; border-radius: 999px; font-size: 11px;
          background: #e8eef7; color: #1976d2; }
  .pill.bad { background: #fde7e7; color: #c62828; }
  .chart-grid { display: grid; grid-template-columns: 1fr 1fr;
                gap: 16px; margin-top: 12px; }
  .chart-card { background: white; padding: 18px; border-radius: 6px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.06); height: 320px;
                position: relative; }
  .chart-card.full { grid-column: 1 / -1; height: 360px; }
  .chart-card h3 { margin: 0 0 10px 0; font-size: 14px;
                   color: #5a6172; font-weight: 600; }
  .empty { color: #999; font-style: italic; padding: 20px;
           text-align: center; }
  @media (max-width: 900px) { .chart-grid { grid-template-columns: 1fr; } }
</style>
</head>
<body>

<h1>NIDSaaS Load Test Dashboard</h1>
<p class="meta">
  Generated <<GENERATED>> &middot;
  <strong><<N_TOTAL>></strong> runs total &middot;
  <span class="mode-kafka"><<N_KAFKA>> kafka</span> &middot;
  <span class="mode-direct"><<N_DIRECT>> direct_http</span>
</p>

<h2>Summary table</h2>
<table id="summary-table">
  <thead>
    <tr>
      <th data-key="scenario">Scenario</th>
      <th data-key="mode">Mode</th>
      <th data-key="sent" class="num">Sent</th>
      <th data-key="accepted" class="num">Accepted</th>
      <th data-key="delivered" class="num">Delivered</th>
      <th data-key="delivery_rate" class="num">Delivery %</th>
      <th data-key="p50" class="num">p50 (ms)</th>
      <th data-key="p95" class="num">p95 (ms)</th>
      <th data-key="p99" class="num">p99 (ms)</th>
      <th data-key="max" class="num">max (ms)</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

<h2>E1 &mdash; throughput sweep (single tenant)</h2>
<div class="chart-grid">
  <div class="chart-card"><h3>p50 latency vs target rate</h3><canvas id="chart-e1-p50"></canvas></div>
  <div class="chart-card"><h3>p95 latency vs target rate</h3><canvas id="chart-e1-p95"></canvas></div>
  <div class="chart-card"><h3>p99 latency vs target rate</h3><canvas id="chart-e1-p99"></canvas></div>
  <div class="chart-card"><h3>max latency vs target rate</h3><canvas id="chart-e1-max"></canvas></div>
</div>

<h2>E2 &mdash; noisy-neighbour delivery rate by tenant</h2>
<div class="chart-grid">
  <div class="chart-card full">
    <h3>Delivery rate per tenant (kafka vs direct_http)</h3>
    <canvas id="chart-e2-delivery"></canvas>
  </div>
</div>

<h2>E5 &mdash; resource footprint</h2>
<div class="chart-grid">
  <div class="chart-card"><h3>Mean CPU % per container</h3><canvas id="chart-e5-cpu"></canvas></div>
  <div class="chart-card"><h3>Mean RSS (MiB) per container</h3><canvas id="chart-e5-rss"></canvas></div>
</div>

<script>
const RUNS = <<JS_PAYLOAD>>;

const KAFKA_COLOR  = '#1976d2';
const DIRECT_COLOR = '#c62828';

function deliveryRate(r) {
  return r.sent > 0 ? (r.delivered / r.sent * 100) : 0;
}

function fmtNum(x, digits) {
  if (x === null || x === undefined || Number.isNaN(x)) return '-';
  return Number(x).toFixed(digits ?? 1);
}

// --- summary table ---
function renderRow(r) {
  const dRate = deliveryRate(r);
  const modeCls = r.mode === 'kafka' ? 'mode-kafka' : 'mode-direct';
  const dPillCls = dRate >= 99 ? 'pill' : 'pill bad';
  return `<tr>
    <td>${r.scenario}</td>
    <td class="${modeCls}">${r.mode}</td>
    <td class="num">${r.sent}</td>
    <td class="num">${r.accepted}</td>
    <td class="num">${r.delivered}</td>
    <td class="num"><span class="${dPillCls}">${fmtNum(dRate, 1)}%</span></td>
    <td class="num">${fmtNum(r.p50)}</td>
    <td class="num">${fmtNum(r.p95)}</td>
    <td class="num">${fmtNum(r.p99)}</td>
    <td class="num">${fmtNum(r.max)}</td>
  </tr>`;
}

const tbody = document.querySelector('#summary-table tbody');
let sortKey = null, sortDir = 1;

function rerender(rows) {
  tbody.innerHTML = rows.map(renderRow).join('');
}

function sortBy(key) {
  if (sortKey === key) sortDir = -sortDir;
  else { sortKey = key; sortDir = 1; }
  document.querySelectorAll('#summary-table th').forEach(th => {
    th.classList.remove('sorted-asc', 'sorted-desc');
    if (th.dataset.key === key)
      th.classList.add(sortDir > 0 ? 'sorted-asc' : 'sorted-desc');
  });
  const sorted = RUNS.slice().sort((a, b) => {
    let va, vb;
    if (key === 'delivery_rate') {
      va = deliveryRate(a); vb = deliveryRate(b);
    } else {
      va = a[key]; vb = b[key];
    }
    if (typeof va === 'string') return va.localeCompare(vb) * sortDir;
    return ((va ?? 0) - (vb ?? 0)) * sortDir;
  });
  rerender(sorted);
}

document.querySelectorAll('#summary-table th').forEach(th => {
  th.addEventListener('click', () => sortBy(th.dataset.key));
});
rerender(RUNS);

// --- E1 line charts ---
function rateFromScenario(s) {
  const m = /e1_rate(\d+)/.exec(s);
  return m ? parseInt(m[1], 10) : null;
}

const e1 = RUNS.filter(r => /^e1_rate\d+$/.test(r.scenario));
const e1Rates = [...new Set(e1.map(r => rateFromScenario(r.scenario)))].sort((a, b) => a - b);

function e1Chart(canvasId, metric) {
  const dataFor = (mode) => e1Rates.map(rate => {
    const r = e1.find(x => x.mode === mode && rateFromScenario(x.scenario) === rate);
    return r ? r[metric] : null;
  });
  new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: {
      labels: e1Rates.map(r => r + ' rps'),
      datasets: [
        { label: 'kafka',
          data: dataFor('kafka'),
          borderColor: KAFKA_COLOR,
          backgroundColor: KAFKA_COLOR + '22',
          tension: 0.2 },
        { label: 'direct_http',
          data: dataFor('direct_http'),
          borderColor: DIRECT_COLOR,
          backgroundColor: DIRECT_COLOR + '22',
          tension: 0.2 },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        y: { title: { display: true, text: 'ms' }, beginAtZero: true }
      }
    }
  });
}

if (e1Rates.length > 0) {
  e1Chart('chart-e1-p50', 'p50');
  e1Chart('chart-e1-p95', 'p95');
  e1Chart('chart-e1-p99', 'p99');
  e1Chart('chart-e1-max', 'max');
} else {
  ['chart-e1-p50', 'chart-e1-p95', 'chart-e1-p99', 'chart-e1-max'].forEach(id => {
    const c = document.getElementById(id);
    c.parentElement.innerHTML = '<div class="empty">No E1 runs found.</div>';
  });
}

// --- E2 per-tenant delivery rate ---
const e2 = RUNS.filter(r => r.scenario === 'e2_noisy_neighbour');
if (e2.length > 0) {
  const tenants = [...new Set(
    e2.flatMap(r => Object.keys(r.per_tenant || {}))
  )].sort();
  const dataMode = (mode) => tenants.map(t => {
    const r = e2.find(x => x.mode === mode);
    if (!r || !r.per_tenant[t]) return null;
    return (r.per_tenant[t].delivery_rate || 0) * 100;
  });
  new Chart(document.getElementById('chart-e2-delivery'), {
    type: 'bar',
    data: {
      labels: tenants,
      datasets: [
        { label: 'kafka',
          data: dataMode('kafka'),
          backgroundColor: KAFKA_COLOR },
        { label: 'direct_http',
          data: dataMode('direct_http'),
          backgroundColor: DIRECT_COLOR },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        y: { title: { display: true, text: 'delivery rate (%)' },
             beginAtZero: true, max: 100 }
      }
    }
  });
} else {
  document.getElementById('chart-e2-delivery').parentElement.innerHTML =
    '<div class="empty">No E2 runs found.</div>';
}

// --- E5 resource ---
const e5 = RUNS.filter(r =>
  r.scenario === 'e5_resource_footprint' &&
  r.resource && Object.keys(r.resource).length > 0
);

if (e5.length > 0) {
  const containers = [...new Set(
    e5.flatMap(r => Object.keys(r.resource))
  )].sort();
  const cpu = (mode) => containers.map(c => {
    const r = e5.find(x => x.mode === mode);
    return r && r.resource[c] ? r.resource[c].cpu_mean : null;
  });
  const rss = (mode) => containers.map(c => {
    const r = e5.find(x => x.mode === mode);
    return r && r.resource[c] ? r.resource[c].rss_mean_mib : null;
  });

  new Chart(document.getElementById('chart-e5-cpu'), {
    type: 'bar',
    data: {
      labels: containers,
      datasets: [
        { label: 'kafka',
          data: cpu('kafka'),
          backgroundColor: KAFKA_COLOR },
        { label: 'direct_http',
          data: cpu('direct_http'),
          backgroundColor: DIRECT_COLOR },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: { y: { title: { display: true, text: 'CPU mean (%)' },
                     beginAtZero: true } }
    }
  });

  new Chart(document.getElementById('chart-e5-rss'), {
    type: 'bar',
    data: {
      labels: containers,
      datasets: [
        { label: 'kafka',
          data: rss('kafka'),
          backgroundColor: KAFKA_COLOR },
        { label: 'direct_http',
          data: rss('direct_http'),
          backgroundColor: DIRECT_COLOR },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: { y: { title: { display: true, text: 'RSS mean (MiB)' },
                     beginAtZero: true } }
    }
  });
} else {
  document.getElementById('chart-e5-cpu').parentElement.innerHTML =
    '<div class="empty">No E5 runs found (E5 requires <code>--mode e5</code> with docker stats).</div>';
  document.getElementById('chart-e5-rss').parentElement.innerHTML = '';
}
</script>

</body>
</html>
"""


def main() -> None:
    p = argparse.ArgumentParser(
        description="Build a self-contained HTML dashboard from outputs/*.json runs.",
    )
    p.add_argument("paths", nargs="+",
                   help="Glob(s) of run JSONs to include, e.g. outputs/*.json")
    p.add_argument("--output", "-o", default=None,
                   help="Write to file instead of stdout (e.g. dashboard.html).")
    args = p.parse_args()

    expanded: list[str] = []
    for path in args.paths:
        if any(ch in path for ch in "*?[]"):
            expanded.extend(sorted(glob.glob(path)))
        else:
            expanded.append(path)

    runs = [_load(p) for p in expanded if os.path.isfile(p)]
    if not runs:
        sys.stderr.write("no matching files\n")
        sys.exit(2)

    html = build_dashboard(runs)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(html)
        sys.stderr.write(f"wrote {len(runs)} runs to {args.output}\n")
    else:
        sys.stdout.write(html)


if __name__ == "__main__":
    main()
