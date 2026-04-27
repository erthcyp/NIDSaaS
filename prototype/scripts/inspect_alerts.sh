#!/usr/bin/env bash
# inspect_alerts.sh
# ----------------------------------------------------------------------
# Pretty-print the alerts the webhook_receiver has captured so far.
# Shows: summary (count by decision / tier / tenant) and per-trace
# details for the most recent N alerts.
#
# Usage:
#   ./scripts/inspect_alerts.sh           # default: last 10 alerts
#   ./scripts/inspect_alerts.sh 50        # show last 50 alerts
#   N=5 WEBHOOK=http://other:9000  ./scripts/inspect_alerts.sh
# ----------------------------------------------------------------------
set -uo pipefail

WEBHOOK="${WEBHOOK:-http://localhost:9000}"
N="${1:-${N:-10}}"

if ! curl -sf -m 5 "${WEBHOOK}/traces" -o /tmp/.inspect_traces.json 2>/dev/null; then
    printf "\033[31m  ✗ webhook receiver not reachable at %s\033[0m\n" "${WEBHOOK}"
    printf "    hint: cd prototype && docker compose ps\n"
    exit 1
fi

python3 - "$N" <<'PYEOF'
import json, sys
from collections import Counter
from datetime import datetime

# ANSI helpers
RED   = "\033[31m"
GREEN = "\033[32m"
YEL   = "\033[33m"
BLU   = "\033[34m"
MAG   = "\033[35m"
CYN   = "\033[36m"
DIM   = "\033[2m"
B     = "\033[1m"
R     = "\033[0m"

def hr():
    print(DIM + "═" * 72 + R)

n_show = int(sys.argv[1])

with open("/tmp/.inspect_traces.json") as f:
    raw = json.load(f)

# Defensive unwrapping — webhook may envelope traces or include count fields
if isinstance(raw, dict) and isinstance(raw.get("traces"), dict):
    traces = raw["traces"]
elif isinstance(raw, dict):
    traces = {k: v for k, v in raw.items() if isinstance(v, list)}
elif isinstance(raw, list):
    traces = {f"_{i}": v for i, v in enumerate(raw) if isinstance(v, list)}
else:
    traces = {}

# flatten to list of (trace_id, entry, ts) — handles missing/non-dict gracefully
events = []
for tid, entries in traces.items():
    for e in entries:
        if not isinstance(e, dict):
            continue
        ts = (e.get("receive_ts") or e.get("verdict_ts")
              or e.get("ingest_ts") or 0.0)
        events.append((tid, e, ts))

# Sort newest first by best-available timestamp
events.sort(key=lambda x: x[2], reverse=True)

# --- header ----------------------------------------------------------
hr()
print(f"{B}  NIDSaaS alert inspector{R}    "
      f"{DIM}(source: webhook /traces){R}")
hr()

if not events:
    print(f"  {YEL}⚠ no alerts received yet{R}")
    print(f"  {DIM}hint: run scripts/demo_pcap.sh first, or wait for pipeline{R}")
    sys.exit(0)

# --- summary counts --------------------------------------------------
decisions = Counter()
tiers = Counter()
tenants = Counter()
ts_min, ts_max = float("inf"), 0.0
for _, e, ts in events:
    dec = e.get("decision")
    decisions[dec] += 1
    tiers[e.get("tier", "?")] += 1
    ten = e.get("tenant", "?")
    tenants[ten] += 1
    if ts:
        ts_min = min(ts_min, ts)
        ts_max = max(ts_max, ts)

print(f"  {B}Total events:{R}    {len(events)}")
print(f"  {B}Total traces:{R}    {len(traces)}")
if ts_min < float("inf"):
    span = ts_max - ts_min
    t0 = datetime.fromtimestamp(ts_min).strftime("%H:%M:%S")
    t1 = datetime.fromtimestamp(ts_max).strftime("%H:%M:%S")
    print(f"  {B}Time range:{R}      {t0} → {t1}  "
          f"{DIM}({span:.1f} s){R}")
print()

# --- decision breakdown ---------------------------------------------
print(f"  {B}Decisions{R}")
for dec, cnt in sorted(decisions.items(), key=lambda x: -x[1]):
    if dec == 1:
        label = f"{RED}🚨 malicious{R}"
    elif dec == 0:
        label = f"{GREEN}✓  benign{R}"
    else:
        label = f"{DIM}?  {dec}{R}"
    pct = 100 * cnt / len(events)
    print(f"    {label:<28} {cnt:>5}   ({pct:.0f}%)")
print()

# --- tier breakdown -------------------------------------------------
TIER_LABEL = {
    "tier0_signature": (CYN, "Snort signature fast-path"),
    "tier1_rate":      (BLU, "rate rule (volumetric)"),
    "tier2_gate":      (MAG, "ML cascade (RF + conformal + GBDT)"),
}
print(f"  {B}Tiers{R}     {DIM}(which stage caught the flow){R}")
for tier, cnt in sorted(tiers.items(), key=lambda x: -x[1]):
    color, desc = TIER_LABEL.get(tier, (DIM, ""))
    pct = 100 * cnt / len(events)
    line = f"    {color}{tier:<20}{R} {cnt:>5}  ({pct:.0f}%)"
    if desc:
        line += f"  {DIM}{desc}{R}"
    print(line)
print()

# --- tenant breakdown -----------------------------------------------
print(f"  {B}Tenants{R}")
for ten, cnt in sorted(tenants.items(), key=lambda x: -x[1]):
    pct = 100 * cnt / len(events)
    print(f"    {ten:<20} {cnt:>5}   ({pct:.0f}%)")
print()

# --- recent details --------------------------------------------------
hr()
shown = min(n_show, len(events))
print(f"{B}  Recent {shown} alert(s){R}    {DIM}(newest first){R}")
hr()
for tid, e, ts in events[:shown]:
    dec = e.get("decision")
    tier = e.get("tier", "?")
    score = e.get("score")
    color, _ = TIER_LABEL.get(tier, (DIM, ""))
    dec_label = (f"{RED}MALICIOUS{R}" if dec == 1
                 else f"{GREEN}benign{R}" if dec == 0 else f"{DIM}?{R}")
    score_s = f"{score:.3f}" if isinstance(score, (int, float)) else "?"

    print(f"  {DIM}── trace:{R} {str(tid)[:48]}")
    print(f"    {dec_label}  {color}tier={tier}{R}  score={score_s}")

    flow_id = e.get("flow_id") or e.get("flow")
    if flow_id:
        print(f"    flow={flow_id}")
    ten = e.get("tenant")
    if ten:
        print(f"    tenant={ten}")

    # latency: prefer pre-computed e2e_ms, else derive
    e2e_ms = e.get("e2e_ms")
    if e2e_ms is None:
        ing = e.get("ingest_ts")
        rcv = e.get("receive_ts")
        if ing and rcv:
            e2e_ms = (rcv - ing) * 1000
    if isinstance(e2e_ms, (int, float)):
        print(f"    latency={e2e_ms:.1f} ms")

    if ts:
        ts_s = datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]
        print(f"    received_at={ts_s}")

    # surface evidence/extras opaquely
    extras = {k: v for k, v in e.items() if k not in {
        "decision", "tier", "score", "flow_id", "flow", "tenant",
        "trace_id", "ingest_ts", "verdict_ts", "receive_ts",
        "e2e_ms", "chunk_id", "receive_monotonic"}}
    if extras:
        for k, v in list(extras.items())[:5]:
            v_s = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            if len(v_s) > 60:
                v_s = v_s[:57] + "..."
            print(f"    {DIM}{k}={v_s}{R}")
    print()

if shown < len(events):
    print(f"  {DIM}... {len(events) - shown} older event(s) hidden. "
          f"Re-run with arg N to see more.{R}")
PYEOF
