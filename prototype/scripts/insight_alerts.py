"""Alert insight tool — answers 'what kinds of attacks did the system flag?'

Pulls the last N alerts from the webhook receiver's per-tenant ring
buffer and produces a multi-section report:

  1. Headline counts             (total alerts, decision / tier breakdown)
  2. Detection tier breakdown    (signature vs rate vs ML)
  3. Rate-rule firings           (V / L / S / R / P / B)
  4. Top destination ports       (what is being attacked)
  5. Top source IPs              (who is attacking)
  6. Score distribution          (how confident the ML scorer is)
  7. PCAP capture-time histogram (when did the attacks actually happen)
  8. Sample full payloads        (one representative alert per tier)

Usage::

    python3 scripts/insight_alerts.py                        # tenant=somchart, last 200
    python3 scripts/insight_alerts.py --tenant acme --limit 100
    python3 scripts/insight_alerts.py --raw                  # also dump raw JSON
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics
import sys
import urllib.request
from datetime import datetime


# --- ANSI helpers ---
B = "\033[1m"; D = "\033[2m"; R = "\033[0m"
GR = "\033[32m"; YL = "\033[33m"; CY = "\033[36m"; RD = "\033[31m"; MG = "\033[35m"

def hr(): print(D + "─" * 70 + R)
def title(s: str): print(); hr(); print(B + "  " + s + R); hr()
def kv(k, v, color=""):
    print(f"  {k:<28s} {color}{v}{R}")
def bar(label: str, n: int, total: int, width: int = 40, color: str = CY):
    """Render an ASCII bar chart row."""
    if total <= 0:
        return
    pct = n / total
    fill = int(round(pct * width))
    bar = "█" * fill + "░" * (width - fill)
    print(f"  {label:<28s} {color}{bar}{R} {n:>5d}  ({pct*100:5.1f}%)")


def fetch_alerts(webhook: str, tenant: str, limit: int) -> list[dict]:
    url = f"{webhook}/alerts/{tenant}?limit={limit}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
    except Exception as e:
        sys.exit(f"failed to fetch {url}: {e}")
    return data.get("items", [])


def parse_flow_id(fid: str) -> tuple[str, str, str, str, str]:
    """CICFlowMeter Flow ID = src-dst-sport-dport-proto.
    Returns (src, dst, sport, dport, proto) or empty strings if not parseable."""
    if not fid:
        return ("", "", "", "", "")
    parts = fid.split("-")
    if len(parts) >= 5 and "." in parts[0]:
        return (parts[0], parts[1], parts[2], parts[3], parts[4])
    return ("", "", "", "", "")


def proto_name(p: str) -> str:
    return {"6": "TCP", "17": "UDP", "1": "ICMP"}.get(p, f"proto-{p}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--webhook", default="http://localhost:9000")
    ap.add_argument("--tenant", default="somchart")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--raw", action="store_true",
                    help="Also dump the raw JSON of the most recent alert.")
    args = ap.parse_args()

    items = fetch_alerts(args.webhook, args.tenant, args.limit)
    if not items:
        print(f"  no alerts found for tenant '{args.tenant}' "
              f"(limit={args.limit}, webhook={args.webhook})")
        sys.exit(0)

    n_total = len(items)

    # =================================================================
    # 1. Headline
    # =================================================================
    title(f"Alert Insight  —  tenant='{args.tenant}', last {n_total} alerts")
    decisions = collections.Counter(it.get("decision") for it in items)
    kv("total alerts", n_total, B)
    kv("decision=1 (attack)", decisions.get(1, 0), RD)
    kv("decision=0 (benign)", decisions.get(0, 0), GR)

    # =================================================================
    # 2. Detection tier
    # =================================================================
    title("Detection tier  —  how the alerts were caught")
    tiers = collections.Counter(it.get("tier", "?") for it in items)
    for tier, n in tiers.most_common():
        col = MG if "signature" in tier else (YL if "rate" in tier else CY)
        bar(tier, n, n_total, color=col)

    # =================================================================
    # 3. Rate-rule firings
    # =================================================================
    title("Rate-rule firings  (V/L/S/R/P/B)")
    rate_names = {
        "V": "V — Volumetric DDoS",
        "L": "L — Slow HTTP",
        "S": "S — SYN-flood",
        "R": "R — RST anomaly",
        "P": "P — Port scan",
        "B": "B — Brute force (ports 21/22/23)",
    }
    rate_hits = collections.Counter()
    for it in items:
        rs = it.get("rate_signals") or {}
        for k, v in rs.items():
            if v:
                rate_hits[k] += int(v)
    if not rate_hits:
        print(f"  {D}(no rate rules fired in this window){R}")
    else:
        max_hits = max(rate_hits.values())
        for k in "VLSRPB":
            if k in rate_hits:
                bar(rate_names[k], rate_hits[k], max_hits, color=YL)

    # =================================================================
    # 4. Top destination ports — what's being attacked
    # =================================================================
    title("Top destination ports  —  what is being attacked")
    dports = collections.Counter()
    for it in items:
        _, _, _, dport, _ = parse_flow_id(it.get("flow_id", ""))
        if dport:
            dports[dport] += 1
    if not dports:
        print(f"  {D}(no IP-format flow_ids — flows may be synthetic){R}")
    else:
        port_meaning = {
            "80": "HTTP", "443": "HTTPS", "53": "DNS", "22": "SSH",
            "21": "FTP", "23": "Telnet", "445": "SMB", "139": "NetBIOS",
            "138": "NetBIOS-DGM", "137": "NetBIOS-NS", "3389": "RDP",
            "3268": "AD Global Catalog", "389": "LDAP",
        }
        max_n = max(dports.values())
        for port, n in dports.most_common(10):
            label = f"{port:>5s}  ({port_meaning.get(port, '?')})"
            bar(label, n, max_n, color=CY)

    # =================================================================
    # 5. Top source IPs — who's attacking
    # =================================================================
    title("Top source IPs  —  who is attacking")
    sources = collections.Counter()
    for it in items:
        src, _, _, _, _ = parse_flow_id(it.get("flow_id", ""))
        if src:
            sources[src] += 1
    if not sources:
        print(f"  {D}(no IP-format source identifiable){R}")
    else:
        max_n = max(sources.values())
        for ip, n in sources.most_common(10):
            bar(ip, n, max_n, color=RD)

    # =================================================================
    # 6. Score distribution
    # =================================================================
    title("ML score distribution  (Tier-2 alerts only)")
    t2 = [it for it in items if "tier2" in (it.get("tier") or "")]
    if not t2:
        print(f"  {D}(no tier2_gate alerts in this window){R}")
    else:
        scores = [float(it.get("score", 0)) for it in t2]
        kv("count",    len(scores))
        kv("min",      f"{min(scores):.4f}")
        kv("max",      f"{max(scores):.4f}")
        kv("mean",     f"{statistics.fmean(scores):.4f}")
        if len(scores) >= 2:
            kv("median", f"{statistics.median(scores):.4f}")
            kv("stdev",  f"{statistics.stdev(scores):.4f}")
        # tau* threshold
        taus = [it.get("tau_star") for it in t2 if it.get("tau_star") is not None]
        if taus:
            kv("tau* (calibrated)", f"{taus[0]:.4f}")
            above = sum(1 for s in scores if s >= taus[0])
            kv(f"  → above tau*", f"{above}/{len(scores)} ({above/len(scores)*100:.1f}%)")
        # crude bucket histogram
        buckets = [0] * 10
        for s in scores:
            buckets[min(9, int(s * 10))] += 1
        max_b = max(buckets) if any(buckets) else 1
        print()
        for i, n in enumerate(buckets):
            label = f"  [{i/10:.1f} – {(i+1)/10:.1f})"
            bar(label, n, max_b, color=MG)

    # =================================================================
    # 7. Capture-time histogram (PCAP capture time)
    # =================================================================
    title("PCAP capture-time histogram  (when did the traffic happen?)")
    captures = []
    for it in items:
        ts = it.get("flow_capture_ts")
        if ts:
            captures.append(ts)
    if not captures:
        print(f"  {D}(no PCAP capture timestamps — flows may be synthetic){R}")
    else:
        # Count by hour
        hour_buckets = collections.Counter()
        for ts in captures:
            # Try "DD/MM/YYYY HH:MM:SS"
            try:
                # CICFlowMeter v4 format
                d, t = ts.split(" ", 1)
                hour = t.split(":")[0]
                day = d
                hour_buckets[f"{day} {hour}:00"] += 1
            except Exception:
                continue
        if hour_buckets:
            max_n = max(hour_buckets.values())
            for hour_key, n in sorted(hour_buckets.items()):
                bar(hour_key, n, max_n, color=GR)

    # =================================================================
    # 8. Sample full payloads
    # =================================================================
    title("Sample alert  —  one per tier (full payload)")
    seen_tiers = set()
    for it in items:
        tier = it.get("tier", "?")
        if tier in seen_tiers:
            continue
        seen_tiers.add(tier)
        print()
        print(f"  {B}tier = {tier}{R}")
        # Render compact JSON
        compact = {k: v for k, v in it.items()
                   if v is not None and k not in ("rate_signals",)}
        print(json.dumps(compact, indent=4, default=str)[:1500])
        # Then the rate signals if present
        rs = it.get("rate_signals")
        if rs:
            fired = {k: v for k, v in rs.items() if v}
            if fired:
                print(f"    rate signals fired: {fired}")

    # =================================================================
    # 9. Raw dump (optional)
    # =================================================================
    if args.raw:
        title("Raw alert (most recent, full JSON)")
        print(json.dumps(items[0], indent=2, default=str))

    print()


if __name__ == "__main__":
    main()
