"""Pretty-print loadtest JSON summary as percentage table.

Usage:
    python3 show_summary.py outputs/*.json
    python3 show_summary.py outputs/e2_*.json
"""
import json, sys, glob
from pathlib import Path

paths = []
for arg in sys.argv[1:]:
    paths.extend(sorted(glob.glob(arg)))

for path in paths:
    with open(path) as f:
        d = json.load(f)
    s = d['summary']
    name = Path(path).name
    print(f'=== {name} ===')
    print(f"scenario={s.get('scenario')}, mode={s['mode']}")
    sent = s['sent']
    delivered = s['delivered_traces']
    pct = (delivered / sent * 100) if sent else 0
    print(f"sent={sent:,}, delivered={delivered:,} ({pct:.2f}%)")
    print(f"e2e p50={s['e2e_ms_p50']:.0f}ms, p95={s['e2e_ms_p95']:.0f}ms, "
          f"p99={s['e2e_ms_p99']:.0f}ms, max={s['e2e_ms_max']:.0f}ms")
    pt = s.get('per_tenant', {})
    if pt:
        print()
        print(f'  {"tenant":<10} {"sent":>6} {"deliv":>6} {"rate":>8} '
              f'{"p50":>7} {"p95":>7} {"p99":>7} {"max":>7}')
        print('  ' + '-' * 60)
        for t, ts in pt.items():
            lat = ts.get('latency_ms', {})
            print(f'  {t:<10} {ts["sent"]:>6} {ts["delivered"]:>6} '
                  f'{ts["delivery_rate"]*100:>7.2f}% '
                  f'{lat.get("p50",0):>7.0f} {lat.get("p95",0):>7.0f} '
                  f'{lat.get("p99",0):>7.0f} {lat.get("max",0):>7.0f}')
    print()
