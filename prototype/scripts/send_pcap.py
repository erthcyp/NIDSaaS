"""Stream a PCAP file through the running NIDSaaS gateway.

Authenticates as the given tenant, slices the PCAP into chunks, and
POSTs each chunk to the gateway's /ingest_pcap endpoint. The gateway
publishes onto tenant.{u}.pcap_chunks; the flow_extractor (CICFlowMeter)
and snort_sidecar consumer groups process each chunk in parallel and
emit derived flow + signature events that the detector consumes.

Open the dashboard at http://localhost:9000 *before* running this
script — alerts will start appearing in the table within a few seconds.

Typical usage::

    # Stream the first 5,000 packets of Friday-WorkingHours.pcap as
    # tenant 'somchart' (auto-slices via tshark/tcpdump if available)
    python3 scripts/send_pcap.py \
        --pcap ~/pcap_CIC_IDS2017/Friday-WorkingHours.pcap \
        --tenant somchart --packets 5000

    # Stream a pre-sliced PCAP as-is
    python3 scripts/send_pcap.py \
        --pcap /tmp/already_sliced.pcap --tenant somchart
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import httpx


def slice_pcap(src: Path, n_packets: int, dst: Path) -> None:
    """Slice the first n_packets of `src` into `dst`. Requires tshark
    or tcpdump on PATH. We do this on the host (not inside Docker) so
    the user can re-use the slice across runs."""
    if shutil.which("tshark"):
        cmd = ["tshark", "-r", str(src), "-c", str(n_packets), "-w", str(dst)]
    elif shutil.which("tcpdump"):
        cmd = ["tcpdump", "-r", str(src), "-c", str(n_packets), "-w", str(dst)]
    else:
        sys.exit("error: install tshark (apt install tshark) or tcpdump")
    print(f"  slicing first {n_packets} packets …")
    subprocess.check_call(cmd, stderr=subprocess.DEVNULL)


def authenticate(client: httpx.Client, gateway: str,
                 tenant: str, secret: str) -> str:
    r = client.post(f"{gateway}/oauth/token",
                    data={"grant_type": "client_credentials",
                          "client_id": tenant,
                          "client_secret": secret})
    if r.status_code != 200:
        sys.exit(f"  auth failed: HTTP {r.status_code} {r.text}")
    token = r.json()["access_token"]
    return token


def reset_webhook(client: httpx.Client, webhook: str) -> None:
    try:
        client.post(f"{webhook}/traces/reset", timeout=5)
    except Exception:
        pass     # webhook is optional — script still useful without it


def stream_chunks(path: Path, chunk_bytes: int):
    """Yield raw byte chunks from `path` so we can POST them
    in roughly-real-time without holding the whole file in memory."""
    with open(path, "rb") as f:
        while True:
            buf = f.read(chunk_bytes)
            if not buf:
                return
            yield buf


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pcap", required=True,
                   help="Source PCAP path (e.g. Friday-WorkingHours.pcap).")
    p.add_argument("--tenant", default="somchart",
                   help="Tenant client_id registered in gateway .env")
    p.add_argument("--secret", default=None,
                   help="Tenant client_secret (default: <tenant>-secret)")
    p.add_argument("--gateway", default="http://localhost:8080")
    p.add_argument("--webhook", default="http://localhost:9000")
    p.add_argument("--packets", type=int, default=2000,
                   help="If >0, slice the source PCAP to first N packets "
                        "before sending. Set to 0 to send the whole file.")
    p.add_argument("--chunk-mb", type=float, default=1.0,
                   help="Per-request chunk size in MB (default: 1.0)")
    p.add_argument("--reset-webhook", action="store_true",
                   help="POST /traces/reset on the webhook before sending.")
    args = p.parse_args()

    src = Path(args.pcap).expanduser()
    if not src.is_file():
        sys.exit(f"  error: PCAP not found at {src}")

    secret = args.secret or f"{args.tenant}-secret"
    chunk_bytes = int(args.chunk_mb * 1024 * 1024)

    # Slice if --packets > 0; otherwise stream the whole file.
    if args.packets > 0:
        sliced = Path(f"/tmp/{src.stem}_first{args.packets}.pcap")
        if not sliced.exists() or sliced.stat().st_size < 100:
            slice_pcap(src, args.packets, sliced)
        path = sliced
    else:
        path = src

    size_mb = path.stat().st_size / (1024 * 1024)
    n_chunks = (path.stat().st_size + chunk_bytes - 1) // chunk_bytes

    print()
    print(f"  source     : {src}")
    print(f"  to send    : {path}  ({size_mb:.1f} MB, {n_chunks} chunks)")
    print(f"  tenant     : {args.tenant}")
    print(f"  gateway    : {args.gateway}")
    print(f"  chunk size : {args.chunk_mb} MB")
    print()

    with httpx.Client(timeout=30.0) as client:
        # 1. Authenticate
        token = authenticate(client, args.gateway, args.tenant, secret)
        print(f"  authenticated as {args.tenant} (token: {token[:24]}…)")

        # 2. Optionally reset webhook
        if args.reset_webhook:
            reset_webhook(client, args.webhook)
            print(f"  webhook traces reset")

        # 3. Stream PCAP chunks
        print()
        print(f"  POSTing chunks to {args.gateway}/ingest_pcap …")
        t0 = time.time()
        bytes_sent = 0
        ok = 0
        deduped = 0
        failed = 0

        for i, chunk in enumerate(stream_chunks(path, chunk_bytes), start=1):
            try:
                r = client.post(f"{args.gateway}/ingest_pcap",
                                content=chunk,
                                headers={
                                    "Authorization": f"Bearer {token}",
                                    "Content-Type": "application/octet-stream",
                                    "X-Chunk-Id": f"pcap-{src.stem}-{i:04d}",
                                    "X-Pcap-File": src.name,
                                    "X-Trace-Id": f"pcap-{src.stem}-{i:04d}",
                                })
                if r.status_code == 202:
                    ok += 1
                    if r.json().get("status") == "deduped":
                        deduped += 1
                else:
                    failed += 1
                    print(f"    chunk {i}/{n_chunks} HTTP {r.status_code}: {r.text[:120]}")
            except Exception as e:
                failed += 1
                print(f"    chunk {i}/{n_chunks} error: {e}")
                continue

            bytes_sent += len(chunk)
            elapsed = time.time() - t0
            mb_s = (bytes_sent / (1024 * 1024)) / max(elapsed, 1e-6)
            print(f"    chunk {i:3d}/{n_chunks}   "
                  f"{bytes_sent / (1024*1024):6.1f} MB  "
                  f"{mb_s:5.1f} MB/s",
                  flush=True)

        elapsed = time.time() - t0

    print()
    print(f"  done — {ok}/{n_chunks} chunks accepted "
          f"({deduped} deduped, {failed} failed) "
          f"in {elapsed:.1f}s")
    print()
    print(f"  → watch the dashboard at {args.webhook} for incoming alerts")
    print(f"    (it polls every 1s — alerts arrive within 5–10 seconds)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
