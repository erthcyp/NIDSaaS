"""Scenario runner for the load-test harness.

Exposes a tiny CLI that maps experiment IDs (e1 / e2 / e5) to
preconfigured LoadProfile sets and feeds them to driver.run_load.

  e1  Throughput-latency frontier — single tenant, ramp rate.
      Output: one JSON per (mode, rate) cell so report.py can stitch them.

  e2  Multi-tenant noisy-neighbour — tenant-A steady, tenant-B ramps.
      Output: a single JSON capturing both tenants' tail latency.

  e5  Resource footprint — fixed rate for D seconds with docker stats
      sampled once per second. Output is the standard run JSON plus a
      ``resource_samples`` block.

The runner does NOT switch INGEST_MODE on the gateway for you; flip the
env and recreate the gateway container before pointing the harness at
it. report.py expects pairs of runs (kafka vs direct_http) anyway.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import asdict
from typing import Any

import httpx

from docker_stats import sample_loop as docker_sample, summarize as docker_summarize
from driver import (
    LoadProfile,
    RunSummary,
    build_summary,
    fetch_traces,
    join_records,
    reset_traces,
    run_load,
    save_run,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [loadtest] %(message)s")
log = logging.getLogger("loadtest")


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_GATEWAY = os.environ.get("LOADTEST_GATEWAY_URL", "http://localhost:8080")
DEFAULT_WEBHOOK = os.environ.get("LOADTEST_WEBHOOK_URL", "http://localhost:9000")
DEFAULT_TENANTS = ("acme", "globex", "initech")
DEFAULT_SECRETS = {
    "acme": "acme-secret",
    "globex": "globex-secret",
    "initech": "initech-secret",
}

OUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")


# ---------------------------------------------------------------------------
# Mode-aware gateway probe
# ---------------------------------------------------------------------------
async def gateway_mode(gateway_url: str) -> str:
    async with httpx.AsyncClient(timeout=5.0) as c:
        r = await c.get(f"{gateway_url}/healthz")
        r.raise_for_status()
        return r.json().get("ingest_mode", "unknown")


async def assert_gateway_mode(gateway_url: str, want: str) -> None:
    got = await gateway_mode(gateway_url)
    if got != want:
        raise SystemExit(
            f"gateway INGEST_MODE is {got!r} but harness was asked for {want!r}.\n"
            f"  Fix:  sed -i 's/^INGEST_MODE=.*/INGEST_MODE={want}/' ../.env "
            f"&& docker compose up -d --no-deps --force-recreate gateway"
        )


# ---------------------------------------------------------------------------
# E1 — single-tenant throughput sweep
# ---------------------------------------------------------------------------
async def run_e1(args: argparse.Namespace) -> None:
    tenant = args.tenant
    secret = DEFAULT_SECRETS[tenant]
    profile = LoadProfile(
        tenant=tenant, secret=secret,
        rate_per_sec=args.rate, duration_sec=args.duration,
        payload_kind=args.payload,
        pcap_bytes_per_chunk=args.pcap_bytes,
    )
    summary, sends, joined = await run_load(
        [profile],
        gateway_url=args.gateway, webhook_url=args.webhook,
        scenario=f"e1_rate{int(args.rate)}",
        mode=args.mode,
        settle_sec=args.settle,
    )
    summary.notes = (f"E1 throughput sweep: tenant={tenant} rate={args.rate}rps "
                     f"payload={args.payload} duration={args.duration}s")
    path = save_run(OUT_DIR, summary, joined, sends)
    log.info("E1 wrote %s", path)
    _print_brief(summary)


# ---------------------------------------------------------------------------
# E2 — multi-tenant noisy-neighbour
# ---------------------------------------------------------------------------
async def run_e2(args: argparse.Namespace) -> None:
    quiet = args.quiet_tenant
    noisy = args.noisy_tenant
    profiles = [
        LoadProfile(tenant=quiet, secret=DEFAULT_SECRETS[quiet],
                    rate_per_sec=args.quiet_rate, duration_sec=args.duration,
                    payload_kind=args.payload, pcap_bytes_per_chunk=args.pcap_bytes),
        LoadProfile(tenant=noisy, secret=DEFAULT_SECRETS[noisy],
                    rate_per_sec=args.noisy_rate, duration_sec=args.duration,
                    payload_kind=args.payload, pcap_bytes_per_chunk=args.pcap_bytes),
    ]
    summary, sends, joined = await run_load(
        profiles,
        gateway_url=args.gateway, webhook_url=args.webhook,
        scenario="e2_noisy_neighbour",
        mode=args.mode,
        settle_sec=args.settle,
    )
    summary.notes = (f"E2 noisy-neighbour: quiet={quiet}@{args.quiet_rate}rps "
                     f"noisy={noisy}@{args.noisy_rate}rps duration={args.duration}s")
    path = save_run(OUT_DIR, summary, joined, sends)
    log.info("E2 wrote %s", path)
    _print_brief(summary)


# ---------------------------------------------------------------------------
# E5 — resource footprint (load + docker stats sampler)
# ---------------------------------------------------------------------------
async def run_e5(args: argparse.Namespace) -> None:
    profiles = [
        LoadProfile(tenant=t, secret=DEFAULT_SECRETS[t],
                    rate_per_sec=args.rate, duration_sec=args.duration,
                    payload_kind=args.payload,
                    pcap_bytes_per_chunk=args.pcap_bytes)
        for t in args.tenants
    ]

    containers = args.containers or [
        "nidsaas_gateway", "nidsaas_detector", "nidsaas_flow_extractor",
        "nidsaas_snort_sidecar", "nidsaas_alert_fanout",
        "nidsaas_kafka", "nidsaas_webhook_receiver",
    ]
    log.info("E5 sampling %s every %.1fs for %.1fs",
             containers, args.stats_interval, args.duration + args.settle)

    # Drive the load and the stats sampler concurrently.
    sampler_task = asyncio.create_task(
        docker_sample(containers, args.stats_interval, args.duration + args.settle)
    )
    summary, sends, joined = await run_load(
        profiles,
        gateway_url=args.gateway, webhook_url=args.webhook,
        scenario="e5_resource_footprint",
        mode=args.mode,
        settle_sec=args.settle,
    )
    samples = await sampler_task
    resource = docker_summarize(samples)

    # Fold the resource block into the summary's notes/extras.
    summary.notes = (f"E5 resource footprint: tenants={args.tenants} "
                     f"rate={args.rate}rps duration={args.duration}s")
    path = save_run(OUT_DIR, summary, joined, sends)
    # Re-open the saved JSON to append the resource block (driver.save_run
    # writes a fixed schema; we extend it here on purpose so the schema
    # stays single-source).
    with open(path) as f:
        payload = json.load(f)
    payload["resource_samples"] = resource
    with open(path, "w") as f:
        json.dump(payload, f, default=str, indent=2)
    log.info("E5 wrote %s (with resource_samples)", path)
    _print_brief(summary)
    log.info("resource summary:")
    for name, s in resource.items():
        log.info("  %-30s cpu mean=%.1f%% max=%.1f%%  rss mean=%.0fMiB max=%.0fMiB",
                 name, s["cpu_mean"], s["cpu_max"],
                 s["rss_mean_mib"], s["rss_max_mib"])


def _print_brief(summary: RunSummary) -> None:
    sent = summary.sent
    delivered = summary.delivered_traces
    rate = (delivered / sent * 100.0) if sent else 0.0
    log.info("---- %s [%s] ----", summary.scenario, summary.mode)
    log.info("sent=%d accepted=%d deduped=%d failed=%d delivered=%d (%.1f%%)",
             sent, summary.accepted, summary.deduped, summary.failed,
             delivered, rate)
    log.info("e2e ms: p50=%s p95=%s p99=%s max=%s",
             _fmt(summary.e2e_ms_p50), _fmt(summary.e2e_ms_p95),
             _fmt(summary.e2e_ms_p99), _fmt(summary.e2e_ms_max))
    for t, blk in summary.per_tenant.items():
        lat = blk["latency_ms"]
        log.info("  %-10s sent=%d delivered=%d (%.1f%%) p50=%s p95=%s p99=%s",
                 t, blk["sent"], blk["delivered"],
                 (blk["delivery_rate"] or 0) * 100,
                 _fmt(lat["p50"]), _fmt(lat["p95"]), _fmt(lat["p99"]))


def _fmt(x: float | None) -> str:
    return f"{x:.1f}" if x is not None else "-"


# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(prog="run_experiment")
    parser.add_argument("scenario", choices=["e1", "e2", "e5"])
    parser.add_argument("--gateway", default=DEFAULT_GATEWAY)
    parser.add_argument("--webhook", default=DEFAULT_WEBHOOK)
    parser.add_argument("--mode", required=True, choices=["kafka", "direct_http"],
                        help="Records the active gateway mode in the output. The "
                             "harness asserts the gateway is in this mode before "
                             "running.")
    parser.add_argument("--duration", type=float, default=30.0,
                        help="Seconds to sustain load.")
    parser.add_argument("--settle", type=float, default=10.0,
                        help="Seconds of grace after sends finish before fetching traces.")
    parser.add_argument("--payload", choices=["csv", "pcap"], default="csv",
                        help="csv -> /ingest, pcap -> /ingest_pcap with synthetic chunks.")
    parser.add_argument("--pcap-bytes", type=int, default=8_192,
                        help="Synthetic pcap chunk size when --payload=pcap.")
    parser.add_argument("--rate", type=float, default=20.0,
                        help="E1/E5 per-tenant target rate (req/sec).")
    parser.add_argument("--tenant", default="acme", help="E1 single tenant.")
    parser.add_argument("--tenants", nargs="+", default=list(DEFAULT_TENANTS),
                        help="E5 tenant set.")
    parser.add_argument("--quiet-tenant", default="acme", help="E2 quiet tenant.")
    parser.add_argument("--noisy-tenant", default="globex", help="E2 noisy tenant.")
    parser.add_argument("--quiet-rate", type=float, default=10.0,
                        help="E2 quiet tenant target rate (req/sec).")
    parser.add_argument("--noisy-rate", type=float, default=200.0,
                        help="E2 noisy tenant target rate (req/sec).")
    parser.add_argument("--containers", nargs="+", default=None,
                        help="Override the docker container set sampled for E5.")
    parser.add_argument("--stats-interval", type=float, default=1.0,
                        help="E5 docker stats sampling interval (sec).")
    args = parser.parse_args()

    asyncio.run(assert_gateway_mode(args.gateway, args.mode))

    if args.scenario == "e1":
        asyncio.run(run_e1(args))
    elif args.scenario == "e2":
        asyncio.run(run_e2(args))
    elif args.scenario == "e5":
        asyncio.run(run_e5(args))


if __name__ == "__main__":
    main()
