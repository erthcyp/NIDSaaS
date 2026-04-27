"""Low-level load driver shared by all scenarios.

Sends gateway POST requests at a target rate per tenant, tags each with a
unique trace id, and joins the gateway-side send timestamp with the
webhook-receiver-side arrival timestamp at the end of the run.

Every scenario in run_experiment.py composes one or more LoadProfile
objects and hands them to ``run_load(...)``. Profiles run concurrently so
multi-tenant scenarios (E2 noisy neighbour, E3 scalability) drop out for
free.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import random
import statistics
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import httpx

log = logging.getLogger("driver")


# ---------------------------------------------------------------------------
# Shapes
# ---------------------------------------------------------------------------
@dataclass
class LoadProfile:
    tenant: str
    secret: str
    rate_per_sec: float                  # target requests per second for this tenant
    duration_sec: float                  # total wall-clock seconds to sustain it
    payload_kind: str = "csv"            # "csv" -> /ingest, "pcap" -> /ingest_pcap
    pcap_bytes_per_chunk: int = 8_192    # only used when payload_kind == "pcap"


@dataclass
class SendRecord:
    trace_id: str
    tenant: str
    send_ts: float                       # wall-clock
    send_monotonic: float                # used for latency math
    accepted: bool
    status_code: int
    deduped: bool
    error: str | None = None


@dataclass
class JoinedRecord:
    trace_id: str
    tenant: str
    send_ts: float
    receive_ts: float | None
    e2e_ms: float | None                 # receive_ts - send_ts
    receipts: int                        # how many webhook deliveries shared this trace_id
    accepted: bool                       # gateway accepted the original POST
    deduped: bool


@dataclass
class RunSummary:
    scenario: str
    mode: str
    started_at: float
    duration_sec: float
    profiles: list[dict[str, Any]]
    sent: int
    accepted: int
    deduped: int
    failed: int
    delivered_traces: int                # traces with at least one webhook arrival
    e2e_ms_p50: float | None
    e2e_ms_p95: float | None
    e2e_ms_p99: float | None
    e2e_ms_max: float | None
    per_tenant: dict[str, dict[str, Any]] = field(default_factory=dict)
    notes: str = ""


# ---------------------------------------------------------------------------
# Token cache
# ---------------------------------------------------------------------------
async def fetch_token(client: httpx.AsyncClient, gateway_url: str,
                      tenant: str, secret: str) -> str:
    last_err: Exception | None = None
    for i in range(20):
        try:
            r = await client.post(
                f"{gateway_url}/oauth/token",
                data={"grant_type": "client_credentials",
                      "client_id": tenant,
                      "client_secret": secret},
                timeout=5.0,
            )
            r.raise_for_status()
            return r.json()["access_token"]
        except Exception as e:  # noqa: BLE001
            last_err = e
            await asyncio.sleep(0.5 * (i + 1))
    raise RuntimeError(f"token fetch failed for {tenant}: {last_err}")


# ---------------------------------------------------------------------------
# Synthetic payload generators
# ---------------------------------------------------------------------------
def synth_flow_record(tenant: str, rng: random.Random) -> dict[str, Any]:
    """A single CIC-IDS2017-shaped flow record.

    The harness measures transport latency, not detection precision, so
    every record is sized to fire the Tier-1 rate rule (high pps + high
    SYN-to-ACK ratio). Tier-1 emits an attack verdict synchronously,
    which guarantees the request hits the alert_fanout -> webhook path
    and shows up in /traces. If we generated benign flows the webhook
    would never see them and the join would be empty.
    """
    # Hard-coded into the SYN flood corner of the rate-rule space:
    #   V:  pps = (fwd+bwd) / (duration_us / 1e6) ≈ 80 / 0.05 = 1600 >= 500
    #   S:  SYN >= 30 AND SYN/ACK >= 3 always satisfied (SYN 50-100, ACK 1-3)
    # Tier-1 fires synchronously, the verdict reaches alert_fanout, and
    # the webhook records the trace_id — which is the join key the load
    # report needs.
    # NB: flow_id MUST be globally unique across runs — gateway dedupes
    # POST bodies on flow_id within dedup_window_sec (60s default). Seeded
    # rng would replay the same sequence across runs → false dedupes.
    # uuid.uuid4() bypasses the seed and gives ~0 collision probability.
    return {
        "flow_id": f"loadtest-{tenant}-{uuid.uuid4().hex}",
        "source_file": "loadtest",
        "row_id": rng.randint(0, 10**9),
        "label": "ATTACK",
        "features": {
            "Destination Port": rng.choice([22, 23, 3389]),
            "Flow Duration": rng.randint(20_000, 80_000),
            "Total Fwd Packets": rng.randint(60, 120),
            "Total Backward Packets": rng.randint(0, 3),
            "Total Length of Fwd Packets": rng.randint(2_000, 20_000),
            "Total Length of Bwd Packets": rng.randint(0, 200),
            "SYN Flag Count": rng.randint(50, 100),
            "ACK Flag Count": rng.randint(1, 3),
            "RST Flag Count": rng.randint(0, 3),
            "Flow Packets/s": rng.uniform(1500, 2000),
            "Flow Bytes/s": rng.uniform(2e5, 2e6),
            "Packet Length Std": rng.uniform(20, 200),
            "Idle Mean": rng.uniform(0, 1e6),
            "Init_Win_bytes_forward": rng.randint(15, 60),
        },
    }


def synth_pcap_chunk(byte_count: int, rng: random.Random) -> bytes:
    """A minimal classic-pcap byte string. The flow_extractor will fail
    to extract any flows from this (it's not a real capture) but the
    end-to-end transport timing is what we're measuring; a junk chunk
    still goes through the same kafka topic / HTTP fan-out path."""
    # 24-byte global header (LE magic, v2.4, no thiszone, default snaplen, link=ETHERNET)
    hdr = bytes.fromhex("d4c3b2a1") + bytes.fromhex("02000400") + bytes.fromhex(
        "00000000" "00000000" "ffff0000" "01000000"
    )
    payload_total = max(0, byte_count - len(hdr))
    return hdr + rng.randbytes(payload_total)


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------
async def _send_flow(
    client: httpx.AsyncClient, gateway_url: str,
    tenant: str, token: str, body: dict[str, Any], trace_id: str,
) -> SendRecord:
    headers = {"Authorization": f"Bearer {token}", "X-Trace-Id": trace_id}
    send_ts = time.time()
    send_monotonic = time.monotonic()
    try:
        r = await client.post(f"{gateway_url}/ingest", json=body, headers=headers,
                              timeout=30.0)
    except Exception as e:  # noqa: BLE001
        return SendRecord(trace_id, tenant, send_ts, send_monotonic,
                          False, 0, False, str(e))
    deduped = False
    accepted = False
    if r.status_code == 202:
        try:
            j = r.json()
            deduped = j.get("status") == "deduped"
            accepted = not deduped
        except Exception:
            accepted = True
    return SendRecord(trace_id, tenant, send_ts, send_monotonic,
                      accepted, r.status_code, deduped)


async def _send_pcap(
    client: httpx.AsyncClient, gateway_url: str,
    tenant: str, token: str, chunk: bytes, trace_id: str,
) -> SendRecord:
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Trace-Id": trace_id,
        "X-Chunk-Id": trace_id,
        "X-Pcap-File": "loadtest.pcap",
        "Content-Type": "application/octet-stream",
    }
    send_ts = time.time()
    send_monotonic = time.monotonic()
    try:
        r = await client.post(f"{gateway_url}/ingest_pcap", content=chunk,
                              headers=headers, timeout=120.0)
    except Exception as e:  # noqa: BLE001
        return SendRecord(trace_id, tenant, send_ts, send_monotonic,
                          False, 0, False, str(e))
    deduped = False
    accepted = False
    if r.status_code == 202:
        try:
            j = r.json()
            deduped = j.get("status") == "deduped"
            accepted = not deduped
        except Exception:
            accepted = True
    return SendRecord(trace_id, tenant, send_ts, send_monotonic,
                      accepted, r.status_code, deduped)


# ---------------------------------------------------------------------------
# Per-tenant loop
# ---------------------------------------------------------------------------
async def _run_profile(
    client: httpx.AsyncClient,
    gateway_url: str,
    profile: LoadProfile,
    out: list[SendRecord],
    seed: int,
) -> None:
    rng = random.Random(seed)
    token = await fetch_token(client, gateway_url, profile.tenant, profile.secret)

    interval = 1.0 / max(profile.rate_per_sec, 1e-3)
    end = time.monotonic() + profile.duration_sec
    next_at = time.monotonic()
    inflight: list[asyncio.Task] = []

    async def _record(coro: Awaitable[SendRecord]) -> None:
        try:
            rec = await coro
        except Exception as e:  # noqa: BLE001
            rec = SendRecord(uuid.uuid4().hex, profile.tenant,
                             time.time(), time.monotonic(),
                             False, 0, False, str(e))
        out.append(rec)

    while time.monotonic() < end:
        now = time.monotonic()
        if now < next_at:
            await asyncio.sleep(min(next_at - now, 0.05))
            continue
        next_at += interval

        trace_id = uuid.uuid4().hex
        if profile.payload_kind == "pcap":
            coro = _send_pcap(client, gateway_url, profile.tenant, token,
                              synth_pcap_chunk(profile.pcap_bytes_per_chunk, rng),
                              trace_id)
        else:
            coro = _send_flow(client, gateway_url, profile.tenant, token,
                              synth_flow_record(profile.tenant, rng),
                              trace_id)
        inflight.append(asyncio.create_task(_record(coro)))

        # Bound in-flight so a slow gateway doesn't blow memory
        if len(inflight) > 5_000:
            done, pending = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            inflight = list(pending)

    if inflight:
        await asyncio.gather(*inflight)


# ---------------------------------------------------------------------------
# Webhook trace fetch + join
# ---------------------------------------------------------------------------
async def fetch_traces(client: httpx.AsyncClient, webhook_url: str) -> dict[str, list[dict[str, Any]]]:
    r = await client.get(f"{webhook_url}/traces", timeout=30.0)
    r.raise_for_status()
    return r.json().get("traces", {})


async def reset_traces(client: httpx.AsyncClient, webhook_url: str) -> None:
    try:
        await client.post(f"{webhook_url}/traces/reset", timeout=10.0)
    except Exception as e:  # noqa: BLE001
        log.warning("trace reset failed (will get noisy joins): %s", e)


def _percentile(xs: list[float], p: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def _summarize_latencies(values: list[float]) -> dict[str, float | None]:
    return {
        "n": len(values),
        "p50": _percentile(values, 50),
        "p95": _percentile(values, 95),
        "p99": _percentile(values, 99),
        "max": max(values) if values else None,
        "mean": statistics.fmean(values) if values else None,
    }


def join_records(
    sends: list[SendRecord],
    traces: dict[str, list[dict[str, Any]]],
) -> list[JoinedRecord]:
    by_trace = {s.trace_id: s for s in sends}
    out: list[JoinedRecord] = []
    for tid, send in by_trace.items():
        receipts = traces.get(tid, [])
        if receipts:
            first = min(r["receive_ts"] for r in receipts)
            e2e_ms = (first - send.send_ts) * 1000.0
            out.append(JoinedRecord(tid, send.tenant, send.send_ts,
                                    first, e2e_ms, len(receipts),
                                    send.accepted, send.deduped))
        else:
            out.append(JoinedRecord(tid, send.tenant, send.send_ts,
                                    None, None, 0, send.accepted, send.deduped))
    return out


def build_summary(
    scenario: str, mode: str, started_at: float, duration_sec: float,
    profiles: list[LoadProfile], joined: list[JoinedRecord], notes: str = "",
) -> RunSummary:
    sent = len(joined)
    accepted = sum(1 for j in joined if j.accepted)
    deduped = sum(1 for j in joined if j.deduped)
    failed = sum(1 for j in joined if not j.accepted and not j.deduped)
    delivered = [j for j in joined if j.e2e_ms is not None]
    overall = [j.e2e_ms for j in delivered]
    summary = RunSummary(
        scenario=scenario, mode=mode, started_at=started_at,
        duration_sec=duration_sec,
        profiles=[dataclasses.asdict(p) for p in profiles],
        sent=sent, accepted=accepted, deduped=deduped, failed=failed,
        delivered_traces=len(delivered),
        e2e_ms_p50=_percentile(overall, 50),
        e2e_ms_p95=_percentile(overall, 95),
        e2e_ms_p99=_percentile(overall, 99),
        e2e_ms_max=max(overall) if overall else None,
        notes=notes,
    )
    by_tenant: dict[str, list[float]] = {}
    by_tenant_sent: dict[str, int] = {}
    by_tenant_delivered: dict[str, int] = {}
    for j in joined:
        by_tenant_sent[j.tenant] = by_tenant_sent.get(j.tenant, 0) + 1
        if j.e2e_ms is not None:
            by_tenant.setdefault(j.tenant, []).append(j.e2e_ms)
            by_tenant_delivered[j.tenant] = by_tenant_delivered.get(j.tenant, 0) + 1
    for t, sent_n in by_tenant_sent.items():
        lat = _summarize_latencies(by_tenant.get(t, []))
        summary.per_tenant[t] = {
            "sent": sent_n,
            "delivered": by_tenant_delivered.get(t, 0),
            "delivery_rate": (by_tenant_delivered.get(t, 0) / sent_n
                              if sent_n else None),
            "latency_ms": lat,
        }
    return summary


# ---------------------------------------------------------------------------
# Entry point used by run_experiment.py
# ---------------------------------------------------------------------------
async def run_load(
    profiles: list[LoadProfile],
    *,
    gateway_url: str,
    webhook_url: str,
    scenario: str,
    mode: str,
    settle_sec: float = 10.0,
    seed: int | None = None,
    pre_hook: Callable[[httpx.AsyncClient], Awaitable[None]] | None = None,
) -> tuple[RunSummary, list[SendRecord], list[JoinedRecord]]:
    """Send according to ``profiles`` concurrently, then join with traces.

    ``settle_sec`` is the grace period after the last send before fetching
    traces — pcap chunks can take seconds to extract + score, so we
    don't want to call the verdict missing just because the harness was
    too eager.

    ``seed=None`` (default) draws a time-based seed so each run produces
    distinct synthetic feature payloads. The gateway dedupes on a SHA-1
    of the ``features`` dict within ``GATEWAY_DEDUP_WINDOW_SEC`` (60s
    default); a fixed seed makes back-to-back runs collide.
    """
    if seed is None:
        seed = time.time_ns() & 0xFFFFFFFF
    duration = max(p.duration_sec for p in profiles)

    async with httpx.AsyncClient() as client:
        if pre_hook is not None:
            await pre_hook(client)
        await reset_traces(client, webhook_url)

        sends: list[SendRecord] = []
        started_at = time.time()
        await asyncio.gather(*[
            _run_profile(client, gateway_url, p, sends, seed + i)
            for i, p in enumerate(profiles)
        ])
        log.info("send phase done: %d records; settling %.1fs", len(sends), settle_sec)
        await asyncio.sleep(settle_sec)
        traces = await fetch_traces(client, webhook_url)

    joined = join_records(sends, traces)
    summary = build_summary(scenario, mode, started_at, duration, profiles, joined)
    return summary, sends, joined


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def save_run(
    out_dir: str, summary: RunSummary,
    joined: list[JoinedRecord], sends: list[SendRecord],
) -> str:
    os.makedirs(out_dir, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(summary.started_at))
    fname = f"{summary.scenario}_{summary.mode}_{stamp}.json"
    path = os.path.join(out_dir, fname)
    payload = {
        "summary": dataclasses.asdict(summary),
        "sends": [dataclasses.asdict(s) for s in sends],
        "joined": [dataclasses.asdict(j) for j in joined],
    }
    with open(path, "w") as f:
        json.dump(payload, f, default=str, indent=2)
    return path
