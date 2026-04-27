"""Tenant simulator.

Spins up one async worker per tenant. Two modes:

CSV mode (default, SIM_MODE=csv)
    Reads /csv/*.csv (CIC-IDS2017 pre-extracted flows) and POSTs one row at a
    time to /ingest. Fast, deterministic, matches the offline research
    pipeline's feature space exactly.

Pcap mode (SIM_MODE=pcap)
    Reads /pcaps/*.pcap (raw CIC-IDS2017 captures), slices each file into
    packet-boundary-safe chunks (default 5000 packets per chunk), and POSTs
    each chunk to /ingest_pcap. The gateway forwards chunks to the
    flow_extractor sidecar, which runs CICFlowMeter v4 and republishes
    extracted flows to tenant.{u}.raw. This matches the paper's Figure 1
    end-to-end (pcap -> flow extraction -> detection).

Scenario presets (CSV mode only):
    tenant[0] -> mostly attack rows (portscan + DoS)
    tenant[1] -> benign only
    tenant[2] -> benign with a brute-force burst

In pcap mode all tenants receive the same pcap menu unless per-tenant
subdirectories exist under PCAP_DIR (e.g. /pcaps/acme/*.pcap).
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import random
import shutil
import struct
import subprocess
from pathlib import Path
from typing import Iterable, Iterator

import httpx
import pandas as pd

log = logging.getLogger("simulator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [sim] %(message)s")

GATEWAY = os.environ.get("GATEWAY_URL", "http://gateway:8080")
CSV_ROOT = Path(os.environ.get("CSV_DIR", "/csv"))
PCAP_ROOT = Path(os.environ.get("PCAP_DIR", "/pcaps"))
TENANTS = [t.strip() for t in os.environ.get("TENANTS", "acme,globex,initech").split(",") if t.strip()]
OAUTH_PAIRS = os.environ.get("OAUTH_CLIENTS", "")
ROWS_PER_TENANT = int(os.environ.get("SIM_ROWS_PER_TENANT", "2000"))
RATE_PER_SEC = float(os.environ.get("SIM_RATE_PER_SEC", "20"))

SIM_MODE = os.environ.get("SIM_MODE", "csv").strip().lower()
PCAP_PACKETS_PER_CHUNK = int(os.environ.get("SIM_PCAP_PACKETS_PER_CHUNK", "5000"))
PCAP_CHUNK_GAP_SEC = float(os.environ.get("SIM_PCAP_CHUNK_GAP_SEC", "1.0"))
PCAP_MAX_CHUNKS_PER_FILE = int(os.environ.get("SIM_PCAP_MAX_CHUNKS_PER_FILE", "40"))
PCAP_FILES_PER_TENANT = int(os.environ.get("SIM_PCAP_FILES_PER_TENANT", "1"))

PRESETS = ["attack_heavy", "benign_only", "brute_force_burst"]


def _secret_for(client_id: str) -> str:
    for pair in OAUTH_PAIRS.split(";"):
        if ":" in pair:
            cid, secret = pair.split(":", 1)
            if cid.strip() == client_id:
                return secret.strip()
    raise RuntimeError(f"no client_secret configured for tenant {client_id!r}")


def _iter_rows(df: pd.DataFrame) -> Iterable[dict]:
    for _, row in df.iterrows():
        features = {k: (None if pd.isna(v) else v) for k, v in row.items() if k != "Label"}
        yield {
            "flow_id": f"{row.get('Flow ID', '') or random.random():.20}",
            "source_file": "",
            "row_id": int(row.name) if isinstance(row.name, (int, float)) else None,
            "label": str(row.get("Label", "")),
            "features": features,
        }


def _pick_slice(preset: str) -> pd.DataFrame:
    """Read CIC-IDS2017 CSVs and return a preset-flavoured slice."""
    csv_files = sorted(CSV_ROOT.glob("*.csv"))
    if not csv_files:
        log.warning("no CSVs under %s; generating synthetic benign traffic", CSV_ROOT)
        return _synthetic_benign(ROWS_PER_TENANT)

    dfs: list[pd.DataFrame] = []
    for p in csv_files[:3]:                  # cap file count for startup speed
        try:
            dfs.append(pd.read_csv(p, low_memory=False, nrows=20000))
        except Exception as e:  # noqa: BLE001
            log.warning("skip %s: %s", p.name, e)
    df = pd.concat(dfs, ignore_index=True).dropna(subset=["Label"] if "Label" in dfs[0].columns else [])
    label_col = "Label" if "Label" in df.columns else df.columns[-1]

    is_benign = df[label_col].astype(str).str.upper().str.contains("BENIGN")
    benign = df[is_benign]
    attack = df[~is_benign]

    if preset == "attack_heavy":
        a = attack.sample(n=min(len(attack), int(0.7 * ROWS_PER_TENANT)), random_state=1)
        b = benign.sample(n=ROWS_PER_TENANT - len(a), random_state=1)
        return pd.concat([a, b]).sample(frac=1, random_state=1).reset_index(drop=True)
    if preset == "benign_only":
        return benign.sample(n=min(len(benign), ROWS_PER_TENANT), random_state=2).reset_index(drop=True)
    if preset == "brute_force_burst":
        b = benign.sample(n=int(0.85 * ROWS_PER_TENANT), random_state=3)
        bf = attack[attack[label_col].astype(str).str.contains("Brute", case=False, na=False)]
        if len(bf) == 0:
            bf = attack.sample(n=int(0.15 * ROWS_PER_TENANT), random_state=3)
        else:
            bf = bf.sample(n=min(len(bf), int(0.15 * ROWS_PER_TENANT)), random_state=3)
        return pd.concat([b, bf]).reset_index(drop=True)
    return benign.head(ROWS_PER_TENANT)


def _synthetic_benign(n: int) -> pd.DataFrame:
    rng = random.Random(42)
    rows = []
    for i in range(n):
        rows.append({
            "Flow ID": f"synthetic-{i}",
            "Destination Port": rng.choice([80, 443, 8080]),
            "Flow Duration": rng.randint(10_000, 2_000_000),
            "Total Fwd Packets": rng.randint(1, 20),
            "Total Backward Packets": rng.randint(1, 20),
            "SYN Flag Count": rng.randint(0, 2),
            "ACK Flag Count": rng.randint(1, 20),
            "RST Flag Count": 0,
            "Flow Packets/s": rng.uniform(10, 200),
            "Flow Bytes/s": rng.uniform(1e3, 1e5),
            "Packet Length Std": rng.uniform(10, 300),
            "Label": "BENIGN",
        })
    return pd.DataFrame(rows)


async def _token(client: httpx.AsyncClient, tenant: str) -> str:
    r = await client.post(
        f"{GATEWAY}/oauth/token",
        data={"grant_type": "client_credentials",
              "client_id": tenant,
              "client_secret": _secret_for(tenant)},
    )
    r.raise_for_status()
    return r.json()["access_token"]


async def run_tenant(tenant: str, preset: str) -> None:
    log.info("[%s] preset=%s", tenant, preset)
    df = _pick_slice(preset)
    log.info("[%s] will send %d rows", tenant, len(df))

    async with httpx.AsyncClient(timeout=10.0) as client:
        # retry token until gateway is up
        token = None
        for i in range(30):
            try:
                token = await _token(client, tenant)
                break
            except Exception as e:  # noqa: BLE001
                log.warning("[%s] token fail (%s); retry %d/30", tenant, e, i + 1)
                await asyncio.sleep(2)
        if token is None:
            log.error("[%s] gave up getting token", tenant)
            return

        headers = {"Authorization": f"Bearer {token}"}
        sent = accepted = deduped = 0
        gap = 1.0 / max(RATE_PER_SEC, 0.1)

        for rec in _iter_rows(df):
            try:
                r = await client.post(f"{GATEWAY}/ingest", json=rec, headers=headers)
                if r.status_code == 202:
                    body = r.json()
                    if body.get("status") == "deduped":
                        deduped += 1
                    else:
                        accepted += 1
                else:
                    log.warning("[%s] ingest %d: %s", tenant, r.status_code, r.text[:200])
            except Exception as e:  # noqa: BLE001
                log.warning("[%s] send error: %s", tenant, e)
            sent += 1
            if sent % 200 == 0:
                log.info("[%s] sent=%d accepted=%d deduped=%d", tenant, sent, accepted, deduped)
            await asyncio.sleep(gap)

        log.info("[%s] DONE: sent=%d accepted=%d deduped=%d", tenant, sent, accepted, deduped)


# ---------------------------------------------------------------------------
# Pcap mode helpers
# ---------------------------------------------------------------------------
# pcap file format (classic):
#   struct pcap_file_header  -- 24 bytes, always at offset 0
#   struct pcap_record_hdr   -- 16 bytes per packet
#   <incl_len bytes of packet data>
#
# To slice a pcap on packet boundaries we copy the 24-byte global header into
# every output chunk and then append whole (record header + payload) pairs
# until the chunk hits PCAP_PACKETS_PER_CHUNK. Byte-boundary slicing would
# truncate packets and CICFlowMeter would refuse the chunk.
_PCAP_GLOBAL_HDR_LEN = 24
_PCAP_RECORD_HDR_LEN = 16

# pcapng Section Header Block magic. Files with this header need to be
# normalised to classic pcap (libpcap format) before our packet-boundary
# chunker can slice them — the chunker assumes a flat {24-byte global
# header, N * (16-byte record header + payload)} layout that pcapng does
# not have.
_PCAPNG_MAGIC = b"\x0a\x0d\x0d\x0a"
_PCAP_CONVERTED_DIR = Path("/tmp/pcap_converted")


def _is_pcapng(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == _PCAPNG_MAGIC
    except OSError:
        return False


def _ensure_classic_pcap(path: Path) -> Path:
    """Return a path guaranteed to be classic libpcap format.

    If `path` is already classic pcap, returns it unchanged.
    If it's pcapng, converts to /tmp/pcap_converted/<stem>.pcap via editcap
    and returns the new path. Conversion is cached so repeated calls with
    the same `path` are cheap.
    """
    if not _is_pcapng(path):
        return path
    _PCAP_CONVERTED_DIR.mkdir(parents=True, exist_ok=True)
    out = _PCAP_CONVERTED_DIR / (path.stem + ".pcap")
    if out.exists() and out.stat().st_size > 0:
        return out
    if shutil.which("editcap") is None:
        log.error(
            "[pcap] %s is pcapng but editcap is missing; install wireshark-common",
            path,
        )
        raise RuntimeError("editcap binary not found")
    log.info("[pcap] converting pcapng -> classic pcap: %s -> %s", path, out)
    try:
        subprocess.run(
            ["editcap", "-F", "libpcap", str(path), str(out)],
            check=True, capture_output=True, text=True,
        )
    except subprocess.CalledProcessError as e:
        log.error("[pcap] editcap failed for %s: %s", path, e.stderr.strip())
        raise
    return out


def _iter_pcap_chunks(path: Path, packets_per_chunk: int) -> Iterator[bytes]:
    """Yield self-contained pcap byte-strings, each with <=N packets."""
    with open(path, "rb") as f:
        global_hdr = f.read(_PCAP_GLOBAL_HDR_LEN)
        if len(global_hdr) < _PCAP_GLOBAL_HDR_LEN:
            log.warning("[pcap] %s too small to contain a pcap header", path)
            return

        # Detect endianness from the magic number so we parse incl_len correctly.
        magic = global_hdr[:4]
        if magic in (b"\xa1\xb2\xc3\xd4", b"\xa1\xb2\x3c\x4d"):
            endian = ">"
        elif magic in (b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1"):
            endian = "<"
        else:
            log.warning("[pcap] %s: unknown magic %s; assuming little-endian", path, magic.hex())
            endian = "<"
        record_fmt = endian + "IIII"  # ts_sec, ts_usec, incl_len, orig_len

        buf = io.BytesIO()
        buf.write(global_hdr)
        count = 0
        while True:
            rec = f.read(_PCAP_RECORD_HDR_LEN)
            if len(rec) < _PCAP_RECORD_HDR_LEN:
                break
            _, _, incl_len, _ = struct.unpack(record_fmt, rec)
            payload = f.read(incl_len)
            if len(payload) < incl_len:
                break
            buf.write(rec)
            buf.write(payload)
            count += 1
            if count >= packets_per_chunk:
                yield buf.getvalue()
                buf = io.BytesIO()
                buf.write(global_hdr)
                count = 0
        if count > 0:
            yield buf.getvalue()


def _pcaps_for_tenant(tenant: str) -> list[Path]:
    """Per-tenant subdir takes priority; fall back to shared PCAP_ROOT."""
    tdir = PCAP_ROOT / tenant
    if tdir.is_dir():
        files = sorted(tdir.glob("*.pcap"))
        if files:
            return files[:PCAP_FILES_PER_TENANT]
    files = sorted(PCAP_ROOT.glob("*.pcap"))
    return files[:PCAP_FILES_PER_TENANT]


async def _token_with_retry(client: httpx.AsyncClient, tenant: str) -> str | None:
    for i in range(30):
        try:
            return await _token(client, tenant)
        except Exception as e:  # noqa: BLE001
            log.warning("[%s] token fail (%s); retry %d/30", tenant, e, i + 1)
            await asyncio.sleep(2)
    log.error("[%s] gave up getting token", tenant)
    return None


async def run_tenant_pcap(tenant: str) -> None:
    pcaps = _pcaps_for_tenant(tenant)
    if not pcaps:
        log.warning("[%s] no pcap under %s or %s/%s; skipping",
                    tenant, PCAP_ROOT, PCAP_ROOT, tenant)
        return

    # Normalise pcapng -> classic pcap so the chunker sees a flat layout.
    # Conversion is cached under /tmp/pcap_converted so repeated tenants
    # re-use the same converted file.
    pcaps = [_ensure_classic_pcap(p) for p in pcaps]

    log.info("[%s] pcap mode: files=%s chunks_per_file=%d packets_per_chunk=%d",
             tenant, [p.name for p in pcaps], PCAP_MAX_CHUNKS_PER_FILE,
             PCAP_PACKETS_PER_CHUNK)

    # Large timeout because CICFlowMeter may take several seconds per chunk.
    async with httpx.AsyncClient(timeout=60.0) as client:
        token = await _token_with_retry(client, tenant)
        if token is None:
            return

        base_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        }
        total_chunks = 0
        total_bytes = 0

        for pcap in pcaps:
            idx = 0
            for chunk in _iter_pcap_chunks(pcap, PCAP_PACKETS_PER_CHUNK):
                if idx >= PCAP_MAX_CHUNKS_PER_FILE:
                    log.info("[%s] reached max_chunks_per_file=%d; next pcap",
                             tenant, PCAP_MAX_CHUNKS_PER_FILE)
                    break
                cid = f"{pcap.stem}-{idx:05d}"
                headers = {
                    **base_headers,
                    "X-Chunk-Id": cid,
                    "X-Pcap-File": pcap.name,
                }
                try:
                    r = await client.post(
                        f"{GATEWAY}/ingest_pcap",
                        content=chunk,
                        headers=headers,
                    )
                    if r.status_code != 202:
                        log.warning("[%s] ingest_pcap %d: %s",
                                    tenant, r.status_code, r.text[:200])
                    total_chunks += 1
                    total_bytes += len(chunk)
                except Exception as e:  # noqa: BLE001
                    log.warning("[%s] chunk send error: %s", tenant, e)
                idx += 1
                await asyncio.sleep(PCAP_CHUNK_GAP_SEC)

        log.info("[%s] DONE pcap: chunks=%d bytes=%d",
                 tenant, total_chunks, total_bytes)


# ---------------------------------------------------------------------------
async def main() -> None:
    if SIM_MODE == "pcap":
        log.info("SIM_MODE=pcap: using /ingest_pcap + flow_extractor path")
        # Pre-convert any pcapng captures once, serialised, before the tenants
        # start. If three tenants share one Friday-WorkingHours.pcap we don't
        # want them racing on editcap for the same output path.
        seen: set[Path] = set()
        for t in TENANTS:
            for p in _pcaps_for_tenant(t):
                if p in seen:
                    continue
                seen.add(p)
                try:
                    _ensure_classic_pcap(p)
                except Exception as e:  # noqa: BLE001
                    log.error("[pcap] pre-convert failed for %s: %s", p, e)
        await asyncio.gather(*[run_tenant_pcap(t) for t in TENANTS])
    else:
        log.info("SIM_MODE=csv: using /ingest (pre-extracted flow rows)")
        await asyncio.gather(*[
            run_tenant(t, PRESETS[i % len(PRESETS)])
            for i, t in enumerate(TENANTS)
        ])


if __name__ == "__main__":
    asyncio.run(main())
