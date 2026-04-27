"""Lightweight `docker stats` sampler for the E5 resource-footprint run.

Calls ``docker stats --no-stream --format ...`` once per ``interval``
seconds and aggregates per-container CPU and memory across the run.
This is intentionally crude (subprocess shells out, parses %-suffixed
strings) but it's enough to eyeball whether Kafka mode keeps the
detector / fan-out cooler than the Direct-HTTP equivalent.
"""

from __future__ import annotations

import asyncio
import logging
import re
import shutil
import statistics
import subprocess
from dataclasses import dataclass, field

log = logging.getLogger("docker_stats")

_FMT = "{{.Name}};{{.CPUPerc}};{{.MemUsage}}"


@dataclass
class ContainerSample:
    cpu_percent: list[float] = field(default_factory=list)
    rss_mib: list[float] = field(default_factory=list)


def _parse_cpu(s: str) -> float | None:
    m = re.search(r"([0-9.]+)%", s)
    return float(m.group(1)) if m else None


def _parse_mem(s: str) -> float | None:
    """e.g. '123.4MiB / 7.7GiB' -> 123.4 (MiB)."""
    m = re.match(r"\s*([0-9.]+)\s*([KMG]i?B)\s*/", s)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2).upper()
    if unit.startswith("K"):
        return val / 1024
    if unit.startswith("M"):
        return val
    if unit.startswith("G"):
        return val * 1024
    return val


def _take_sample(name_filter: list[str]) -> dict[str, tuple[float, float]]:
    if shutil.which("docker") is None:
        return {}
    try:
        res = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", _FMT],
            capture_output=True, text=True, timeout=10.0,
        )
    except subprocess.TimeoutExpired:
        return {}
    out: dict[str, tuple[float, float]] = {}
    for line in res.stdout.splitlines():
        parts = line.split(";")
        if len(parts) != 3:
            continue
        name, cpu_s, mem_s = parts
        if name_filter and name not in name_filter:
            continue
        cpu = _parse_cpu(cpu_s)
        mem = _parse_mem(mem_s)
        if cpu is not None and mem is not None:
            out[name] = (cpu, mem)
    return out


async def sample_loop(
    container_names: list[str],
    interval: float,
    duration: float,
) -> dict[str, ContainerSample]:
    samples: dict[str, ContainerSample] = {n: ContainerSample() for n in container_names}
    end = asyncio.get_event_loop().time() + duration
    while asyncio.get_event_loop().time() < end:
        loop = asyncio.get_event_loop()
        snapshot = await loop.run_in_executor(None, _take_sample, container_names)
        for name, (cpu, mem) in snapshot.items():
            samples.setdefault(name, ContainerSample()).cpu_percent.append(cpu)
            samples[name].rss_mib.append(mem)
        await asyncio.sleep(interval)
    return samples


def summarize(samples: dict[str, ContainerSample]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for name, s in samples.items():
        if not s.cpu_percent:
            continue
        out[name] = {
            "cpu_mean": statistics.fmean(s.cpu_percent),
            "cpu_max": max(s.cpu_percent),
            "rss_mean_mib": statistics.fmean(s.rss_mib),
            "rss_max_mib": max(s.rss_mib),
            "samples": len(s.cpu_percent),
        }
    return out
