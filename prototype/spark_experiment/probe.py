"""Latency probe — Direct Kafka vs Kafka-through-Spark.

Sends N timestamped messages to one of two Kafka paths and measures the
round-trip latency from producer.send() until the consumer reads the
message back.

  * direct path:   producer → direct.probe → consumer
  * spark  path:   producer → spark.probe.in → Spark → spark.probe.out → consumer

Both paths use the same broker and identical message size. The only
difference is whether Spark Structured Streaming sits in the middle
(with its 100 ms micro-batch trigger).

Usage::

    pip install kafka-python --break-system-packages
    python3 probe.py --mode direct --n 100
    python3 probe.py --mode spark  --n 100

  --bootstrap localhost:19092   # Kafka external listener
  --n 100                       # number of probe messages
  --rate 50                     # send rate per second (avoid bursts skewing tail)
"""
from __future__ import annotations

import argparse
import statistics
import threading
import time
import uuid
from typing import Callable, Iterable, List

from kafka import KafkaConsumer, KafkaProducer, TopicPartition


def percentiles(xs: list[float], ps: Iterable[float]) -> dict[float, float]:
    if not xs:
        return {p: float("nan") for p in ps}
    s = sorted(xs)
    out: dict[float, float] = {}
    for p in ps:
        idx = max(0, min(len(s) - 1, int(round((p / 100) * (len(s) - 1)))))
        out[p] = s[idx]
    return out


# kafka-python sometimes can't auto-detect Kafka 3.x protocol on first
# connect, raising NoBrokersAvailable. Pin the api_version to skip
# auto-detection — Kafka 3.9 is wire-compatible with 2.5+ clients.
_API_VERSION = (2, 5, 0)

# Hard timeouts so the probe fails fast instead of hanging forever
# when the bootstrap host is unreachable (e.g. flaky WSL2 port
# forwarding to localhost:19092 on Windows).
#
# Producer can use a tight request_timeout. Consumer must keep
# request_timeout_ms > session_timeout_ms (default 10s) per kafka-python.
_PROD_KW = {
    "api_version": _API_VERSION,
    "request_timeout_ms": 10_000,
    "metadata_max_age_ms": 5_000,
    "reconnect_backoff_ms": 500,
    "reconnect_backoff_max_ms": 2_000,
}
_CONS_KW = {
    "api_version": _API_VERSION,
    "request_timeout_ms": 30_000,   # > session_timeout_ms (default 10s)
    "session_timeout_ms": 10_000,
    "metadata_max_age_ms": 5_000,
    "reconnect_backoff_ms": 500,
    "reconnect_backoff_max_ms": 2_000,
}


def _make_consumer_at_end(bootstrap: str, topic: str,
                          consumer_timeout_ms: int) -> KafkaConsumer:
    """Build a KafkaConsumer manually-assigned to all partitions of
    `topic`, with read position pinned to current end (so it sees only
    messages produced AFTER this returns).

    We use assign()+seek() instead of subscribe()+seek_to_end() because
    the latter has racy interactions with consumer-group join in
    kafka-python 2.3+ (subscription seems to complete but messages are
    silently skipped).
    """
    # No group_id → fully manual mode, no rebalance, no auto-commit
    cons = KafkaConsumer(
        bootstrap_servers=bootstrap,
        consumer_timeout_ms=consumer_timeout_ms,
        enable_auto_commit=False,
        **_CONS_KW)

    # Discover partitions for the topic
    parts = cons.partitions_for_topic(topic)
    deadline = time.monotonic() + 10
    while not parts:
        time.sleep(0.5)
        parts = cons.partitions_for_topic(topic)
        if time.monotonic() > deadline:
            raise RuntimeError(f"topic {topic} has no partitions visible")

    tps = [TopicPartition(topic, p) for p in parts]
    cons.assign(tps)
    # Position at END so we only see new messages
    end_offsets = cons.end_offsets(tps)
    for tp in tps:
        cons.seek(tp, end_offsets[tp])
    print(f"  [setup] consumer pinned to {topic} "
          f"({len(tps)} partition(s), starting offsets={list(end_offsets.values())})")
    return cons


def _collect_in_thread(cons: KafkaConsumer, n: int, deadline_sec: float,
                        parser: Callable[[str], int], stop_event: threading.Event
                        ) -> List[float]:
    """Poll the consumer continuously and append (now − sent_ns) ms to
    `latencies` for each message. Runs in a background thread so it
    overlaps with the producer. Without this, the producer would finish
    sending all N messages before the consumer ever polls, and the
    measured "latency" would just reflect collection delay, not real
    transport time."""
    latencies: List[float] = []
    deadline = time.monotonic() + deadline_sec
    while (len(latencies) < n
           and not stop_event.is_set()
           and time.monotonic() < deadline):
        records = cons.poll(timeout_ms=200)
        for tp, msgs in records.items():
            for msg in msgs:
                try:
                    sent_ns = parser(msg.value.decode())
                except Exception:
                    continue
                latencies.append((time.time_ns() - sent_ns) / 1e6)
                if len(latencies) >= n:
                    break
            if len(latencies) >= n:
                break
    return latencies


def _run_probe(bootstrap: str, n: int, rate: float,
               in_topic: str, out_topic: str,
               parser: Callable[[str], int],
               consumer_timeout_ms: int,
               deadline_sec: float,
               label: str) -> List[float]:
    """Generic probe runner: spin up a consumer-pinned-to-end thread,
    then have the main thread send N messages at `rate` rps to
    `in_topic`. Wait for the collector to finish, return latencies."""
    prod = KafkaProducer(bootstrap_servers=bootstrap, acks="all",
                         linger_ms=0, **_PROD_KW)
    cons = _make_consumer_at_end(bootstrap, out_topic,
                                 consumer_timeout_ms=consumer_timeout_ms)

    latencies: List[float] = []
    stop = threading.Event()

    def runner():
        nonlocal latencies
        latencies = _collect_in_thread(cons, n, deadline_sec, parser, stop)

    t = threading.Thread(target=runner, daemon=True)
    t.start()

    # Send messages at the requested rate. Each message records its
    # send timestamp in the value so the consumer can compute latency.
    interval = 1.0 / max(rate, 1e-3)
    next_at = time.monotonic()
    for i in range(n):
        now = time.monotonic()
        if now < next_at:
            time.sleep(next_at - now)
        next_at += interval

        sent_ns = time.time_ns()
        key = f"{label[0]}-{i}".encode()
        prod.send(in_topic, key=key, value=str(sent_ns).encode())
    prod.flush()
    print(f"[{label}] sent {n} messages — collecting...")

    # wait for collector to finish or timeout
    t.join(timeout=deadline_sec + 5)
    stop.set()

    if len(latencies) < n:
        print(f"  [{label}] collected {len(latencies)}/{n}")

    prod.close()
    cons.close()
    return latencies


def run_direct(bootstrap: str, n: int, rate: float) -> list[float]:
    """producer → direct.probe → consumer (same topic, no Spark)."""
    return _run_probe(
        bootstrap, n, rate,
        in_topic="direct.probe", out_topic="direct.probe",
        parser=lambda v: int(v),
        consumer_timeout_ms=15_000,
        deadline_sec=30,
        label="direct")


def run_spark(bootstrap: str, n: int, rate: float) -> list[float]:
    """producer → spark.probe.in → Spark → spark.probe.out → consumer."""
    return _run_probe(
        bootstrap, n, rate,
        in_topic="spark.probe.in", out_topic="spark.probe.out",
        parser=lambda v: int(v.split("|spark_ts=")[0]),
        consumer_timeout_ms=30_000,
        deadline_sec=120,
        label="spark")


def report(name: str, lat: list[float]) -> None:
    if not lat:
        print(f"  {name}: no samples received")
        return
    pcts = percentiles(lat, [50, 95, 99])
    mean = statistics.fmean(lat)
    print(f"  {name:8s}  n={len(lat):4d}  "
          f"mean={mean:7.1f}ms  "
          f"p50={pcts[50]:7.1f}ms  "
          f"p95={pcts[95]:7.1f}ms  "
          f"p99={pcts[99]:7.1f}ms  "
          f"max={max(lat):7.1f}ms")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["direct", "spark", "both"],
                    default="both")
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--rate", type=float, default=50.0,
                    help="send rate (messages/sec)")
    args = ap.parse_args()

    print(f"Latency probe — bootstrap={args.bootstrap}  "
          f"n={args.n}  rate={args.rate}/s")
    print()

    if args.mode in ("direct", "both"):
        lat_direct = run_direct(args.bootstrap, args.n, args.rate)
        report("direct", lat_direct)

    if args.mode in ("spark", "both"):
        lat_spark = run_spark(args.bootstrap, args.n, args.rate)
        report("spark",  lat_spark)

    if args.mode == "both" and lat_direct and lat_spark:
        d_p50 = percentiles(lat_direct, [50])[50]
        s_p50 = percentiles(lat_spark,  [50])[50]
        d_p99 = percentiles(lat_direct, [99])[99]
        s_p99 = percentiles(lat_spark,  [99])[99]
        print()
        print(f"  Δ p50: {s_p50/d_p50:6.1f}× slower  ({d_p50:.1f} → {s_p50:.1f}\\,ms)")
        print(f"  Δ p99: {s_p99/d_p99:6.1f}× slower  ({d_p99:.1f} → {s_p99:.1f}\\,ms)")


if __name__ == "__main__":
    main()
