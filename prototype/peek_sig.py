"""One-shot peek at tenant.*.signature topics — prints row counts and a
sample message per tenant. Intended to run inside a python:3.12-slim
container on the nidsaas Docker network.

Usage:
    docker run --rm --network prototype_nidsaas \
      -v $PWD/peek_sig.py:/peek.py:ro \
      python:3.12-slim bash -c "pip install -q aiokafka && python /peek.py"
"""
import asyncio
import json
import time

from aiokafka import AIOKafkaConsumer


async def peek() -> None:
    topics = [
        "tenant.acme.signature",
        "tenant.globex.signature",
        "tenant.initech.signature",
    ]
    c = AIOKafkaConsumer(
        *topics,
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        group_id=f"peek-sig-{time.time()}",
        consumer_timeout_ms=8000,
    )
    await c.start()
    counts: dict[str, int] = {}
    samples: dict[str, dict] = {}
    try:
        async for m in c:
            counts[m.topic] = counts.get(m.topic, 0) + 1
            if m.topic not in samples:
                samples[m.topic] = json.loads(m.value)
    finally:
        await c.stop()

    print("=== row counts ===")
    for t, n in sorted(counts.items()):
        print(f"{t}: {n} rows")
    print()
    print("=== samples ===")
    for t, s in samples.items():
        print(f"--- {t} ---")
        print(json.dumps(s, indent=2))


if __name__ == "__main__":
    asyncio.run(peek())
