# NIDSaaS load-test harness

This harness drives the `nidsaas_gateway` at controlled rates and joins
gateway-side send timestamps with `webhook_receiver`-side arrival
timestamps to produce per-experiment latency / throughput / loss
statistics. It is the empirical backing for the paper's claim that
Kafka-mediated transport beats a Direct-HTTP fan-out baseline at
multi-tenant SaaS scale.

The whole point of the harness is to compare two transport modes that
share **identical** business logic (gateway auth, dedup; CFM extraction;
Snort 3 detection engine; cascade scoring; webhook delivery). Only the
edges between services differ:

| Mode          | Gateway → extractor / snort | Sidecar → detector | Detector → fan-out |
|---------------|------------------------------|--------------------|---------------------|
| `kafka`       | `tenant.{u}.pcap_chunks`     | `tenant.{u}.raw` / `tenant.{u}.signature` | `tenant.{u}.alerts` |
| `direct_http` | POST `/process_chunk`        | POST `/score_flow` / `/signature_hit` | POST `/alert` |

Switch with `INGEST_MODE=kafka|direct_http` in `.env` (followed by
`docker compose up -d --no-deps gateway` to take effect).

## What gets measured

Each load run uniquely tags every gateway POST with `X-Trace-Id`. The
trace id propagates through the pipeline (kafka headers in kafka mode,
HTTP headers in direct_http mode) and is recorded by the webhook
receiver alongside the arrival timestamp. After the run, the harness
joins the two sides:

- **end-to-end latency** = `webhook_receive_ts - gateway_send_ts`
- **success rate** = % of trace ids that reached the webhook receiver
- **per-tenant breakdown** = above, grouped by tenant
- **first-vs-last verdict latency** for pcap chunks (chunks fan out to
  many flow records, so we report both the time-to-first-verdict and
  time-to-last-verdict)

## Experiments

`run_experiment.py` exposes three scenarios that map to the paper's
empirical sections:

- **E1 throughput sweep**: ramp gateway POST rate per tenant from a low
  floor to the saturation knee, record P50 / P95 / P99 latency at each
  step. Both modes are run back-to-back so the comparison is paired.
- **E2 noisy neighbour**: hold tenant-A at a steady rate; ramp tenant-B
  from idle to overload; report the latency degradation observed by
  tenant-A. The Kafka mode's per-tenant partition isolation should
  dominate the Direct-HTTP shared event-loop here.
- **E5 resource footprint**: run a fixed-rate workload for 60 s in each
  mode while sampling `docker stats` every second. Report mean / max
  CPU + RSS for each container in each mode.

All three scenarios share the same low-level driver and reporter, so
adding E3 (horizontal scalability) and E4 (fault tolerance) later is
mostly scenario glue.

## Quickstart

The harness runs from the host (it's the simulator side of the system,
not a service). It depends only on Python 3.10+ and `httpx`.

```bash
cd prototype/loadtest
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# bring up the stack first (kafka mode by default)
( cd .. && docker compose up -d --build )

# E1 — quick smoke
python run_experiment.py e1 --mode kafka --duration 30 --rate 5
python run_experiment.py e1 --mode direct_http --duration 30 --rate 5

# Compare two finished runs
python report.py outputs/e1_kafka_*.json outputs/e1_direct_http_*.json
```

`run_experiment.py` writes `outputs/<scenario>_<mode>_<UTC>.json` with
the raw per-trace records plus a summary block. `report.py` reads N of
those files and prints a side-by-side comparison table; pair them by
scenario, not by mode.

## Mode switching cookbook

The transport mode is chosen by the gateway at request time, so flipping
the env var only needs the gateway to restart:

```bash
# kafka -> direct_http
sed -i 's/^INGEST_MODE=.*/INGEST_MODE=direct_http/' ../.env
docker compose up -d --no-deps --force-recreate gateway

# direct_http -> kafka
sed -i 's/^INGEST_MODE=.*/INGEST_MODE=kafka/' ../.env
docker compose up -d --no-deps --force-recreate gateway
```

For E5 (resource footprint) the harness automates this in-flight.
