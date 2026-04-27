# flow_extractor

## Role in the stack

Closes the "tenant sends raw traffic; service extracts flows" loop in the
paper's Figure 1. In CSV mode this service is unused (the tenant
simulator sends pre-extracted flow rows straight to `/ingest`). In pcap
mode the tenant simulator instead POSTs pcap chunks to `/ingest_pcap`,
the gateway forwards them to `tenant.{u}.pcap_chunks`, and this
sidecar extracts flows with CICFlowMeter v4 and republishes to
`tenant.{u}.raw` — the exact topic the detector already consumes.

```
tenant.{u}.pcap_chunks  -->  [flow_extractor + CICFlowMeter v4]  -->  tenant.{u}.raw
```

## Files

| File | Purpose |
|---|---|
| `Dockerfile` | Multi-stage: Gradle/JDK-11 builder compiles CICFlowMeter v4 from source; runtime image is `eclipse-temurin:11-jre-jammy` + Python. First build is ~8 min (Gradle downloads the dependency graph); subsequent builds are cached. |
| `extractor.py` | aiokafka consumer → CICFlowMeter subprocess → aiokafka producer. |
| `requirements.txt` | Python deps: `aiokafka`, `pandas`, `numpy`. |
| `README.md` | This file. |

## Environment variables

| Var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Broker URI inside the compose network. |
| `TENANTS` | `acme,globex,initech` | Tenants whose `pcap_chunks` topics this sidecar subscribes to. |
| `CFM_BIN` | `/opt/CICFlowMeter-4.0/bin/cfm` | Path to the CICFlowMeter CLI launcher (`cic.cs.unb.ca.ifm.Cmd`). The sibling `bin/CICFlowMeter` script is the Swing GUI entry and exits silently with rc=0 when no DISPLAY. |
| `CFM_TIMEOUT_SEC` | `180` | Per-chunk extraction timeout. |
| `CFM_MAX_CHUNK_BYTES` | `16777216` (16 MB) | Must be ≥ broker `message.max.bytes` and ≥ gateway `/ingest_pcap` limit. |

## How it runs

The compose `flow_extractor` block depends on `topic_init` and
`kafka`. It restarts on failure (`restart: unless-stopped`). No ports
are exposed; it only talks to Kafka.

## Behavior walkthrough (extractor.py)

1. `main()` builds one consumer subscribed to all
   `tenant.{u}.pcap_chunks` topics and one producer.
2. For each incoming message:
   - Extracts `tenant` from the topic name.
   - Reads message headers `chunk_id` / `pcap_file` (set by the
     gateway when it forwarded the chunk).
   - Writes the binary body to a temp pcap file inside a
     `TemporaryDirectory`.
3. `_run_cfm_sync()` shells out to CICFlowMeter in a thread-pool executor
   so the main asyncio loop stays responsive.
4. CICFlowMeter writes `<pcap>_Flow.csv` next to the input. The sidecar
   reads it with pandas and, for each row, publishes one flow record to
   `tenant.{u}.raw` using the same JSON schema the FastAPI gateway
   produces on `/ingest`.
5. Temp dir is cleaned up automatically per chunk.

## Interfaces

### Inbound

Topic: `tenant.{u}.pcap_chunks`

Message value: raw pcap bytes (PCAP global header + packet records).

Message headers:

| Header | Meaning |
|---|---|
| `chunk_id` | Client-assigned string, echoed into the flow record. |
| `pcap_file` | Original pcap filename, for traceability. |
| `tenant` | Tenant ID (redundant with topic). |

Partition key: `chunk_id`.

### Outbound

Topic: `tenant.{u}.raw` (same as CSV mode).

Payload:

```json
{
  "flow_id": "192.168.10.5-52.1.2.3-443-51234-6",
  "source_file": "Monday-WorkingHours.pcap",
  "row_id": 42,
  "label": null,
  "features": { "Destination Port": 443, "Flow Duration": 12345, ... },
  "tenant": "acme",
  "ingest_ts": 1714000000.123,
  "extractor": "CICFlowMeter-4.0",
  "chunk_id": "Monday-00007"
}
```

Note `label` is always `null` — pcap mode has no ground-truth labels.
The detector's metrics-from-labels comparison only applies in CSV mode.

## Running standalone (debugging)

```bash
docker compose up --build flow_extractor
docker compose logs -f flow_extractor
```

You can also hand-feed a chunk with kafkacat from the host:

```bash
kcat -b localhost:19092 -t tenant.acme.pcap_chunks \
     -P -H chunk_id=manual-0 -H pcap_file=smoke.pcap \
     -k manual-0 < ./test-fixtures/small.pcap
```

## Logs you should see when healthy

```
flow_extractor consumer started
flow_extractor producer started
flow_extractor ready | tenants=['acme','globex','initech'] | cfm=/opt/CICFlowMeter-4.0/bin/CICFlowMeter | max_chunk=16777216
[acme] chunk=Monday-WorkingHours-00000 bytes=4982312 flows=1847 elapsed=4.3s
[acme] chunk=Monday-WorkingHours-00001 bytes=4991024 flows=1912 elapsed=4.1s
```

## Common problems

| Symptom | Fix |
|---|---|
| Gradle build dies with "Could not resolve ..." during first image build | Network hiccup. Rerun `docker compose build flow_extractor`. The gradle cache is in the builder stage; failure leaves the runtime image unbuilt but no state persists. |
| `CFM rc=1 stderr=... UnsatisfiedLinkError: libpcap` | Runtime image lost `libpcap0.8`. Rebuild. The Dockerfile installs it from Ubuntu Jammy repos. |
| `no flow csv from N byte chunk` | Pcap chunk was truncated mid-packet (simulator cut bytes without preserving the per-packet header). The bundled splitter in `tenant_simulator/simulator.py` slices on packet boundaries; don't replace it with naive byte slicing. |
| `CFM timeout after 180s` | Chunk too large or pathological traffic (many long flows). Raise `CFM_TIMEOUT_SEC` or lower `SIM_PCAP_PACKETS_PER_CHUNK` in `.env`. |
| Kafka error `MessageSizeTooLarge` | Broker `message.max.bytes` lower than chunk size. Compose sets it to 16 MB; verify with `docker exec nidsaas_kafka kafka-configs.sh --bootstrap-server kafka:9092 --entity-type brokers --entity-name 1 --describe`. |
| Detector predicts garbage on pcap-mode flows | CICFlowMeter column names must match RF training exactly. If you swapped to a different extractor (nfstream, port), expect this. Stick with CICFlowMeter v4. |
