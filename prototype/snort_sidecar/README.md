# Snort Sidecar

## Role in the stack

The Snort 3 sidecar replays pcap files through Snort in file-mode (daq_file) to generate signature-based detections. For each tenant with pcaps in `/pcaps/<tenant>/`, the sidecar runs Snort, parses alerts from the `alert_fast.txt` file, and publishes matches to `tenant.{u}.signature` so the detector can use them as a Tier-1 fast-path signal.

```
/pcaps/<tenant>/*.pcap ---\
                           [Snort 3 --daq file] --> alert_fast.txt
                                                         |
                                                   [Parse + Publish]
                                                         |
                                              Kafka tenant.{u}.signature
```

## Files

| File | Purpose |
|------|---------|
| `sidecar.py` | Main daemon. Spawns one async loop per tenant. For each tenant with pcaps, runs `snort --daq file` on each pcap and publishes parsed alerts to Kafka. Gracefully idles if pcap dir is missing. |
| `requirements.txt` | Python dependencies: `aiokafka`. |
| `Dockerfile` | Multi-stage build. Ubuntu 22.04 stage compiles Snort 3 + LibDAQ from source. Python 3.12-slim runtime stage copies binaries and community rules. Build takes ~10 min on first run. |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address. |
| `TENANTS` | `acme,globex,initech` | Comma-separated tenant list. Sidecar expects pcap dirs at `/pcaps/<tenant>/`. |
| `SNORT_PCAP_DIR` | `/pcaps` | Root directory where per-tenant pcap folders live. |
| `SNORT_BIN` | `/opt/snort3/bin/snort` | Path to compiled Snort 3 binary (inside the container). |
| `SNORT_RULES` | `/opt/snort3/etc/snort/snort.lua` | Snort config file (Lua format). |
| `SNORT_EXTRA_RULES` | `/rules/snort3-community.rules` | Community rule file to load. |

## How it runs

Docker Compose starts the sidecar after topic_init completes:

```yaml
snort_sidecar:
  build: ./snort_sidecar
  depends_on:
    topic_init:
      condition: service_completed_successfully
  env_file: .env
  volumes:
    - ../pcap_CIC_IDS2017:/pcaps:ro
  networks: [nidsaas]
  restart: unless-stopped
```

The sidecar runs indefinitely (restart: unless-stopped). If a tenant's pcap dir is missing, that tenant loop idles (sleeps for 3600s in a loop). If pcaps are present, they are replayed in sorted order at wall-clock pace.

## What it does (behavior walkthrough)

1. **Startup** (`main()`):
   - Creates a global `AIOKafkaProducer` with 30 retry attempts (2s backoff).
   - Registers SIGTERM and SIGINT handlers to set a `stop` event.
   - Spawns one `tenant_loop(tenant, producer)` coroutine for each tenant and awaits them all.

2. **Per-tenant loop** (`tenant_loop(tenant, producer)`):
   - Checks if `/pcaps/<tenant>` exists.
   - If not, logs "idle" and sleeps forever (no-op).
   - If yes, creates an `alert_fast.txt` file in the pcap directory.
   - Spawns two concurrent tasks: `publisher()` and `runner()`.

3. **Alert publisher** (`publisher()` inside tenant_loop):
   - Calls `tail(alerts_path)` to open the alert_fast.txt file and stream new lines as they appear.
   - For each line, matches it against `ALERT_RE` (regex for snort3 alert_fast format).
   - On match, extracts: timestamp, GID, SID, message, protocol, src/dst IP, src/dst port.
   - Constructs a flow_id from src:sport-dst:dport/protocol.
   - Publishes to `tenant.{u}.signature` a JSON payload with flow_id, sigma_s=1, SID, GID, and message.

4. **Pcap replayer** (`runner()` inside tenant_loop):
   - Finds all `*.pcap` and `*.pcapng` files in the tenant dir (sorted).
   - For each pcap, calls `run_snort_for(tenant, pcap, alerts_path)`.
   - `run_snort_for()` spawns a subprocess: `snort -c <rules.lua> -R <community.rules> --daq file -r <pcap> -A alert_fast -l <tmpdir> -q`.
   - Waits for Snort to exit. Logs non-zero exit codes (non-fatal; continues to next pcap).

5. **Shutdown**: On SIGTERM, the stop event is set; producer is stopped and task exits.

## Interfaces

### Inbound

- **Filesystem `/pcaps/<tenant>/`**
  - Expected to contain `.pcap` or `.pcapng` files.
  - Each file is replayed through Snort in `--daq file` mode.
  - If the directory is missing, the sidecar idles gracefully.

### Outbound

- **Kafka topic `tenant.{u}.signature`**
  - Payload (JSON):
    ```json
    {
      "tenant": "acme",
      "flow_id": "192.168.1.1:1234-10.0.0.1:443/TCP",
      "sigma_s": 1,
      "sid": 2019973,
      "gid": 1,
      "msg": "GPL SHELLCODE x86 setreuid shell attempt",
      "proto": "TCP",
      "src": "192.168.1.1",
      "sport": "1234",
      "dst": "10.0.0.1",
      "dport": "443",
      "snort_ts": "04/25-12:00:00.123456",
      "publish_ts": 1234567890.456
    }
    ```

## Running standalone (for debugging)

Requires a compiled Snort 3 binary and Kafka:

```bash
# From inside the container (after build):
export KAFKA_BOOTSTRAP=localhost:19092
export SNORT_PCAP_DIR=/pcaps
export TENANTS=acme
python sidecar.py
```

Or build and run:

```bash
docker build -t nidsaas-snort .
docker run -e KAFKA_BOOTSTRAP=localhost:19092 \
           -e TENANTS=acme \
           -v /path/to/pcaps:/pcaps:ro \
           nidsaas-snort
```

To test Snort directly (inside the container):

```bash
docker run --rm -it nidsaas-snort bash
cd /opt/snort3/bin
./snort -c /opt/snort3/etc/snort/snort.lua -R /rules/snort3-community.rules \
  --daq file -r /pcaps/acme/monday.pcap -A alert_fast -l /tmp
cat /tmp/alert_fast.txt
```

## Logs you should see when it's healthy

```
2025-04-25 12:00:00 [snort] snort sidecar ready; tenants=['acme', 'globex', 'initech']
2025-04-25 12:00:01 [snort] [acme] snort running on monday.pcap
2025-04-25 12:00:05 [snort] [acme] published alert: flow_id=192.168.1.1:1234-10.0.0.1:443/TCP sid=2019973
2025-04-25 12:00:06 [snort] [globex] no pcaps mounted at /pcaps/globex; idle
2025-04-25 12:00:07 [snort] [initech] snort running on wednesday.pcap
```

## Common problems

1. **Pcap directory missing (tenant idles)**
   - This is expected and safe. The sidecar quietly idles if `/pcaps/<tenant>/` does not exist.
   - To populate pcaps: `mkdir -p /pcaps/<tenant> && cp *.pcap /pcaps/<tenant>/`
   - Restart the sidecar: `docker restart nidsaas_snort_sidecar`

2. **Snort exits with non-zero return code**
   - Check logs: `docker logs nidsaas_snort_sidecar | grep "snort exited rc="`
   - Verify the pcap file is not corrupted: `tcpdump -r /pcaps/<tenant>/test.pcap | head`
   - Verify Snort rules are syntactically correct: manually run snort with `-c /opt/snort3/etc/snort/snort.lua`

3. **No alerts generated (publisher running but no messages)**
   - Snort may not have rules that match the pcap.
   - Manually test: `docker exec nidsaas_snort_sidecar snort -c /opt/snort3/etc/snort/snort.lua -R /rules/snort3-community.rules --daq file -r /pcaps/acme/monday.pcap -A alert_fast -l /tmp && cat /tmp/alert_fast.txt`
   - If no output, the pcap traffic does not trigger any rules.

4. **Kafka not reachable**
   - Check `KAFKA_BOOTSTRAP` env var and verify Kafka is running: `docker logs nidsaas_kafka`
   - Container retries 30 times before failing.

5. **alert_fast.txt not created or file stuck**
   - Verify Snort wrote output: `ls -la /pcaps/<tenant>/`
   - Check if the file is in the right place (Snort uses `-l` to specify output dir).
   - Manually check Snort command for errors in the logs.

## Build caveats

The multi-stage Dockerfile takes ~10 minutes to compile Snort 3 on the first build. Subsequent builds reuse cached layers:

- **Build stage**: Ubuntu 22.04, installs dev tools, clones and compiles LibDAQ + Snort 3 from git, downloads community rules.
- **Runtime stage**: Python 3.12-slim, installs only runtime dependencies (libpcap, libpcre2, libssl, etc.), copies compiled binaries and rules from build stage.

If you modify `sidecar.py` but not the Dockerfile or requirements, only the Python layer rebuilds (fast). If you modify the Snort build command, the build stage re-runs (~10 min).
