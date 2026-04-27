# Snort 3 sidecar container.
#
# Builds Snort 3 + LibDAQ from source (same steps as the repo README), then
# ships a tiny Python daemon that replays pcaps through snort --daq file and
# publishes parsed alerts to tenant.{u}.signature.
#
# Build takes ~10 min on a first run; layers cache afterwards.

FROM ubuntu:22.04 AS build

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake git pkg-config flex bison libfl-dev \
        libpcap-dev libpcre2-dev zlib1g-dev libdumbnet-dev \
        libluajit-5.1-dev libssl-dev libhwloc-dev \
        autotools-dev libtool liblzma-dev \
        ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# LibDAQ
RUN git clone --depth 1 https://github.com/snort3/libdaq.git \
 && cd libdaq \
 && ./bootstrap \
 && ./configure --prefix=/usr/local/lib/daq_s3 \
 && make -j"$(nproc)" && make install

# Snort 3
RUN git clone --depth 1 https://github.com/snort3/snort3.git \
 && cd snort3 \
 && ./configure_cmake.sh \
        --prefix=/opt/snort3 \
        --with-daq-includes=/usr/local/lib/daq_s3/include/ \
        --with-daq-libraries=/usr/local/lib/daq_s3/lib/ \
 && cd build && make -j"$(nproc)" && make install

# Community rules
RUN mkdir -p /rules && cd /rules \
 && wget -q https://www.snort.org/downloads/community/snort3-community-rules.tar.gz \
 && tar xzf snort3-community-rules.tar.gz --strip-components=1 \
 && rm snort3-community-rules.tar.gz

# Smoke-test rules: match any IP/TCP/UDP packet. Keeps σ_S hot even when
# the community ruleset has no content-based signature for the replayed
# pcap (CIC-IDS2017 attacks are behavioral — DDoS LOIC, PortScan, Botnet —
# and do not trigger snort3-community's CVE/malware-specific rules by
# design). These rules exist so the prototype can demonstrate σ_S
# end-to-end wiring and can be removed in a production ruleset.
#
# NOTE: Snort 3 REQUIRES at least one detection option (content, dsize,
# ttl, flow, ...) — a "bare" rule with just sid/rev loads but never fires
# ("WARNING: does not have any detection options"). dsize:>0 covers data
# packets; ttl:>0 covers every IP packet including bare handshake/ACKs.
RUN cat >> /rules/snort3-community.rules <<'EOF'
alert ip  any any -> any any (msg:"NIDSAAS_TEST_ANY_IP";  sid:9000001; rev:1; ttl:>0;)
alert tcp any any -> any any (msg:"NIDSAAS_TEST_ANY_TCP"; sid:9000002; rev:1; dsize:>0;)
alert udp any any -> any any (msg:"NIDSAAS_TEST_ANY_UDP"; sid:9000003; rev:1; dsize:>0;)
EOF

# -----------------------------------------------------------------------------
FROM python:3.12-slim

ARG DEBIAN_FRONTEND=noninteractive
# libnuma1 is a runtime dep of snort3's hwloc integration — it is NOT pulled
# in as a hard dep of libhwloc15, so without it `snort` exits rc=127 with
# "libnuma.so.1: cannot open shared object file". Builder stage gets it via
# libhwloc-dev's recommends; runtime stage has to ask for it explicitly.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpcap0.8 libpcre2-8-0 libluajit-5.1-2 libhwloc15 libssl3 \
        libdumbnet1 zlib1g libnuma1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/snort3 /opt/snort3
COPY --from=build /usr/local/lib/daq_s3 /usr/local/lib/daq_s3
COPY --from=build /rules /rules
RUN echo '/usr/local/lib/daq_s3/lib/' > /etc/ld.so.conf.d/daq.conf && ldconfig

ENV PATH="/opt/snort3/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sidecar.py .

CMD ["python", "sidecar.py"]
