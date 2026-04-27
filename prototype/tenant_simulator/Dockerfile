FROM python:3.12-slim
WORKDIR /app

# wireshark-common ships `editcap`, which we use to normalise pcapng -> classic
# pcap on startup (Wireshark-resaved captures like CIC-IDS2017's Friday pcap
# are pcapng despite the .pcap extension). DEBIAN_FRONTEND avoids the package's
# setuid-prompt stalling the build.
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        wireshark-common \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "simulator.py"]
