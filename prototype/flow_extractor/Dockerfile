# ----------------------------------------------------------------------------
# Multi-stage build: compile CICFlowMeter v4 (Java, Gradle) in a builder
# image, then ship a slim JRE runtime with a Python wrapper.
#
# CICFlowMeter v4's build.gradle uses `apply plugin: 'maven'`, which was
# removed in Gradle 7. We pin the builder to the Gradle 6.x line (the last
# major that ships the legacy `maven` plugin) so the upstream repo builds
# unpatched. JDK 8 is what CICFlowMeter v4 was originally developed against.
# ----------------------------------------------------------------------------
FROM gradle:6.9.4-jdk8 AS builder
USER root
# jnetpcap is not on Maven Central; the CICFlowMeter repo ships a bundled jar
# under jnetpcap/linux/jnetpcap-1.4.r1425/jnetpcap.jar and expects you to
# install it into the local Maven cache before running Gradle. We need `mvn`
# on the builder for that.
RUN apt-get update && apt-get install -y --no-install-recommends maven \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git config --global advice.detachedHead false \
 && git clone --depth 1 https://github.com/ahlashkari/CICFlowMeter.git .
# 1. Install the bundled jnetpcap.jar into the local Maven repo (~/.m2) so
#    Gradle can resolve org.jnetpcap:jnetpcap:1.4.1.
RUN mvn install:install-file \
        -Dfile=jnetpcap/linux/jnetpcap-1.4.r1425/jnetpcap.jar \
        -DgroupId=org.jnetpcap \
        -DartifactId=jnetpcap \
        -Dversion=1.4.1 \
        -Dpackaging=jar
# 2. Patch build.gradle so Gradle actually consults the local Maven repo
#    (upstream only lists mavenCentral + clojars).
RUN sed -i '0,/mavenCentral()/{s/mavenCentral()/mavenLocal()\n    mavenCentral()/}' build.gradle
# The upstream repo ships a Gradle wrapper but pinning gradle via image
# avoids the wrapper's external download. distZip produces
# build/distributions/CICFlowMeter-4.0.zip; we extract it here with `jar xf`
# (part of the JDK) so the runtime stage doesn't need unzip — Ubuntu 22.04's
# unzip trips a false-positive "overlapped components" zip-bomb check on
# Gradle's distZip output.
RUN gradle --no-daemon distZip \
 && cd build/distributions \
 && jar xf CICFlowMeter-4.0.zip

# ----------------------------------------------------------------------------
# Runtime: JDK 8-compiled bytecode is forward-compatible with JRE 11, so we
# stay on 11 for a smaller, more widely-mirrored runtime image.
FROM eclipse-temurin:11-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip libpcap0.8 \
    && rm -rf /var/lib/apt/lists/*

# The Ubuntu `libpcap0.8` runtime package ships libpcap.so.0.8 (a symlink to
# the real libpcap.so.1.10.1) but NOT the unversioned `libpcap.so` — that
# symlink normally comes with `libpcap-dev`. jnetpcap's libjnetpcap.so was
# compiled with `-lpcap`, so the dynamic linker resolves its NEEDED entry
# against the unversioned name. Without this symlink `ldd libjnetpcap.so`
# reports `libpcap.so => not found` and every CFM invocation dies with
# UnsatisfiedLinkError deep inside org.jnetpcap.Pcap.<clinit>. Creating the
# symlink avoids pulling in the full -dev package (~20 MB of headers we
# don't need at runtime).
RUN ln -sf /usr/lib/x86_64-linux-gnu/libpcap.so.0.8 \
           /usr/lib/x86_64-linux-gnu/libpcap.so \
 && ldconfig

COPY --from=builder /src/build/distributions/CICFlowMeter-4.0 /opt/CICFlowMeter-4.0
RUN chmod +x /opt/CICFlowMeter-4.0/bin/CICFlowMeter /opt/CICFlowMeter-4.0/bin/cfm
ENV PATH="/opt/CICFlowMeter-4.0/bin:${PATH}"

# jnetpcap's native JNI shims (libjnetpcap.so, libjnetpcap-pcap100.so) live
# under jnetpcap/linux/jnetpcap-1.4.r1425/ in the upstream repo and are NOT
# bundled by `gradle distZip` — we have to copy them into the runtime image
# ourselves. Without them CICFlowMeter dies with UnsatisfiedLinkError and
# produces no CSV (but oddly still exits rc=0, so the parent process sees
# silent failure).
#
# CRITICAL: the Gradle-generated launcher at /opt/CICFlowMeter-4.0/bin/CICFlowMeter
# hard-codes `DEFAULT_JVM_OPTS='"-Djava.library.path=../lib/native"'`, so the
# JVM resolves native libs relative to the bin/ dir — i.e. it looks in
# /opt/CICFlowMeter-4.0/lib/native/, NOT in /usr/lib/ or on LD_LIBRARY_PATH.
# Dropping the .so files anywhere else makes them invisible to the JNI loader
# and CICFlowMeter silently exits rc=0 with zero flows.
RUN mkdir -p /opt/CICFlowMeter-4.0/lib/native
COPY --from=builder /src/jnetpcap/linux/jnetpcap-1.4.r1425/libjnetpcap.so /opt/CICFlowMeter-4.0/lib/native/
COPY --from=builder /src/jnetpcap/linux/jnetpcap-1.4.r1425/libjnetpcap-pcap100.so /opt/CICFlowMeter-4.0/lib/native/

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY extractor.py .

ENV PYTHONUNBUFFERED=1
CMD ["python3", "extractor.py"]
