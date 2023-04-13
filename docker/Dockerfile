################################################################################
# Build stage 0 `builder`:
# Install OpenSearch Benchmark from source inside a virtualenv
################################################################################

FROM python:3.11.2-slim as builder

RUN apt-get -y update && \
    apt-get install -y curl git gcc && \
    apt-get -y upgrade && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opensearch-benchmark/
COPY ./setup.py /opensearch-benchmark/
COPY ./setup.cfg /opensearch-benchmark/
COPY ./version.txt /opensearch-benchmark/
COPY ./README.md /opensearch-benchmark/
COPY ./MANIFEST.in /opensearch-benchmark/
COPY ./osbenchmark/ /opensearch-benchmark/osbenchmark/

RUN python3 -m venv /opensearch-benchmark/venv
ENV PATH="/opensearch-benchmark/venv/bin:$PATH"

WORKDIR /opensearch-benchmark
# Wipe away any lingering caches, copied over from the local machine
RUN find ./opensearch-benchmark -name '__pycache__' -o -name '*.pyc' | xargs rm -f
RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install /opensearch-benchmark

################################################################################
# Build stage 1 (the actual OpenSearch Benchmark image):
# Copy OpenSearch Benchmark from stage 0 and fix permissions to support randomized UIDs
# Define VOLUME for ~/.benchmark
################################################################################

FROM python:3.11.2-slim

ENV BENCHMARK_RUNNING_IN_DOCKER True

RUN apt-get -y update && \
    apt-get install -y curl git pbzip2 pigz && \
    apt-get -y upgrade && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 opensearch-benchmark && \
    useradd -d /opensearch-benchmark -m -k /dev/null -g 1000 -N -u 1000 -l -s /bin/bash benchmark

COPY --chown=1000:0 --from=builder /opensearch-benchmark/venv /opensearch-benchmark/venv

WORKDIR /opensearch-benchmark

RUN mkdir -p /opensearch-benchmark/.benchmark && \
    chown -R 1000:0 /opensearch-benchmark/.benchmark

USER 1000

ENV PATH=/opensearch-benchmark/venv/bin:$PATH

ARG VERSION
ARG BUILD_DATE

LABEL org.label-schema.schema-version="1.0" \
  org.label-schema.vendor="OpenSearch-Project" \
  org.label-schema.name="opensearch-benchmark" \
  org.label-schema.url="https://opensearch.org/" \
  org.label-schema.version="$VERSION" \
  org.label-schema.license="Apache-2.0" \
  org.label-schema.description="A community driven, open source project to run performance tests for OpenSearch" \
  org.label-schema.build-date="$BUILD_DATE" \
  org.label-schema.vcs-url="https://github.com/opensearch-project/OpenSearch-Benchmark"

VOLUME ["/opensearch-benchmark/.benchmark"]

ENTRYPOINT [ "opensearch-benchmark" ]