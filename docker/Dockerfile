###############################################################################
# Install OpenSearch Benchmark from PyPI to build a Docker image
###############################################################################

FROM python:3.11.2-slim
ARG VERSION

ENV BENCHMARK_RUNNING_IN_DOCKER True

RUN apt-get -y update && \
    apt-get install -y curl git gcc pbzip2 pigz && \
    apt-get -y upgrade && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 opensearch-benchmark && \
    useradd -d /opensearch-benchmark -m -k /dev/null -g 1000 -N -u 1000 -l -s /bin/bash benchmark

RUN python3 -m pip install h5py==3.10.0; if [ -z "$VERSION" ] ; then python3 -m pip install opensearch-benchmark ; else python3 -m pip install opensearch-benchmark==$VERSION ; fi

WORKDIR /opensearch-benchmark

RUN mkdir -p /opensearch-benchmark/.benchmark && \
    chown -R 1000:0 /opensearch-benchmark/.benchmark

USER 1000

ARG BUILD_DATE

LABEL org.label-schema.schema-version="1.0" \
  org.label-schema.vendor="OpenSearch-Project" \
  org.label-schema.name="opensearch-benchmark" \
  org.label-schema.url="https://opensearch.org/" \
  org.label-schema.version="$VERSION" \
  org.label-schema.license="Apache-2.0" \
  org.label-schema.description="A community driven, open source project to run performance tests for OpenSearch" \
  org.label-schema.build-date="$BUILD_DATE" \
  org.label-schema.vcs-url="https://github.com/opensearch-project/OpenSearch-Benchmark" \
  "DOCKERFILE"="https://github.com/opensearch-project/opensearch-benchmark/blob/main/docker/Dockerfile"

VOLUME ["/opensearch-benchmark/.benchmark"]

ENTRYPOINT [ "opensearch-benchmark" ]
