########################################################
# Install OpenSearch Benchmark to build a Docker image #
########################################################

#
# Stage 1: build packages and compile where needed
#
FROM python:3.11.2-slim AS build-stage

RUN apt-get -y update && \
    apt-get install -y curl git gcc pbzip2 pigz make jq && \
    apt-get -y upgrade

COPY . opensearch-benchmark

# There is no binary package currently available for yappi on ARM.
RUN cd opensearch-benchmark; \
    make build; \
    if test "`uname -m`" = aarch64; then pip wheel yappi && cp yappi-*.whl dist; fi


#
# Stage 2: create image
#
FROM python:3.11.2-slim AS image-stage
ENV BENCHMARK_RUNNING_IN_DOCKER=True

RUN groupadd --gid 1000 opensearch-benchmark && \
    useradd -d /opensearch-benchmark -m -k /dev/null -g 1000 -N -u 1000 -l -s /bin/bash benchmark

WORKDIR /opensearch-benchmark

RUN mkdir -p /opensearch-benchmark/.benchmark && \
    chown -R 1000:0 /opensearch-benchmark/.benchmark

COPY --from=build-stage /opensearch-benchmark/dist/*.whl ./

RUN set -ex; \
    apt-get -y update; \
    apt-get install -y git pbzip2; \
    apt-get -y upgrade; \
    rm -rf /var/lib/apt/lists/*; \
    PIP_ONLY_BINARY=h5py pip install *.whl; \
    rm *.whl

USER 1000

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
  org.label-schema.vcs-url="https://github.com/opensearch-project/OpenSearch-Benchmark" \
  "DOCKERFILE"="https://github.com/opensearch-project/opensearch-benchmark/blob/main/docker/Dockerfile"

VOLUME ["/opensearch-benchmark/.benchmark"]

ENTRYPOINT [ "opensearch-benchmark" ]
