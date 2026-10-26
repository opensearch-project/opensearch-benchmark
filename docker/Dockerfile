########################################################
# Install OpenSearch Benchmark to build a Docker image #
########################################################

# The Python base image bundles a pip release that trusts the compromised eMudhra emSign CA. Upgrade pip
# from a hash-pinned wheel before it downloads anything, so the download can't be tampered with.
ARG PINNED_PIP="pip @ https://files.pythonhosted.org/packages/3f/06/3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/pip-26.2.2-py3-none-any.whl#sha256=fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70"

#
# Stage 1: build packages and compile where needed
#
FROM python:3.11-slim-trixie AS build-stage
ARG PINNED_PIP

RUN apt-get -y update && \
    apt-get install -y curl git gcc lbzip2 pbzip2 pigz make jq && \
    apt-get -y upgrade && \
    pip install --no-cache-dir --disable-pip-version-check "$PINNED_PIP"

COPY . opensearch-benchmark

# There is no binary package currently available for yappi on ARM.
RUN cd opensearch-benchmark; \
    make build; \
    if test "`uname -m`" = aarch64; then pip wheel yappi && cp yappi-*.whl dist; fi


#
# Stage 2: create image
#
FROM python:3.11-slim-trixie AS image-stage
ARG PINNED_PIP
ENV BENCHMARK_RUNNING_IN_DOCKER=True

RUN groupadd --gid 1000 opensearch-benchmark && \
    useradd -d /opensearch-benchmark -m -k /dev/null -g 1000 -N -u 1000 -l -s /bin/bash benchmark

WORKDIR /opensearch-benchmark

RUN mkdir -p /opensearch-benchmark/.benchmark && \
    chown -R 1000:0 /opensearch-benchmark/.benchmark

COPY --from=build-stage /opensearch-benchmark/dist/*.whl ./

RUN set -ex; \
    apt-get -y update; \
    apt-get install -y git lbzip2 pbzip2; \
    apt-get -y upgrade; \
    rm -rf /var/lib/apt/lists/*; \
    pip install --no-cache-dir --disable-pip-version-check "$PINNED_PIP"; \
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
