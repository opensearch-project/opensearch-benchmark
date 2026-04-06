---
layout: default
title: gRPC Support
parent: OSB
nav_order: 20
---

# gRPC Support

OpenSearch Benchmark supports benchmarking OpenSearch clusters via gRPC transport using the `--grpc-target-hosts` option. gRPC support requires the [transport-grpc](https://github.com/opensearch-project/OpenSearch/tree/main/modules/transport-grpc) module to be enabled on the OpenSearch cluster.

## Compatibility Matrix

| OpenSearch Benchmark Version | OpenSearch Version | opensearch-protobufs Version | Notes |
|------------------------------|-------------------|------------------------------|-------|
| 2.0.0                        | 3.3, 3.4          | 0.19.0                       | Initial gRPC support |
| 2.1.0                        | 3.5+              | 1.2.0                        | Updated for OpenSearch 3.5 protobuf changes |

## Supported Operations

- Bulk ingestion
- Match queries
- Term queries
- KNN queries

## Usage

To benchmark with gRPC, specify both REST and gRPC endpoints:

```bash
opensearch-benchmark run \
    --target-hosts=localhost:9200 \
    --grpc-target-hosts=localhost:9401 \
    --workload=http_logs \
    --test-procedure=grpc-append-no-conflicts-index-only
```

The `--target-hosts` option is still required for cluster management operations (index creation, health checks, etc.), while `--grpc-target-hosts` is used for bulk ingestion and search operations when available.

## Workload Support

Not all workloads have gRPC test procedures defined. Check the workload documentation for gRPC-specific test procedures (typically prefixed with `grpc-`).
