# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Milvus engine module.

Exposes the engine-registry interface for Milvus: create_client_factory,
create_async_client, register_runners, wait_for_client, on_execute_error.

Uses pymilvus's AsyncMilvusClient (grpc.aio native) for ~28% better scaling
at 32 clients vs. sync pymilvus + ThreadPoolExecutor.
"""

from osbenchmark.engine.milvus.client import MilvusClientFactory, MilvusDatabaseClient  # noqa: F401


def create_client_factory(hosts, client_options):
    """Return a MilvusClientFactory."""
    return MilvusClientFactory(hosts, client_options)


def create_async_client(hosts, client_options, cfg=None):
    """Return an async Milvus client. cfg is accepted for interface symmetry."""
    return MilvusClientFactory(hosts, client_options).create_async()


def register_runners():
    """Register Milvus runners, overriding OS defaults for shared operation types."""
    # pylint: disable=import-outside-toplevel
    from osbenchmark import workload
    from osbenchmark.worker_coordinator import runner
    from osbenchmark.engine.milvus import runners as milvus_runners

    runner.register_runner(workload.OperationType.Bulk, milvus_runners.MilvusBulkIndex(), async_runner=True)
    runner.register_runner(workload.OperationType.BulkVectorDataSet, milvus_runners.MilvusBulkVectorDataSet(), async_runner=True)
    runner.register_runner(workload.OperationType.Search, milvus_runners.MilvusQuery(), async_runner=True)
    runner.register_runner(workload.OperationType.VectorSearch, milvus_runners.MilvusVectorSearch(), async_runner=True)
    runner.register_runner(workload.OperationType.CreateIndex, milvus_runners.MilvusCreateIndex(), async_runner=True)
    runner.register_runner(workload.OperationType.DeleteIndex, milvus_runners.MilvusDeleteIndex(), async_runner=True)
    runner.register_runner(workload.OperationType.Refresh, milvus_runners.MilvusRefresh(), async_runner=True)
    runner.register_runner(workload.OperationType.ForceMerge, milvus_runners.MilvusForceMerge(), async_runner=True)
    runner.register_runner(workload.OperationType.ClusterHealth, milvus_runners.MilvusClusterHealth(), async_runner=True)
    runner.register_runner(workload.OperationType.IndexStats, milvus_runners.MilvusIndicesStats(), async_runner=True)
    runner.register_runner("warmup-knn-indices", milvus_runners.MilvusWarmupRunner(), async_runner=True)
    for op_type in [workload.OperationType.PutPipeline, workload.OperationType.DeletePipeline,
                    workload.OperationType.CreateSearchPipeline, workload.OperationType.PutSettings]:
        runner.register_runner(op_type, milvus_runners.MilvusNoOp(str(op_type)), async_runner=True)


def wait_for_client(milvus_client, max_attempts=40):
    """Wait for Milvus gRPC endpoint to become reachable.

    Uses a lightweight health check via the client's ping/list-collections path
    to avoid loading pymilvus in the coordinator process.
    """
    # pylint: disable=import-outside-toplevel
    import time
    import requests as req

    host = getattr(milvus_client, "host", None) or getattr(milvus_client, "_host", "localhost")
    port = getattr(milvus_client, "port", 19530)
    for attempt in range(max_attempts):
        try:
            # POST to REST API (Milvus 2.4+ exposes REST on same port as gRPC)
            resp = req.post(f"http://{host}:{port}/v2/vectordb/collections/list",
                            json={}, timeout=5,
                            headers={"Content-Type": "application/json"})
            if resp.status_code in (200, 400):
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def on_execute_error(e):
    """Translate pymilvus/grpc exceptions to OSB's (ops, unit, meta, fatal) tuple.

    Returns None if the exception is not engine-specific.
    """
    # pylint: disable=import-outside-toplevel
    try:
        import grpc
        if isinstance(e, grpc.RpcError):
            code = e.code() if hasattr(e, "code") else None
            fatal = code == grpc.StatusCode.UNAVAILABLE if code else False
            return 0, "ops", {
                "success": False,
                "error-type": "transport",
                "error-description": f"Milvus gRPC error: {e.details() if hasattr(e, 'details') else str(e)}",
            }, fatal
    except ImportError:
        pass

    try:
        from pymilvus.exceptions import MilvusException
        if isinstance(e, MilvusException):
            return 0, "ops", {
                "success": False,
                "error-type": "milvus",
                "error-description": str(e),
            }, False
    except ImportError:
        pass

    return None
