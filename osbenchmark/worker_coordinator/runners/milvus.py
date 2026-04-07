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
Milvus-specific runner implementations for OpenSearch Benchmark.

Timing boundaries:
- BEFORE timing: parse bodies, build params, construct schemas, extract vectors
- INSIDE timing: gRPC calls only (insert, search, create, flush, compact, load)
- AFTER timing: convert responses, calculate recall, build result dicts

All runners catch exceptions and return {"success": False} dicts. OSB framework
catches opensearchpy.TransportError, not pymilvus.MilvusException — without
this, uncaught exceptions terminate the task.
"""

import logging
import time

from osbenchmark.database.clients.milvus.helpers import (
    build_collection_schema,
    build_search_params,
    calculate_topk_recall,
    convert_milvus_search_response,
    parse_vector_body,
)
from osbenchmark import workload
from osbenchmark.worker_coordinator.runner import Runner, request_context_holder

logger = logging.getLogger(__name__)


class MilvusBulkVectorDataSet(Runner):
    """Bulk inserts vector datasets into Milvus.

    No periodic flush in the runner — Milvus 2.6 auto-flushes at segment
    buffer threshold (~16MB). The workload schedules explicit refresh
    operations between phases. Runner instances are singletons shared
    across workers — mutable state would race.
    """

    async def __call__(self, milvus_client, params):
        size = params.get("size", 0)
        body = params["body"]
        vector_field = params.get("target_field_name", "embedding")

        prepared, index = parse_vector_body(body, vector_field)
        if not prepared:
            return {"weight": size, "unit": "docs", "success": True}

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            result = await milvus_client.bulk(body=prepared, index=index)
            error_count = len(prepared) - len(result.get("items", []))
            return {
                "weight": size,
                "unit": "docs",
                "success": not result.get("errors", False),
                "error-count": error_count,
            }
        except Exception as e:
            self.logger.warning("MilvusBulkVectorDataSet failed: %s", e)
            return {"weight": size, "unit": "docs", "success": False,
                    "error-type": "milvus", "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "milvus-bulk-vector-data-set"


class MilvusBulkIndex(Runner):
    """Bulk indexes documents for generic bulk operations."""

    async def __call__(self, milvus_client, params):
        bulk_size = params.get("bulk-size", 0)
        unit = params.get("unit", "docs")
        body = params.get("body")
        index = params.get("index")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            result = await milvus_client.bulk(body=body, index=index)
            return {
                "weight": bulk_size,
                "unit": unit,
                "success": not result.get("errors", False),
            }
        except Exception as e:
            self.logger.warning("MilvusBulkIndex failed: %s", e)
            return {"weight": bulk_size, "unit": unit, "success": False,
                    "error-type": "milvus", "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "milvus-bulk-index"


class MilvusVectorSearch(Runner):
    """Vector similarity search against Milvus.

    Timing: build params BEFORE, search() INSIDE, convert + recall AFTER.
    """

    # Class-level timing accumulators (TIMING INSTRUMENTATION — REMOVE BEFORE MERGE)
    _timing_total_calls = 0
    _timing_build_us = 0.0
    _timing_search_us = 0.0
    _timing_response_conv_us = 0.0
    _timing_total_us = 0.0

    @staticmethod
    def _extract_doc_id(milvus_id):
        return str(milvus_id)

    async def __call__(self, milvus_client, params):
        import time as _time  # pylint: disable=import-outside-toplevel
        t_call_start = _time.perf_counter()

        body = params.get("body", {})
        index = params.get("index")
        k = params.get("k", 100)
        collection_name = index or getattr(milvus_client, "_collection_name", "target_index")
        client_options = getattr(milvus_client, "client_options", {})
        vector_field = params.get("target_field_name", "embedding")

        # BEFORE timing: extract vector, build params
        t_build_start = _time.perf_counter()
        query = body.get("query", {})
        knn = query.get("knn", body.get("knn", {}))
        vector = None
        anns_field = None
        if knn:
            for field_name, field_config in knn.items():
                if isinstance(field_config, dict) and "vector" in field_config:
                    vector = field_config["vector"]
                    anns_field = field_name
                    k = field_config.get("k", k)
                    break
            if vector is None:
                vector = knn.get("vector")
                anns_field = knn.get("field", vector_field)

        if vector is None:
            return {"weight": 1, "unit": "ops", "success": False, "hits": 0}

        if hasattr(vector, 'tolist'):
            vector = vector.tolist()

        search_config = build_search_params(params, client_options)
        search_body = {
            "data": [vector],
            "anns_field": anns_field,
            "limit": k,
            "output_fields": ["doc_id"],
            "search_params": search_config["search_params"],
        }
        t_build_end = _time.perf_counter()

        # INSIDE timing: gRPC search only
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        t_search_start = _time.perf_counter()
        try:
            raw_result = await milvus_client.search(index=collection_name, body=search_body)
        except Exception as e:
            self.logger.warning("MilvusVectorSearch failed: %s", e)
            return {"weight": 1, "unit": "ops", "success": False,
                    "error-type": "milvus", "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        t_search_end = _time.perf_counter()

        # AFTER timing: convert response, calculate recall
        t_resp_start = _time.perf_counter()
        response = convert_milvus_search_response(raw_result, collection_name)
        hits = response.get("hits", {}).get("total", {}).get("value", 0)
        result = {
            "weight": 1,
            "unit": "ops",
            "hits": hits,
            "hits_relation": "eq",
            "timed_out": False,
            "success": True,
        }

        if params.get("detailed-results", False):
            result["hits_total"] = hits
            result["took"] = response.get("took", 0)

        if params.get("neighbors") is not None and "k" in params:
            response_hits = response.get("hits", {}).get("hits", [])
            candidates = [self._extract_doc_id(h.get("_id", "")) for h in response_hits]
            neighbors = params["neighbors"]
            result["recall@k"] = calculate_topk_recall(candidates, neighbors, k)
            result["recall@1"] = calculate_topk_recall(candidates, neighbors, 1)
        t_resp_end = _time.perf_counter()

        # TIMING INSTRUMENTATION (REMOVE BEFORE MERGE)
        t_call_end = _time.perf_counter()
        MilvusVectorSearch._timing_total_calls += 1
        MilvusVectorSearch._timing_build_us += (t_build_end - t_build_start) * 1e6
        MilvusVectorSearch._timing_search_us += (t_search_end - t_search_start) * 1e6
        MilvusVectorSearch._timing_response_conv_us += (t_resp_end - t_resp_start) * 1e6
        MilvusVectorSearch._timing_total_us += (t_call_end - t_call_start) * 1e6
        if MilvusVectorSearch._timing_total_calls % 1000 == 0:
            n = MilvusVectorSearch._timing_total_calls
            self.logger.warning(
                "[MILVUS TIMING n=%d] build=%.2fus search=%.2fus response=%.2fus total=%.2fus overhead=%.2fus",
                n,
                MilvusVectorSearch._timing_build_us / n,
                MilvusVectorSearch._timing_search_us / n,
                MilvusVectorSearch._timing_response_conv_us / n,
                MilvusVectorSearch._timing_total_us / n,
                (MilvusVectorSearch._timing_total_us - MilvusVectorSearch._timing_search_us) / n,
            )

        return result

    def __repr__(self):
        return "milvus-vector-search"


class MilvusQuery(Runner):
    """General search queries. For vectorsearch workload, all queries use KNN."""

    async def __call__(self, milvus_client, params):
        body = params.get("body", {})
        index = params.get("index")
        collection_name = index or getattr(milvus_client, "_collection_name", "target_index")
        client_options = getattr(milvus_client, "client_options", {})
        vector_field = params.get("target_field_name", "embedding")

        query = body.get("query", {})
        knn = query.get("knn", body.get("knn", {}))
        vector = None
        anns_field = None
        if knn:
            for field_name, field_config in knn.items():
                if isinstance(field_config, dict) and "vector" in field_config:
                    vector = field_config["vector"]
                    anns_field = field_name
                    break
            if vector is None:
                vector = knn.get("vector")
                anns_field = knn.get("field", vector_field)

        if vector is None:
            return {"weight": 1, "unit": "ops", "success": True, "hits": 0}

        if hasattr(vector, 'tolist'):
            vector = vector.tolist()

        k = params.get("k", body.get("size", 100))
        search_config = build_search_params(params, client_options)
        search_body = {
            "data": [vector],
            "anns_field": anns_field,
            "limit": k,
            "output_fields": ["doc_id"],
            "search_params": search_config["search_params"],
        }

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            raw_result = await milvus_client.search(index=collection_name, body=search_body)
        except Exception as e:
            self.logger.warning("MilvusQuery failed: %s", e)
            return {"weight": 1, "unit": "ops", "success": False,
                    "error-type": "milvus", "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

        response = convert_milvus_search_response(raw_result, collection_name)
        hits = response.get("hits", {}).get("total", {}).get("value", 0)
        return {"weight": 1, "unit": "ops", "hits": hits,
                "hits_relation": "eq", "timed_out": False}

    def __repr__(self):
        return "milvus-query"


class MilvusCreateIndex(Runner):
    """Creates a Milvus collection with schema and index.

    Schema derived from workload params, not OpenSearch index body JSON.
    """

    async def __call__(self, milvus_client, params):
        indices = params.get("indices", [])
        if not indices:
            index = params.get("index")
            body_param = params.get("body")
            if index:
                indices = [(index, body_param)]

        client_options = getattr(milvus_client, "client_options", {})

        # BEFORE timing: build schemas
        schemas = []
        for index_name, _ in indices:
            schema, index_params, _ = build_collection_schema(
                milvus_client, params, client_options
            )
            schemas.append((index_name, {"schema": schema, "index_params": index_params}))

        # INSIDE timing: create collection
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            for index_name, create_body in schemas:
                await milvus_client.indices.create(index=index_name, body=create_body)
                self.logger.info("Created Milvus collection: %s", index_name)
            return {"weight": len(schemas), "unit": "ops", "success": True}
        except Exception as e:
            self.logger.warning("MilvusCreateIndex failed: %s", e)
            return {"weight": len(schemas), "unit": "ops", "success": False,
                    "error-type": "milvus", "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "milvus-create-index"


class MilvusDeleteIndex(Runner):

    async def __call__(self, milvus_client, params):
        indices = params.get("indices", [])
        if not indices:
            index = params.get("index")
            if index:
                indices = [index]

        only_if_exists = params.get("only-if-exists", False)
        ops = 0

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            for index_name in indices:
                if not only_if_exists or await milvus_client.indices.exists(index=index_name):
                    await milvus_client.indices.delete(index=index_name)
                    ops += 1
            return {"weight": ops, "unit": "ops", "success": True}
        except Exception as e:
            self.logger.warning("MilvusDeleteIndex failed: %s", e)
            return {"weight": ops, "unit": "ops", "success": False,
                    "error-type": "milvus", "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "milvus-delete-index"


class MilvusRefresh(Runner):

    async def __call__(self, milvus_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            await milvus_client.indices.refresh(index=params.get("index"))
            return {"weight": 1, "unit": "ops", "success": True}
        except Exception as e:
            return {"weight": 1, "unit": "ops", "success": False,
                    "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "milvus-refresh"


class MilvusForceMerge(Runner):

    async def __call__(self, milvus_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            await milvus_client.indices.forcemerge(index=params.get("index"))
            return {"weight": 1, "unit": "ops", "success": True}
        except Exception as e:
            return {"weight": 1, "unit": "ops", "success": False,
                    "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "milvus-force-merge"


class MilvusClusterHealth(Runner):

    async def __call__(self, milvus_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await milvus_client.cluster.health()
            cluster_status = response.get("status", "unknown")
            return {
                "weight": 1,
                "unit": "ops",
                "success": cluster_status in ("green", "yellow"),
                "cluster-status": cluster_status,
                "relocating-shards": 0,
            }
        except Exception:
            return {"weight": 1, "unit": "ops", "success": False, "cluster-status": "red"}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "milvus-cluster-health"


class MilvusIndicesStats(Runner):

    async def __call__(self, milvus_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await milvus_client.indices.stats(index=params.get("index"))
            return {"weight": 1, "unit": "ops", "stats": response}
        except Exception:
            return {"weight": 1, "unit": "ops", "success": False}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "milvus-indices-stats"


class MilvusWarmupRunner(Runner):
    """Load collection and warm up HNSW graph + gRPC channel.

    1. load_collection() — ensures data is in memory (may be no-op if
       create_collection already loaded it)
    2. Warmup queries — issues random vector searches to exercise the HNSW
       graph, warm page cache, and establish gRPC channel. This is critical
       for consistent first-query latency.
    """

    DEFAULT_WARMUP_QUERIES = 100

    async def __call__(self, milvus_client, params):
        index = params.get("index", "target_index")
        vector_field = params.get("target_field_name", "embedding")
        dimension = int(params.get("target_index_dimension", 768))
        k = int(params.get("query_k", params.get("k", 100)))
        client_options = getattr(milvus_client, "client_options", {})
        warmup_queries = self.DEFAULT_WARMUP_QUERIES

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            # Step 1: Ensure collection is loaded
            start = time.perf_counter()
            await milvus_client.load_collection(collection_name=index)
            load_time = time.perf_counter() - start

            # Step 2: Warmup queries with random vectors
            import numpy as np  # pylint: disable=import-outside-toplevel
            np.random.seed(0)
            search_params = build_search_params(params, client_options)

            for _ in range(warmup_queries):
                warmup_vec = np.random.randn(dimension).astype(np.float32).tolist()
                search_body = {
                    "data": [warmup_vec],
                    "anns_field": vector_field,
                    "limit": k,
                    "output_fields": ["doc_id"],
                    "search_params": search_params["search_params"],
                }
                await milvus_client.search(index=index, body=search_body)

            warmup_time = time.perf_counter() - start
            self.logger.info(
                "Warmup complete: load=%.1fs, %d queries in %.1fs total",
                load_time, warmup_queries, warmup_time,
            )
            return {"weight": 1, "unit": "ops", "success": True}
        except Exception as e:
            self.logger.warning("MilvusWarmup failed: %s", e)
            return {"weight": 1, "unit": "ops", "success": False,
                    "error-type": "milvus", "error-description": str(e)}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "warmup-knn-indices"


class MilvusNoOp(Runner):

    def __init__(self, name):
        super().__init__()
        self._name = name

    async def __call__(self, milvus_client, params):
        self.logger.info("Skipping unsupported operation [%s] for Milvus", self._name)
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            return {"weight": 1, "unit": "ops", "success": True}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return self._name


def register_milvus_runners():
    """Register all Milvus-specific runners with the runner registry."""
    from osbenchmark.worker_coordinator.runner import register_runner  # pylint: disable=import-outside-toplevel

    register_runner(workload.OperationType.Bulk, MilvusBulkIndex(), async_runner=True)
    register_runner(workload.OperationType.Search, MilvusQuery(), async_runner=True)
    register_runner(workload.OperationType.PaginatedSearch, MilvusQuery(), async_runner=True)
    register_runner(workload.OperationType.VectorSearch, MilvusVectorSearch(), async_runner=True)
    register_runner(workload.OperationType.BulkVectorDataSet, MilvusBulkVectorDataSet(), async_runner=True)
    register_runner(workload.OperationType.CreateIndex, MilvusCreateIndex(), async_runner=True)
    register_runner(workload.OperationType.DeleteIndex, MilvusDeleteIndex(), async_runner=True)
    register_runner(workload.OperationType.IndexStats, MilvusIndicesStats(), async_runner=True)
    register_runner(workload.OperationType.ClusterHealth, MilvusClusterHealth(), async_runner=True)
    register_runner(workload.OperationType.Refresh, MilvusRefresh(), async_runner=True)
    register_runner(workload.OperationType.ForceMerge, MilvusForceMerge(), async_runner=True)
    register_runner("warmup-knn-indices", MilvusWarmupRunner(), async_runner=True)

    for op_type in [
        workload.OperationType.PutPipeline,
        workload.OperationType.DeletePipeline,
        workload.OperationType.CreateSearchPipeline,
        workload.OperationType.PutSettings,
    ]:
        register_runner(op_type, MilvusNoOp(str(op_type)), async_runner=True)
