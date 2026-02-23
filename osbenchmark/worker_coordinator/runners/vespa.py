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
Vespa-specific runner implementations for OpenSearch Benchmark.

Each runner inherits from the Runner base class and implements the
__call__ protocol. Translation logic (DSL→YQL, document transforms)
is delegated to helpers.py; runners orchestrate the flow and manage
timing via RequestContextHolder.
"""

import asyncio
import logging

from osbenchmark import workload
from osbenchmark.worker_coordinator.runners.base import Runner, request_context_holder
from osbenchmark.database.clients.vespa import PYVESPA_AVAILABLE
from osbenchmark.database.clients.vespa.helpers import (
    convert_to_yql,
    convert_vespa_response,
    parse_bulk_body,
    transform_document_for_vespa,
)


class VespaBulkIndex(Runner):
    """Bulk indexes documents into Vespa using its document feed API.

    Uses pyvespa's VespaAsync (HTTP/2 multiplexing, built-in retry) when
    available, falling back to per-document aiohttp POST requests.
    """

    async def __call__(self, vespa_client, params):
        bulk_size = params.get("bulk-size", 0)
        unit = params.get("unit", "docs")
        body = params.get("body")
        index = params.get("index")
        app_name = getattr(vespa_client, "_app_name", "default")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            documents = parse_bulk_body(body)

            # Transform documents (shared by both paths)
            prepared = []
            for i, doc in enumerate(documents):
                doc_id = doc.get("_id", f"doc_{i}")
                source = doc.get("_source", doc)

                if "index" in source:
                    source = {k: v for k, v in source.items() if k != "index"}

                if "@timestamp" in source or any(isinstance(v, dict) for v in source.values()):
                    source = transform_document_for_vespa(source, app_name)

                prepared.append({"_id": doc_id, "fields": source})

            if PYVESPA_AVAILABLE:
                errors_count = await self._feed_via_pyvespa(
                    vespa_client, prepared, index or app_name, params
                )
            else:
                errors_count = await self._feed_via_aiohttp(
                    vespa_client, prepared, index, params
                )

            return {
                "weight": bulk_size if bulk_size else len(documents),
                "unit": unit,
                "success": errors_count == 0,
                "error-count": errors_count,
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    async def _feed_via_pyvespa(self, vespa_client, documents, schema, params):
        """Feed documents via pyvespa VespaAsync (HTTP/2, built-in retry)."""
        client_opts = getattr(vespa_client, "client_options", {})
        max_workers = params.get("max_concurrent",
                                 int(client_opts.get("max_concurrent", 32)))
        namespace = getattr(vespa_client, "_namespace", "benchmark")

        result = await vespa_client.feed_batch(
            documents=documents,
            schema=schema,
            namespace=namespace,
            max_workers=max_workers,
        )
        return result["errors"]

    async def _feed_via_aiohttp(self, vespa_client, documents, index, params):
        """Feed documents via aiohttp (fallback path)."""
        client_opts = getattr(vespa_client, "client_options", {})
        max_concurrent = params.get("max_concurrent",
                                    int(client_opts.get("max_concurrent", 50)))
        semaphore = asyncio.Semaphore(max_concurrent)
        timeout_val = params.get("request-timeout", 30)
        errors_count = 0

        async def index_doc(doc):
            async with semaphore:
                doc_id = doc["_id"]
                try:
                    await vespa_client.index(
                        index=index, body=doc["fields"], id=doc_id,
                        request_timeout=timeout_val
                    )
                    return {"_id": doc_id, "status": 200}
                except Exception as e:
                    self.logger.warning("Error feeding document %s: %s", doc_id, e)
                    return {"_id": doc_id, "error": str(e)}

        tasks = [index_doc(doc) for doc in documents]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in raw_results:
            if isinstance(result, Exception):
                errors_count += 1
            elif isinstance(result, dict) and "error" in result:
                errors_count += 1

        return errors_count

    def __repr__(self):
        return "vespa-bulk-index"


class VespaVectorSearch(Runner):
    """Executes vector similarity search against Vespa using YQL.

    Converts KNN query to nearestNeighbor YQL via helpers.
    """

    async def __call__(self, vespa_client, params):
        index = params.get("index")
        body = params.get("body", {})
        app_name = getattr(vespa_client, "_app_name", index or "default")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            yql_query, query_params = convert_to_yql(body, app_name)
            search_params = {"yql": yql_query}
            search_params.update(query_params)

            raw_response = await vespa_client.search(index=index, body=search_params)
            response = convert_vespa_response(raw_response)

            hits = response.get("hits", {}).get("total", {}).get("value", 0)
            hits_relation = response.get("hits", {}).get("total", {}).get("relation", "eq")
            timed_out = response.get("timed_out", False)

            result = {
                "weight": 1,
                "unit": "ops",
                "hits": hits,
                "hits_relation": hits_relation,
                "timed_out": timed_out,
            }

            if params.get("detailed-results", False):
                result["hits_total"] = hits
                result["took"] = response.get("took", 0)

            return result
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-vector-search"


class VespaBulkVectorDataSet(Runner):
    """Bulk inserts vector datasets into Vespa.

    Handles large-scale vector ingestion for benchmarking.
    """

    async def __call__(self, vespa_client, params):
        size = params.get("size", 0)
        body = params["body"]

        params_without_body = {k: v for k, v in params.items() if k != "body"}

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            await vespa_client.bulk(body=body, **params_without_body)
            return size, "docs"
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-bulk-vector-data-set"


class VespaQuery(Runner):
    """Executes general search queries against Vespa.

    Converts OpenSearch DSL to YQL, sends to Vespa, converts response back.
    """

    async def __call__(self, vespa_client, params):
        index = params.get("index")
        body = params.get("body", {})
        app_name = getattr(vespa_client, "_app_name", index or "default")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            yql_query, query_params = convert_to_yql(body, app_name)
            search_params = {"yql": yql_query}
            search_params.update(query_params)

            # Forward workload request-timeout to Vespa query timeout
            request_timeout = params.get("request-timeout")
            if request_timeout and "timeout" not in search_params:
                search_params["timeout"] = f"{request_timeout}s"

            raw_response = await vespa_client.search(index=index, body=search_params)
            response = convert_vespa_response(raw_response)

            hits = response.get("hits", {}).get("total", {}).get("value", 0)
            hits_relation = response.get("hits", {}).get("total", {}).get("relation", "eq")
            timed_out = response.get("timed_out", False)

            return {
                "weight": 1,
                "unit": "ops",
                "hits": hits,
                "hits_relation": hits_relation,
                "timed_out": timed_out,
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-query"


class VespaScrollQuery(Runner):
    """Simulates scroll queries using offset/limit pagination.

    Vespa doesn't have a scroll API, so we paginate with offset/limit.
    """

    async def __call__(self, vespa_client, params):
        pages = params.get("pages", 10)
        results_per_page = params.get("results-per-page", 1000)
        index = params.get("index")
        body = params.get("body", {})
        app_name = getattr(vespa_client, "_app_name", index or "default")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            total_hits = 0
            retrieved_pages = 0
            total_took = 0
            timed_out = False

            for page in range(pages):
                offset = page * results_per_page
                page_body = body.copy() if body else {}
                page_body["size"] = results_per_page
                page_body["from"] = offset

                yql_query, query_params = convert_to_yql(page_body, app_name)
                search_params = {"yql": yql_query}
                search_params.update(query_params)

                raw_response = await vespa_client.search(index=index, body=search_params)
                response = convert_vespa_response(raw_response)

                if page == 0:
                    total_hits = response.get("hits", {}).get("total", {}).get("value", 0)

                total_took += response.get("took", 0)
                timed_out = timed_out or response.get("timed_out", False)
                retrieved_pages += 1

                current_results = len(response.get("hits", {}).get("hits", []))
                if current_results < results_per_page:
                    break

            return {
                "weight": retrieved_pages,
                "pages": retrieved_pages,
                "hits": total_hits,
                "hits_relation": "eq",
                "unit": "pages",
                "timed_out": timed_out,
                "took": total_took,
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-scroll-query"


class VespaCreateIndex(Runner):
    """Creates a Vespa schema/document type.

    In Vespa, schemas are deployed via application packages.
    This runner acknowledges the operation with a lightweight HTTP call.
    """

    async def __call__(self, vespa_client, params):
        index = params.get("index")
        body = params.get("body")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.indices.create(index=index, body=body)
            return {
                "weight": 1,
                "unit": "ops",
                "acknowledged": response.get("acknowledged", True),
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-create-index"


class VespaDeleteIndex(Runner):
    """Deletes a Vespa schema/document type."""

    async def __call__(self, vespa_client, params):
        index = params.get("index")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.indices.delete(index=index)
            return {
                "weight": 1,
                "unit": "ops",
                "acknowledged": response.get("acknowledged", True),
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-delete-index"


class VespaIndicesStats(Runner):
    """Retrieves index statistics from Vespa metrics."""

    async def __call__(self, vespa_client, params):
        index = params.get("index")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = vespa_client.indices.stats(index=index)
            return {
                "weight": 1,
                "unit": "ops",
                "stats": response,
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-indices-stats"


class VespaClusterHealth(Runner):
    """Checks Vespa cluster health status."""

    async def __call__(self, vespa_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.cluster.health()
            return {
                "weight": 1,
                "unit": "ops",
                "status": response.get("status", "unknown"),
                "timed_out": response.get("timed_out", False),
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-cluster-health"


class VespaRefresh(Runner):
    """Refresh operation (no-op for Vespa)."""

    async def __call__(self, vespa_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.indices.refresh(index=params.get("index"))
            return {
                "weight": 1,
                "unit": "ops",
                "shards": response.get("_shards", {}),
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-refresh"


class VespaForceMerge(Runner):
    """Force merge operation (no direct equivalent in Vespa)."""

    async def __call__(self, vespa_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = vespa_client.indices.forcemerge(index=params.get("index"))
            return {
                "weight": 1,
                "unit": "ops",
                "shards": response.get("_shards", {}),
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-force-merge"


class VespaWarmupIndicesRunner(Runner):
    """Warmup indices for KNN vector search.

    No-op in Vespa — makes a lightweight health check to maintain timing context.
    """

    async def __call__(self, vespa_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            await vespa_client.cluster.health()
            return {"success": True}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "warmup-knn-indices"


def register_vespa_runners():
    """Register all Vespa-specific runners with the runner registry.

    Overrides the default OpenSearch runners for the operation types
    that Vespa supports.
    """
    from osbenchmark.worker_coordinator.runners import register_runner

    register_runner(workload.OperationType.Bulk, VespaBulkIndex(), async_runner=True)
    register_runner(workload.OperationType.Search, VespaQuery(), async_runner=True)
    register_runner(workload.OperationType.PaginatedSearch, VespaQuery(), async_runner=True)
    register_runner(workload.OperationType.ScrollSearch, VespaScrollQuery(), async_runner=True)
    register_runner(workload.OperationType.VectorSearch, VespaVectorSearch(), async_runner=True)
    register_runner(workload.OperationType.BulkVectorDataSet, VespaBulkVectorDataSet(), async_runner=True)
    register_runner(workload.OperationType.CreateIndex, VespaCreateIndex(), async_runner=True)
    register_runner(workload.OperationType.DeleteIndex, VespaDeleteIndex(), async_runner=True)
    register_runner(workload.OperationType.IndexStats, VespaIndicesStats(), async_runner=True)
    register_runner(workload.OperationType.ClusterHealth, VespaClusterHealth(), async_runner=True)
    register_runner(workload.OperationType.Refresh, VespaRefresh(), async_runner=True)
    register_runner(workload.OperationType.ForceMerge, VespaForceMerge(), async_runner=True)
