# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Vespa-specific runner implementations for OpenSearch Benchmark.

This module provides runner classes that adapt OSB's workload operations
to work with Vespa's API and data model.
"""

import logging
from typing import Dict, Any, List
from osbenchmark.worker_coordinator.runner import Runner
from osbenchmark.client import RequestContextHolder


request_context_holder = RequestContextHolder()


class VespaBulkIndex(Runner):
    """
    Bulk indexes documents into Vespa using its document feed API.

    Adapts OSB's bulk format to Vespa's document/v1 API.
    """

    async def __call__(self, vespa_client, params):
        """
        Execute bulk indexing operation.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with 'body', 'bulk-size', 'unit', 'index'
        :return: Tuple of (bulk_size, unit)
        """
        bulk_size = params.get("bulk-size", 0)
        unit = params.get("unit", "docs")
        body = params.get("body")
        index = params.get("index")

        # Remove params that are passed explicitly to avoid duplicate keyword args
        params_filtered = {k: v for k, v in params.items() if k not in ("body", "index", "bulk-size", "unit")}

        request_context_holder.on_client_request_start()
        # Ensure request_start is set as fallback (aiohttp hooks should set this, but provide fallback)
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.bulk(body=body, index=index, **params_filtered)
            return bulk_size, unit
        finally:
            # Ensure request_end is set as fallback
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-bulk-index"


class VespaVectorSearch(Runner):
    """
    Executes vector similarity search queries against Vespa using YQL.

    Converts OSB's KNN query format to Vespa's nearestNeighbor YQL syntax.
    """

    async def __call__(self, vespa_client, params):
        """
        Execute vector search operation.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with 'body', 'index', query vector
        :return: Dict with search metadata
        """
        index = params.get("index")
        body = params.get("body", {})

        # Remove 'index' and 'body' from params to avoid passing them twice
        params_filtered = {k: v for k, v in params.items() if k not in ("index", "body")}

        request_context_holder.on_client_request_start()
        # Ensure request_start is set as fallback
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.search(index=index, body=body, **params_filtered)

            # Extract metadata
            hits = response.get("hits", {}).get("total", {}).get("value", 0)
            hits_relation = response.get("hits", {}).get("total", {}).get("relation", "eq")
            timed_out = response.get("timed_out", False)

            result = {
                "weight": 1,
                "unit": "ops",
                "hits": hits,
                "hits_relation": hits_relation,
                "timed_out": timed_out
            }

            # Add detailed results if requested
            if params.get("detailed-results", False):
                result["hits_total"] = hits
                result["took"] = response.get("took", 0)

            return result

        finally:
            # Ensure request_end is set as fallback
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-vector-search"


class VespaBulkVectorDataSet(Runner):
    """
    Bulk inserts vector datasets (HDF5, BigANN format) into Vespa.

    Handles large-scale vector ingestion for benchmarking.
    """

    NAME = "vespa-bulk-vector-data-set"

    async def __call__(self, vespa_client, params):
        """
        Execute bulk vector ingestion.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with 'body', 'size'
        :return: Tuple of (size, unit)
        """
        size = params.get("size", 0)
        body = params["body"]

        # Remove 'body' from params to avoid passing it twice
        params_without_body = {k: v for k, v in params.items() if k != "body"}

        request_context_holder.on_client_request_start()
        # Ensure request_start is set as fallback (aiohttp hooks should set this, but provide fallback)
        request_context_holder.on_request_start()
        try:
            await vespa_client.bulk(body=body, **params_without_body)
            return size, "docs"
        finally:
            # Ensure request_end is set as fallback
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return self.NAME


class VespaQuery(Runner):
    """
    Executes general search queries against Vespa.

    Handles standard search, paginated search, and filtered queries.
    """

    async def __call__(self, vespa_client, params):
        """
        Execute search query.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with 'index', 'body'
        :return: Dict with search metadata
        """
        index = params.get("index")
        body = params.get("body", {})

        # Remove params that are passed explicitly to avoid duplicate keyword args
        params_filtered = {k: v for k, v in params.items() if k not in ("index", "body")}

        request_context_holder.on_client_request_start()
        # Ensure request_start is set as fallback
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.search(index=index, body=body, **params_filtered)

            # Extract metadata
            hits = response.get("hits", {}).get("total", {}).get("value", 0)
            hits_relation = response.get("hits", {}).get("total", {}).get("relation", "eq")
            timed_out = response.get("timed_out", False)

            result = {
                "weight": 1,
                "unit": "ops",
                "hits": hits,
                "hits_relation": hits_relation,
                "timed_out": timed_out
            }

            return result

        finally:
            # Ensure request_end is set as fallback
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-query"


class VespaCreateIndex(Runner):
    """
    Creates a Vespa schema/document type.

    In Vespa, this typically involves deploying an application package.
    """

    async def __call__(self, vespa_client, params):
        """
        Create index/schema.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with 'index', 'body'
        :return: Dict with creation status
        """
        index = params.get("index")
        body = params.get("body")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.indices_create(index=index, body=body)
            return {
                "weight": 1,
                "unit": "ops",
                "acknowledged": response.get("acknowledged", True)
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-create-index"


class VespaDeleteIndex(Runner):
    """
    Deletes a Vespa schema/document type.
    """

    async def __call__(self, vespa_client, params):
        """
        Delete index/schema.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with 'index'
        :return: Dict with deletion status
        """
        index = params.get("index")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.indices_delete(index=index)
            return {
                "weight": 1,
                "unit": "ops",
                "acknowledged": response.get("acknowledged", True)
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-delete-index"


class VespaIndicesStats(Runner):
    """
    Retrieves index statistics from Vespa metrics.
    """

    async def __call__(self, vespa_client, params):
        """
        Get index statistics.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with optional 'index'
        :return: Dict with index stats
        """
        index = params.get("index")

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.indices_stats(index=index)
            return {
                "weight": 1,
                "unit": "ops",
                "stats": response
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-indices-stats"


class VespaClusterHealth(Runner):
    """
    Checks Vespa cluster health status.
    """

    async def __call__(self, vespa_client, params):
        """
        Get cluster health.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict (may include wait conditions)
        :return: Dict with health status
        """
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.cluster_health(**params)
            return {
                "weight": 1,
                "unit": "ops",
                "status": response.get("status", "unknown"),
                "timed_out": response.get("timed_out", False)
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-cluster-health"


class VespaRefresh(Runner):
    """
    Refresh operation (no-op for Vespa as it doesn't have explicit refresh).
    """

    async def __call__(self, vespa_client, params):
        """
        Execute refresh (no-op).

        :param vespa_client: Vespa client instance
        :param params: Parameters dict
        :return: Dict with operation status
        """
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.indices_refresh(index=params.get("index"))
            return {
                "weight": 1,
                "unit": "ops",
                "shards": response.get("_shards", {})
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-refresh"


class VespaForceMerge(Runner):
    """
    Force merge operation (no direct equivalent in Vespa).
    """

    async def __call__(self, vespa_client, params):
        """
        Execute force merge (no-op for Vespa).

        :param vespa_client: Vespa client instance
        :param params: Parameters dict
        :return: Dict with operation status
        """
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await vespa_client.indices_force_merge(index=params.get("index"))
            return {
                "weight": 1,
                "unit": "ops",
                "shards": response.get("_shards", {})
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-force-merge"


class VespaWarmupIndicesRunner(Runner):
    """
    Warmup indices operation for KNN vector search.

    In Vespa, this is a no-op since Vespa doesn't require explicit warmup
    of native library files. However, we simulate the operation with a
    lightweight HTTP call to maintain timing context consistency.
    """

    RUNNER_NAME = "warmup-knn-indices"

    async def __call__(self, vespa_client, params):
        """
        Execute warmup operation (lightweight no-op for Vespa).

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with 'index'
        :return: Dict with success status
        """
        index = params.get("index")
        result = {"success": True}

        request_context_holder.on_client_request_start()
        # Ensure request timing context is set
        request_context_holder.on_request_start()
        try:
            # Make a lightweight HTTP call to maintain timing context
            # In Vespa, we can query the application status to verify readiness
            await vespa_client.cluster_health()
            return result
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return self.RUNNER_NAME


class VespaScrollQuery(Runner):
    """
    Simulates scroll queries for Vespa using offset/limit pagination.

    Vespa doesn't have a scroll API like OpenSearch, so we implement
    pagination using offset/limit parameters. This runner executes
    multiple paginated queries to simulate scroll behavior.
    """

    async def __call__(self, vespa_client, params):
        """
        Execute scroll query simulation.

        :param vespa_client: Vespa client instance
        :param params: Parameters dict with 'pages', 'results-per-page', 'index', 'body'
        :return: Dict with scroll metadata
        """
        pages = params.get("pages", 10)
        results_per_page = params.get("results-per-page", 1000)
        index = params.get("index")
        body = params.get("body", {})

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            total_hits = 0
            retrieved_pages = 0
            total_took = 0
            timed_out = False

            for page in range(pages):
                # Calculate offset for this page
                offset = page * results_per_page

                # Create page-specific body with size and from
                page_body = body.copy() if body else {}
                page_body["size"] = results_per_page
                page_body["from"] = offset

                # Remove params that shouldn't be passed to search
                params_filtered = {k: v for k, v in params.items()
                                   if k not in ("index", "body", "pages", "results-per-page")}

                response = await vespa_client.search(index=index, body=page_body, **params_filtered)

                # Get total hits from first page
                if page == 0:
                    total_hits = response.get("hits", {}).get("total", {}).get("value", 0)

                total_took += response.get("took", 0)
                timed_out = timed_out or response.get("timed_out", False)
                retrieved_pages += 1

                # Check if we've retrieved all results
                current_results = len(response.get("hits", {}).get("hits", []))
                if current_results < results_per_page:
                    # No more results to fetch
                    break

            return {
                "weight": retrieved_pages,
                "pages": retrieved_pages,
                "hits": total_hits,
                "hits_relation": "eq",
                "unit": "pages",
                "timed_out": timed_out,
                "took": total_took
            }

        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-scroll-query"


# Registry of Vespa runners
VESPA_RUNNERS = {
    "bulk": VespaBulkIndex(),
    "search": VespaQuery(),
    "vector-search": VespaVectorSearch(),
    "bulk-vector-data-set": VespaBulkVectorDataSet(),
    "create-index": VespaCreateIndex(),
    "delete-index": VespaDeleteIndex(),
    "indices-stats": VespaIndicesStats(),
    "index-stats": VespaIndicesStats(),  # Alias for big5 workload compatibility
    "cluster-health": VespaClusterHealth(),
    "refresh": VespaRefresh(),
    "force-merge": VespaForceMerge(),
    "warmup-knn-indices": VespaWarmupIndicesRunner(),
    "scroll": VespaScrollQuery(),
}


def get_vespa_runner(operation_type: str):
    """
    Get the appropriate Vespa runner for an operation type.

    :param operation_type: The operation type string
    :return: Runner instance or None
    """
    return VESPA_RUNNERS.get(operation_type)
