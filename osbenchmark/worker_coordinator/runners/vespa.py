# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Vespa-specific runners for OpenSearch Benchmark.

These runners implement benchmark operations using Vespa's native APIs.
They handle timing context and API calls directly without translation layers.
"""

import asyncio
import sys

from osbenchmark import workload
# Import from the main runner module to use the same registry
from osbenchmark.worker_coordinator import runner as main_runner
from osbenchmark.worker_coordinator.runners.base import (
    Runner,
    Delegator,
    request_context_holder,
    mandatory,
)

# Use the main runner module's register function to share the same registry
register_runner = main_runner.register_runner


def register_vespa_runners():
    """Register all Vespa-specific runners."""
    OT = workload.OperationType

    register_runner(OT.Bulk, VespaBulkIndex(), async_runner=True)
    register_runner(
        OT.ClusterHealth, VespaRetry(VespaClusterHealth()),
        async_runner=True
    )
    register_runner(
        OT.CreateIndex, VespaRetry(VespaCreateIndex()),
        async_runner=True
    )
    register_runner(
        OT.DeleteIndex, VespaRetry(VespaDeleteIndex()),
        async_runner=True
    )
    register_runner(
        OT.Refresh, VespaRetry(VespaRefresh()), async_runner=True
    )
    register_runner(OT.Search, VespaQuery(), async_runner=True)
    register_runner(
        OT.ForceMerge, VespaRetry(VespaForceMerge()), async_runner=True
    )
    register_runner(
        OT.IndexStats, VespaRetry(VespaIndexStats()), async_runner=True
    )
    register_runner(OT.NodeStats, VespaNodeStats(), async_runner=True)

    # Operations that are no-ops or not applicable to Vespa
    register_runner(
        OT.PutPipeline, VespaNoOp("put-pipeline"), async_runner=True
    )
    register_runner(
        OT.DeletePipeline, VespaNoOp("delete-pipeline"), async_runner=True
    )
    register_runner(
        OT.PutSettings, VespaNoOp("put-settings"), async_runner=True
    )
    register_runner(
        OT.CreateIndexTemplate, VespaNoOp("create-index-template"),
        async_runner=True
    )
    register_runner(
        OT.DeleteIndexTemplate, VespaNoOp("delete-index-template"),
        async_runner=True
    )

    # Sleep works the same way
    register_runner(OT.Sleep, VespaSleep(), async_runner=True)


class VespaRetry(Runner, Delegator):
    """
    Retry wrapper for Vespa operations.

    Similar to the OpenSearch Retry class but handles Vespa-specific errors.
    """

    def __init__(self, delegate, retry_until_success=False):
        super().__init__(delegate=delegate)
        self.retry_until_success = retry_until_success

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __call__(self, client, params):
        default_retry = self.retry_until_success
        retry_until_success = params.get("retry-until-success", default_retry)
        if retry_until_success:
            max_attempts = sys.maxsize
            retry_on_error = True
        else:
            max_attempts = params.get("retries", 0) + 1
            retry_on_error = params.get("retry-on-error", False)

        sleep_time = params.get("retry-wait-period", 0.5)
        retry_on_timeout = params.get("retry-on-timeout", True)

        for attempt in range(max_attempts):
            last_attempt = attempt + 1 == max_attempts
            try:
                return_value = await self.delegate(client, params)
                if last_attempt or not retry_on_error:
                    return return_value
                elif isinstance(return_value, dict):
                    if return_value.get("success", True):
                        return return_value
                    else:
                        self.logger.info(
                            "[%s] error: %s. Retrying in [%.2f]s.",
                            repr(self.delegate), return_value, sleep_time
                        )
                        await asyncio.sleep(sleep_time)
                else:
                    return return_value
            except asyncio.TimeoutError:
                if last_attempt or not retry_on_timeout:
                    raise
                await asyncio.sleep(sleep_time)
            except Exception as e:
                if last_attempt:
                    raise
                self.logger.warning(
                    "Attempt %d failed: %s. Retrying...",
                    attempt + 1, e
                )
                await asyncio.sleep(sleep_time)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

    def __repr__(self):
        return f"retrying [{self.delegate}]"


class VespaDeleteIndex(Runner):
    """
    Delete documents from a Vespa schema.

    Note: Vespa doesn't support dynamic schema deletion without redeployment.
    This operation clears documents or is a no-op if the schema doesn't exist.
    """

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            indices = mandatory(params, "indices", self)
            only_if_exists = params.get("only-if-exists", False)
            ops = 0

            for index_name in indices:
                if only_if_exists:
                    # Check if schema exists - for Vespa, assume it does
                    self.logger.info(
                        "Vespa: Skipping delete for [%s] (no-op)",
                        index_name
                    )
                else:
                    self.logger.info(
                        "Vespa: Delete-index [%s] (no-op)", index_name
                    )
                ops += 1

            return {
                "weight": ops,
                "unit": "ops",
                "success": True
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-delete-index"


class VespaCreateIndex(Runner):
    """
    Create a Vespa schema.

    For benchmarking, we assume schemas are pre-deployed.
    This is a no-op that returns success.
    """

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            indices = mandatory(params, "indices", self)

            for index, _body in indices:
                self.logger.info(
                    "Vespa: Create-index [%s] (no-op)", index
                )

            return {
                "weight": len(indices),
                "unit": "ops",
                "success": True
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-create-index"


class VespaClusterHealth(Runner):
    """
    Check Vespa cluster health.

    Vespa health is binary (up or down) - no yellow/green states.
    """

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            # Vespa is either up or down - check via application status
            # The client's wait_for_rest_layer already verified Vespa is up
            cluster_status = "green"
            relocating_shards = 0

            return {
                "weight": 1,
                "unit": "ops",
                "success": True,
                "cluster-status": cluster_status,
                "relocating-shards": relocating_shards
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-cluster-health"


class VespaBulkIndex(Runner):
    """
    Bulk index documents into Vespa.

    Uses Vespa's Document API with parallel requests.
    """

    # pylint: disable=too-many-nested-blocks
    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()

        try:
            bulk_params = params.get("bulk-params", {})
            api_kwargs = self._default_kw_params(params)

            # action-metadata-present is required but Vespa always needs index
            mandatory(params, "action-metadata-present", self)
            bulk_size = mandatory(params, "bulk-size", self)
            unit = mandatory(params, "unit", self)

            response = await client.bulk(params=bulk_params, **api_kwargs)

            # Parse response
            if isinstance(response, dict):
                took = response.get("took", 0)
                errors = response.get("errors", False)
                items = response.get("items", [])

                error_count = 0
                if errors:
                    for item in items:
                        for _action, result in item.items():
                            if result.get("status", 200) >= 400:
                                error_count += 1

                return {
                    "weight": bulk_size,
                    "unit": unit,
                    "success": not errors,
                    "success-count": bulk_size - error_count,
                    "error-count": error_count,
                    "took": took
                }
            else:
                return {
                    "weight": bulk_size,
                    "unit": unit,
                    "success": True,
                    "success-count": bulk_size,
                    "error-count": 0
                }
        finally:
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-bulk-index"


class VespaRefresh(Runner):
    """
    Refresh operation for Vespa.

    Vespa documents are immediately searchable - this is a no-op.
    """

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            index = params.get("index", "_all")
            self.logger.debug("Vespa: Refresh [%s] (no-op)", index)

            return {
                "weight": 1,
                "unit": "ops",
                "success": True
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-refresh"


class VespaForceMerge(Runner):
    """
    Force merge operation for Vespa.

    Vespa manages compaction automatically - this is a no-op.
    """

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            index = params.get("index")
            self.logger.debug("Vespa: Force-merge [%s] (no-op)", index)

            return {
                "weight": 1,
                "unit": "ops",
                "success": True
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-force-merge"


class VespaQuery(Runner):
    """
    Execute search queries against Vespa.

    Translates OpenSearch DSL queries to Vespa YQL.
    """

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()

        try:
            request_params, _ = self._transport_request_params(params)
            index = params.get("index", "_all")
            cache = params.get("cache")
            body = mandatory(params, "body", self)

            if cache is not None:
                request_params["request_cache"] = str(cache).lower()

            # Use client's search method which handles YQL translation
            response = await client.search(
                body=body, index=index, params=request_params
            )

            # Extract result metrics
            if isinstance(response, dict):
                hits = response.get("hits", {})
                total_hits = hits.get("total", {})
                if isinstance(total_hits, dict):
                    total_count = total_hits.get("value", 0)
                else:
                    total_count = total_hits
                took = response.get("took", 0)
                timed_out = response.get("timed_out", False)
            else:
                total_count = 0
                took = 0
                timed_out = False

            return {
                "weight": 1,
                "unit": "ops",
                "success": not timed_out,
                "hits": total_count,
                "took": took,
                "timed_out": timed_out
            }
        finally:
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-query"


class VespaIndexStats(Runner):
    """
    Get index statistics from Vespa.
    """

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            # Vespa doesn't have index stats in the same way
            # Return minimal structure
            return {
                "weight": 1,
                "unit": "ops",
                "success": True
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-index-stats"


class VespaNodeStats(Runner):
    """
    Get node statistics from Vespa.
    """

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()
        try:
            # Return minimal stats structure
            return {
                "weight": 1,
                "unit": "ops",
                "success": True
            }
        finally:
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return "vespa-node-stats"


class VespaNoOp(Runner):
    """
    No-operation runner for operations not applicable to Vespa.
    """

    def __init__(self, operation_name: str):
        super().__init__()
        self.operation_name = operation_name

    async def __call__(self, client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            self.logger.debug("Vespa: %s (no-op)", self.operation_name)
            return {
                "weight": 1,
                "unit": "ops",
                "success": True
            }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

    def __repr__(self):
        return f"vespa-noop-{self.operation_name}"


class VespaSleep(Runner):
    """
    Sleep operation - same as OpenSearch.
    """

    async def __call__(self, client, params):
        duration = mandatory(params, "duration", self)
        await asyncio.sleep(duration)
        return {
            "weight": 1,
            "unit": "ops",
            "success": True
        }

    def __repr__(self):
        return "vespa-sleep"
