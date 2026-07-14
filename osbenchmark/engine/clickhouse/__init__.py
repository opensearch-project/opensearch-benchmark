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
ClickHouse engine module.

Exposes the engine-registry interface for ClickHouse: create_client_factory,
create_async_client, register_runners, wait_for_client, on_execute_error.

ClickHouse runners expect bodies to already contain pre-translated SQL. The
workload's ClickHouse-native param source (e.g. `clickhouse-query-param-source`)
generates such bodies. Workloads that send OpenSearch-style DSL queries to the
ClickHouse runner will fail with a clear error pointing at the ClickHouse-native
procedure.
"""

from osbenchmark.engine.clickhouse.client import ClickHouseClientFactory, ClickHouseDatabaseClient  # noqa: F401


def create_client_factory(hosts, client_options):
    """Return a ClickHouseClientFactory. Its .create() returns a sync ClickHouseDatabaseClient."""
    return ClickHouseClientFactory(hosts, client_options)


def create_async_client(hosts, client_options, cfg=None):
    """Return an async ClickHouse client. cfg is accepted for interface symmetry but not used."""
    return ClickHouseClientFactory(hosts, client_options).create_async()


def register_runners():
    """Register ClickHouse runners, overriding OS defaults for shared operation types.

    Called AFTER runner.register_default_runners() so ClickHouse implementations win
    over OS defaults for ops that both engines support (Bulk, Search, VectorSearch,
    CreateIndex, DeleteIndex, ClusterHealth, IndexStats, Refresh, ForceMerge).

    ClusterHealth is wrapped in ``Retry(...)`` so workloads can set
    ``retry-until-success: true`` and get the same polling semantics as the OS
    default. Retry's return-value-based retry path (``retry-on-error`` /
    ``retry-until-success``) works engine-agnostically — it inspects the
    runner's dict return, not exception types — so this wrapping is genuinely
    useful even though Retry's exception-based branches only recognize
    opensearchpy transport errors.

    Other admin runners (CREATE TABLE, DROP TABLE, OPTIMIZE, SystemParts) are
    NOT wrapped: they are one-shot at benchmark start with no retry-until-success
    use case, and Retry's exception clauses can't catch ClickHouse's httpx /
    clickhouse-connect exceptions anyway.

    Follow-up: wire ``engine.clickhouse.on_execute_error`` into the sample
    error path so transient httpx/ClickHouse exceptions get consistent handling.
    """
    # pylint: disable=import-outside-toplevel
    from osbenchmark import workload
    from osbenchmark.worker_coordinator import runner
    from osbenchmark.worker_coordinator.runner import Retry
    from osbenchmark.engine.clickhouse import runners as ch_runners

    # Data-plane runners.
    runner.register_runner(workload.OperationType.Bulk,
                           ch_runners.ClickHouseBulkIndex(), async_runner=True)
    runner.register_runner(workload.OperationType.Search,
                           ch_runners.ClickHouseQuery(), async_runner=True)
    runner.register_runner(workload.OperationType.PaginatedSearch,
                           ch_runners.ClickHouseQuery(), async_runner=True)
    runner.register_runner(workload.OperationType.ScrollSearch,
                           ch_runners.ClickHouseScrollQuery(), async_runner=True)
    runner.register_runner(workload.OperationType.VectorSearch,
                           ch_runners.ClickHouseVectorSearch(), async_runner=True)
    runner.register_runner(workload.OperationType.BulkVectorDataSet,
                           ch_runners.ClickHouseBulkVectorDataSet(), async_runner=True)

    # Admin-plane runners.
    runner.register_runner(workload.OperationType.CreateIndex,
                           ch_runners.ClickHouseCreateTable(), async_runner=True)
    runner.register_runner(workload.OperationType.DeleteIndex,
                           ch_runners.ClickHouseDropTable(), async_runner=True)
    runner.register_runner(workload.OperationType.Refresh,
                           ch_runners.ClickHouseNoOp("refresh"), async_runner=True)
    runner.register_runner(workload.OperationType.ForceMerge,
                           ch_runners.ClickHouseOptimizeTable(), async_runner=True)
    # ClusterHealth wraps in Retry: preserves retry-until-success/retry-on-error
    # (return-value based, engine-agnostic) that workloads use for bootstrap.
    runner.register_runner(workload.OperationType.ClusterHealth,
                           Retry(ch_runners.ClickHouseClusterHealth()), async_runner=True)
    runner.register_runner(workload.OperationType.IndexStats,
                           ch_runners.ClickHouseSystemParts(), async_runner=True)

    # NoOp stubs.
    runner.register_runner("warmup-knn-indices",
                           ch_runners.ClickHouseNoOp("warmup-knn-indices"), async_runner=True)
    for op_type in [workload.OperationType.PutPipeline, workload.OperationType.DeletePipeline,
                    workload.OperationType.CreateSearchPipeline]:
        runner.register_runner(op_type, ch_runners.ClickHouseNoOp(str(op_type)), async_runner=True)
    runner.register_runner(workload.OperationType.PutSettings,
                           ch_runners.ClickHouseNoOp("put-settings"), async_runner=True)


def wait_for_client(ch_client, max_attempts=40):
    """Wait for ClickHouse /ping (or SELECT 1) to become available.

    Delegates to the helpers function that implements the polling loop.
    """
    # pylint: disable=import-outside-toplevel
    from osbenchmark.engine.clickhouse.helpers import wait_for_clickhouse
    return wait_for_clickhouse(ch_client, max_attempts=max_attempts)


def on_execute_error(e):
    """Translate clickhouse-connect / httpx exceptions to OSB's (ops, unit, meta, fatal) tuple.

    Returns None if the exception is not engine-specific.
    """
    # pylint: disable=import-outside-toplevel
    try:
        import httpx
        if isinstance(e, httpx.ConnectError):
            return 0, "ops", {"success": False, "error-type": "transport",
                              "error-description": f"ClickHouse connection error: {e}"}, True
        if isinstance(e, httpx.TimeoutException):
            return 0, "ops", {"success": False, "error-type": "transport",
                              "error-description": "ClickHouse request timed out"}, False
        if isinstance(e, httpx.HTTPStatusError):
            return 0, "ops", {"success": False, "error-type": "transport",
                              "http-status": e.response.status_code,
                              "error-description": str(e)}, False
    except ImportError:
        pass

    try:
        import clickhouse_connect.driver.exceptions as ch_exc  # pylint: disable=import-outside-toplevel
        if isinstance(e, ch_exc.ClickHouseError):
            return 0, "ops", {"success": False, "error-type": "clickhouse",
                              "error-description": str(e)}, False
    except ImportError:
        pass

    return None
