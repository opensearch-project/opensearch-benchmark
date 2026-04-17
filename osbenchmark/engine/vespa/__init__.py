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
Vespa engine module.

Exposes the engine-registry interface for Vespa: create_client_factory,
create_async_client, register_runners, wait_for_client, on_execute_error.

Vespa runners expect bodies to already contain pre-translated YQL. The
vectorsearch workload's `vespa-search-only` test procedure generates such
bodies via the `vespa-vector-search-param-source` param source. Workloads
that send OpenSearch-style DSL queries to the Vespa runner will fail with a
clear error pointing at the Vespa-native procedure.
"""

from osbenchmark.engine.vespa.client import VespaClientFactory, VespaDatabaseClient  # noqa: F401


def create_client_factory(hosts, client_options):
    """Return a VespaClientFactory. Its .create() returns a sync VespaDatabaseClient."""
    return VespaClientFactory(hosts, client_options)


def create_async_client(hosts, client_options, cfg=None):
    """Return an async Vespa client. cfg is accepted for interface symmetry but not used."""
    return VespaClientFactory(hosts, client_options).create_async()


def register_runners():
    """Register Vespa runners, overriding OS defaults for shared operation types.

    Called AFTER runner.register_default_runners() so Vespa implementations win
    over OS defaults for ops that both engines support (Bulk, Search, VectorSearch,
    Refresh, ForceMerge, ClusterHealth, IndexStats, CreateIndex, DeleteIndex).
    """
    # pylint: disable=import-outside-toplevel
    from osbenchmark import workload
    from osbenchmark.worker_coordinator import runner
    from osbenchmark.engine.vespa import runners as vespa_runners

    runner.register_runner(workload.OperationType.Bulk, vespa_runners.VespaBulkIndex(), async_runner=True)
    runner.register_runner(workload.OperationType.Search, vespa_runners.VespaQuery(), async_runner=True)
    runner.register_runner(workload.OperationType.VectorSearch, vespa_runners.VespaVectorSearch(), async_runner=True)
    runner.register_runner(workload.OperationType.PaginatedSearch, vespa_runners.VespaQuery(), async_runner=True)
    runner.register_runner(workload.OperationType.ScrollSearch, vespa_runners.VespaScrollQuery(), async_runner=True)
    runner.register_runner(workload.OperationType.BulkVectorDataSet, vespa_runners.VespaBulkVectorDataSet(), async_runner=True)
    runner.register_runner(workload.OperationType.CreateIndex, vespa_runners.VespaCreateIndex(), async_runner=True)
    runner.register_runner(workload.OperationType.DeleteIndex, vespa_runners.VespaDeleteIndex(), async_runner=True)
    runner.register_runner(workload.OperationType.Refresh, vespa_runners.VespaRefresh(), async_runner=True)
    runner.register_runner(workload.OperationType.ForceMerge, vespa_runners.VespaForceMerge(), async_runner=True)
    runner.register_runner(workload.OperationType.ClusterHealth, vespa_runners.VespaClusterHealth(), async_runner=True)
    runner.register_runner(workload.OperationType.IndexStats, vespa_runners.VespaIndicesStats(), async_runner=True)


def wait_for_client(vespa_client, max_attempts=40):
    """Wait for Vespa /ApplicationStatus to become available.

    Delegates to the helpers function that implements the polling loop.
    """
    # pylint: disable=import-outside-toplevel
    from osbenchmark.engine.vespa.helpers import wait_for_vespa
    return wait_for_vespa(vespa_client, max_attempts=max_attempts)


def on_execute_error(e):
    """Translate Vespa/httpx exceptions to OSB's (ops, unit, meta, fatal) tuple.

    Returns None if the exception is not engine-specific.
    """
    # pylint: disable=import-outside-toplevel
    try:
        import httpx
        if isinstance(e, httpx.ConnectError):
            return 0, "ops", {"success": False, "error-type": "transport",
                              "error-description": f"Vespa connection error: {e}"}, True
        if isinstance(e, httpx.TimeoutException):
            return 0, "ops", {"success": False, "error-type": "transport",
                              "error-description": "Vespa request timed out"}, False
        if isinstance(e, httpx.HTTPStatusError):
            return 0, "ops", {"success": False, "error-type": "transport",
                              "http-status": e.response.status_code,
                              "error-description": str(e)}, False
    except ImportError:
        pass

    try:
        from vespa.exceptions import VespaError
        if isinstance(e, VespaError):
            return 0, "ops", {"success": False, "error-type": "vespa",
                              "error-description": str(e)}, False
    except ImportError:
        pass

    return None
