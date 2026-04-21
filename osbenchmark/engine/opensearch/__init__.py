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
OpenSearch engine module.

All code here is thin delegation to the existing osbenchmark.client and
osbenchmark.worker_coordinator.runner modules. Those modules live at their
historical 2.1 paths (restored by upstream PR #1039) because the workloads
repository imports them directly:

    from osbenchmark.worker_coordinator.runner import PutSettings, Retry, Runner
    from osbenchmark.client import RequestContextHolder

Never relocate `osbenchmark/client.py` or `osbenchmark/worker_coordinator/runner.py`
— they are part of the public OSB surface. This engine module just provides
the engine-registry interface on top.
"""

from osbenchmark import client
from osbenchmark.utils import opts
from osbenchmark.worker_coordinator import runner


def create_client_factory(hosts, client_options):
    """Return an OsClientFactory instance. .create() gives a sync client."""
    return client.OsClientFactory(hosts, client_options)


def create_async_client(hosts, client_options, cfg=None):
    """Return an async OS client with REST + gRPC wired up via UnifiedClient.

    Reads cfg["client"]["grpc_hosts"] if available; falls back to
    localhost:9400 (matching existing default behavior).
    """
    rest_factory = client.OsClientFactory(hosts, client_options)
    grpc_hosts = None
    if cfg is not None:
        grpc_hosts = cfg.opts("client", "grpc_hosts", mandatory=False)
    if not grpc_hosts or not grpc_hosts.all_hosts:
        grpc_hosts = opts.TargetHosts("localhost:9400")
    return client.UnifiedClientFactory(rest_factory, grpc_hosts).create_async()


def register_runners():
    """Register OS runners with the runner registry.

    Delegates to runner.register_default_runners() which registers ~40 OS
    runners (BulkIndex, Query, ForceMerge, etc.). That function lives at the
    2.1 path and is imported by workload plugins — don't relocate.
    """
    runner.register_default_runners()


def wait_for_client(os_client, max_attempts=40):
    """Wait for OpenSearch REST API to become available."""
    return client.wait_for_rest_layer(os_client, max_attempts=max_attempts)


def on_execute_error(e):
    """Translate an opensearchpy exception to OSB's (ops, unit, meta, fatal) tuple.

    Returns None if the exception is not an opensearchpy transport error —
    the caller should fall through to generic error handling.
    """
    # pylint: disable=import-outside-toplevel
    import opensearchpy
    if isinstance(e, opensearchpy.TransportError):
        # pylint: disable=unidiomatic-typecheck
        fatal = type(e) is opensearchpy.ConnectionError
        meta = {"success": False, "error-type": "transport"}
        if isinstance(e.status_code, int):
            meta["http-status"] = e.status_code
        if isinstance(e, opensearchpy.ConnectionTimeout):
            meta["error-description"] = "network connection timed out"
        elif e.info:
            meta["error-description"] = f"{e.error} ({e.info})"
        else:
            desc = e.error.decode("utf-8") if isinstance(e.error, bytes) else str(e.error)
            meta["error-description"] = desc
        return 0, "ops", meta, fatal
    return None
