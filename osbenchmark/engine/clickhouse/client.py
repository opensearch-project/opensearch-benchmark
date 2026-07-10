# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""ClickHouse database client for OpenSearch Benchmark.

See the sibling engine/vespa/client.py for the pattern this file follows.
"""

from __future__ import annotations  # ensure all type hints are stringified

import asyncio
import logging
from typing import Any, Dict, List, Optional

from osbenchmark import exceptions
from osbenchmark.context import RequestContextHolder

try:
    import clickhouse_connect  # type: ignore
    CLICKHOUSE_CONNECT_AVAILABLE = True
except ImportError:
    clickhouse_connect = None  # type: ignore
    CLICKHOUSE_CONNECT_AVAILABLE = False


class ClickHouseDatabaseClient(RequestContextHolder):
    """Async ClickHouse client that duck-types the OSB engine client interface.

    Follows the Vespa pattern: self.indices = self, self.cluster = self, etc.,
    so runners can call client.indices.create(...) and resolve to methods on
    this class directly. Inherits from RequestContextHolder (same as
    VespaDatabaseClient) so OSB can attach request lifecycle hooks; multi-engine
    does NOT have a DatabaseClient ABC to satisfy, so duck-typing is sufficient
    for the runner interface.
    """

    def __init__(self, hosts: List[Dict], client_options: Dict) -> None:
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self._hosts = hosts
        self.client_options = client_options or {}
        self._client = None  # clickhouse_connect.AsyncClient, initialized lazily
        self._sync_client = None  # clickhouse_connect.Client, for info()/wait_for_client
        self._client_lock = asyncio.Lock()  # prevents duplicate init under contention
        self._database = None

        # namespace proxies (Vespa pattern). See "nodes_stats" below for the sync stub.
        self.indices = self
        self.cluster = self
        self.transport = self
        self.nodes = self

    # methods stubbed here — filled in P3


class ClickHouseClientFactory:
    """Factory called by osbenchmark.engine.clickhouse.create_client_factory."""

    def __init__(self, hosts: List[Dict], client_options: Dict) -> None:
        self.hosts = hosts
        self.client_options = client_options or {}

    def create(self) -> "ClickHouseDatabaseClient":
        return ClickHouseDatabaseClient(self.hosts, self.client_options)

    def create_async(self) -> "ClickHouseDatabaseClient":
        return ClickHouseDatabaseClient(self.hosts, self.client_options)

    def wait_for_rest_layer(self, max_attempts: int = 40) -> bool:
        raise NotImplementedError  # filled in P3
