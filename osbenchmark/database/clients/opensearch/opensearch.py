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
OpenSearch DatabaseClient adapter for the multi-database abstraction layer.

This module provides ``OpenSearchClientFactory`` (the factory class registered
in ``osbenchmark.database.registry``) and the namespace adapter classes that
wrap an opensearchpy client to satisfy the ``DatabaseClient`` interface.

The canonical OpenSearch client implementation lives at the historical
``osbenchmark.client`` path so that workloads imports keep working unchanged.
This module re-exports the symbols it needs from there rather than duplicating
them, which avoids drift between the two locations.
"""

import logging

# Re-export the canonical OpenSearch client and helpers from osbenchmark.client.
# The implementations live there at the 2.1 path so that workload plugins (and
# anything else relying on the historical import path) keep working unchanged.
# Re-exporting from a single source of truth prevents drift between the two
# locations (e.g. the lazy-grpc-import fix landing in only one of them).
from osbenchmark.client import (  # noqa: F401  pylint: disable=unused-import
    OsClientFactory,
    UnifiedClient,
    UnifiedClientFactory,
    GrpcClientFactory,
    MessageProducerFactory,
    wait_for_rest_layer,
)

# pylint: disable=wrong-import-position
from osbenchmark.database.interface import (
    DatabaseClient,
    IndicesNamespace,
    ClusterNamespace,
    TransportNamespace,
    NodesNamespace
)


class OpenSearchIndicesNamespace(IndicesNamespace):
    """Wrapper for opensearchpy indices namespace"""

    def __init__(self, opensearch_indices):
        self._indices = opensearch_indices

    async def create(self, index, body=None, **kwargs):
        return await self._indices.create(index=index, body=body, **kwargs)

    async def delete(self, index, **kwargs):
        return await self._indices.delete(index=index, **kwargs)

    async def exists(self, index, **kwargs):
        return await self._indices.exists(index=index, **kwargs)

    async def refresh(self, index=None, **kwargs):
        return await self._indices.refresh(index=index, **kwargs)

    async def stats(self, index=None, metric=None, **kwargs):  # pylint: disable=invalid-overridden-method
        return await self._indices.stats(index=index, metric=metric, **kwargs)

    async def forcemerge(self, index=None, **kwargs):
        return await self._indices.forcemerge(index=index, **kwargs)

    def __getattr__(self, name):
        """Delegate unknown attributes to the underlying indices namespace"""
        return getattr(self._indices, name)


class OpenSearchClusterNamespace(ClusterNamespace):
    """Wrapper for opensearchpy cluster namespace"""

    def __init__(self, opensearch_cluster):
        self._cluster = opensearch_cluster

    async def health(self, **kwargs):
        return await self._cluster.health(**kwargs)

    async def put_settings(self, body, **kwargs):
        return await self._cluster.put_settings(body=body, **kwargs)

    def __getattr__(self, name):
        """Delegate unknown attributes to the underlying cluster namespace"""
        return getattr(self._cluster, name)


class OpenSearchTransportNamespace(TransportNamespace):
    """Wrapper for opensearchpy transport namespace"""

    def __init__(self, opensearch_transport):
        self._transport = opensearch_transport

    async def perform_request(self, method, url, params=None, body=None, headers=None):
        return await self._transport.perform_request(
            method=method,
            url=url,
            params=params,
            body=body,
            headers=headers
        )

    def __getattr__(self, name):
        """Delegate unknown attributes to the underlying transport namespace"""
        return getattr(self._transport, name)


class OpenSearchNodesNamespace(NodesNamespace):
    """Wrapper for opensearchpy nodes namespace"""

    def __init__(self, opensearch_nodes):
        self._nodes = opensearch_nodes

    def stats(self, node_id=None, metric=None, **kwargs):
        return self._nodes.stats(node_id=node_id, metric=metric, **kwargs)

    def info(self, node_id=None, metric=None, **kwargs):
        return self._nodes.info(node_id=node_id, metric=metric, **kwargs)

    def __getattr__(self, name):
        """Delegate unknown attributes to the underlying nodes namespace"""
        return getattr(self._nodes, name)


class OpenSearchDatabaseClient(DatabaseClient):
    """
    DatabaseClient implementation for OpenSearch.

    This is a transparent wrapper around the opensearchpy client that implements
    the DatabaseClient interface. It delegates all operations to the underlying
    opensearchpy client with minimal overhead.
    """

    def __init__(self, opensearch_client):
        """
        Initialize with an opensearchpy client instance.

        Args:
            opensearch_client: An instance of opensearchpy.AsyncOpenSearch or UnifiedClient
        """
        self._client = opensearch_client

        # Wrap namespaces
        self._indices_ns = OpenSearchIndicesNamespace(opensearch_client.indices)
        self._cluster_ns = OpenSearchClusterNamespace(opensearch_client.cluster)
        self._transport_ns = OpenSearchTransportNamespace(opensearch_client.transport)
        self._nodes_ns = OpenSearchNodesNamespace(opensearch_client.nodes)

    @property
    def indices(self):
        return self._indices_ns

    @property
    def cluster(self):
        return self._cluster_ns

    @property
    def transport(self):
        return self._transport_ns

    @property
    def nodes(self):
        return self._nodes_ns

    async def bulk(self, body, index=None, doc_type=None, params=None, **kwargs):
        # Note: doc_type is deprecated and removed in opensearchpy 2.x
        # We accept it for backwards compatibility but don't pass it through
        return await self._client.bulk(
            body=body,
            index=index,
            params=params,
            **kwargs
        )

    async def index(self, index, body, id=None, doc_type=None, **kwargs):
        # Note: doc_type is deprecated and removed in opensearchpy 2.x
        # We accept it for backwards compatibility but don't pass it through
        return await self._client.index(
            index=index,
            body=body,
            id=id,
            **kwargs
        )

    async def search(self, index=None, body=None, doc_type=None, **kwargs):
        # Note: doc_type is deprecated and removed in opensearchpy 2.x
        # We accept it for backwards compatibility but don't pass it through
        return await self._client.search(
            index=index,
            body=body,
            **kwargs
        )

    def info(self):
        """Get cluster information from OpenSearch"""
        return self._client.info()

    def return_raw_response(self):
        """Delegate to underlying client if method exists"""
        if hasattr(self._client, 'return_raw_response'):
            return self._client.return_raw_response()

    def close(self):
        """Delegate to underlying client if method exists"""
        if hasattr(self._client, 'close'):
            return self._client.close()

    def __getattr__(self, name):
        """
        Delegate any unknown attributes to the underlying OpenSearch client.
        This ensures full compatibility with operations that aren't in the interface.
        """
        return getattr(self._client, name)


class OpenSearchClientFactory:
    """
    Factory for creating OpenSearch database clients.

    This factory wraps the legacy OsClientFactory and UnifiedClientFactory
    to create clients that implement the DatabaseClient interface.
    """

    def __init__(self, hosts, client_options, grpc_hosts=None):
        """
        Initialize factory with connection parameters.

        Args:
            hosts: List of host dictionaries with "host" and "port" keys
            client_options: Dictionary of client-specific options
            grpc_hosts: Optional TargetHosts for gRPC. When provided, the
                returned client wraps both REST and gRPC stubs via UnifiedClient,
                making `search_service()` available for proto-vector-search runners.
        """
        self.hosts = hosts
        self.client_options = client_options
        self.grpc_hosts = grpc_hosts
        self.logger = logging.getLogger(__name__)

    def create_async(self):
        """
        Create an async OpenSearch client that implements DatabaseClient interface.

        When grpc_hosts is set, the underlying client is a UnifiedClient that
        exposes both REST methods (via __getattr__ delegation) and gRPC stubs
        (via search_service()/document_service()). Otherwise, only REST is wired.

        Returns:
            OpenSearchDatabaseClient wrapping the underlying client
        """
        os_factory = OsClientFactory(self.hosts, self.client_options)

        if self.grpc_hosts:
            unified_factory = UnifiedClientFactory(os_factory, self.grpc_hosts)
            opensearch_client = unified_factory.create_async()
        else:
            opensearch_client = os_factory.create_async()

        return OpenSearchDatabaseClient(opensearch_client)

    def create(self):
        """
        Create a synchronous OpenSearch client.

        Used for telemetry and pre-benchmark operations.
        Returns the native opensearchpy sync client (not wrapped) since
        telemetry expects synchronous methods.

        Returns:
            opensearchpy.OpenSearch sync client
        """
        os_factory = OsClientFactory(self.hosts, self.client_options)
        return os_factory.create()

    def wait_for_rest_layer(self, max_attempts=40):
        """
        Wait for OpenSearch's REST API to become available.

        Args:
            max_attempts: Maximum number of attempts to check availability.

        Returns:
            True if REST API is available, False otherwise.
        """
        # Use legacy OsClientFactory to create a sync client for health check
        os_factory = OsClientFactory(self.hosts, self.client_options)
        opensearch_client = os_factory.create()

        # Use the module-level wait_for_rest_layer function
        return wait_for_rest_layer(opensearch_client, max_attempts)
