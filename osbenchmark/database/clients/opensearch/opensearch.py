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

import logging
import time

import certifi
import opensearchpy
import urllib3
from urllib3.util.ssl_ import is_ipaddress

import grpc
from opensearch.protobufs.services.document_service_pb2_grpc import DocumentServiceStub
from opensearch.protobufs.services.search_service_pb2_grpc import SearchServiceStub

from osbenchmark.kafka_client import KafkaMessageProducer
from osbenchmark import exceptions, doc_link, async_connection
from osbenchmark.context import RequestContextHolder
from osbenchmark.utils import console, convert
from osbenchmark.cloud_provider import CloudProviderFactory

class OsClientFactory:
    """
    Abstracts how the OpenSearch client is created. Intended for testing.
    """
    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = dict(client_options)
        self.ssl_context = None
        self.provider = CloudProviderFactory.get_provider_from_client_options(self.client_options)
        self.logger = logging.getLogger(__name__)

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        if self.provider:
            self.provider.parse_log_in_params(client_options=self.client_options)
            self.provider.mask_client_options(masked_client_options, self.client_options)
            self.logger.info("Masking client options with cloud provider: [%s]", self.provider)

        self.logger.info("Creating OpenSearch client connected to %s with options [%s]", hosts, masked_client_options)
        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        if self.client_options.pop("use_ssl", False):
            # pylint: disable=import-outside-toplevel
            import ssl
            self.logger.info("SSL support: on")
            self.client_options["scheme"] = "https"

            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                                          cafile=self.client_options.pop("ca_certs", certifi.where()))

            if not self.client_options.pop("verify_certs", True):
                self.logger.info("SSL certificate verification: off")
                # order matters to avoid ValueError: check_hostname needs a SSL context with either CERT_OPTIONAL or CERT_REQUIRED
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE

                self.logger.warning("User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a "
                                    "benchmark. Disabling urllib warnings now to avoid a logging storm. "
                                    "See https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings for details.")
                # disable:  "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly \
                # advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings"
                urllib3.disable_warnings()
            else:
                # The peer's hostname can be matched if only a hostname is provided.
                # In other words, hostname checking is disabled if an IP address is
                # found in the host lists.
                self.ssl_context.check_hostname = self._has_only_hostnames(hosts)
                self.ssl_context.verify_mode=ssl.CERT_REQUIRED
                self.logger.info("SSL certificate verification: on")

            # When using SSL_context, all SSL related kwargs in client options get ignored
            client_cert = self.client_options.pop("client_cert", False)
            client_key = self.client_options.pop("client_key", False)

            if not client_cert and not client_key:
                self.logger.info("SSL client authentication: off")
            elif bool(client_cert) != bool(client_key):
                self.logger.error(
                    "Supplied client-options contain only one of client_cert/client_key. "
                )
                defined_client_ssl_option = "client_key" if client_key else "client_cert"
                missing_client_ssl_option = "client_cert" if client_key else "client_key"
                console.println(
                    "'{}' is missing from client-options but '{}' has been specified.\n"
                    "If your OpenSearch setup requires client certificate verification both need to be supplied.\n"
                    "Read the documentation at {}\n".format(
                        missing_client_ssl_option,
                        defined_client_ssl_option,
                        console.format.link(doc_link("command_line_reference.html#client-options")))
                )
                raise exceptions.SystemSetupError(
                    "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                        defined_client_ssl_option,
                        missing_client_ssl_option))
            elif client_cert and client_key:
                self.logger.info("SSL client authentication: on")
                self.ssl_context.load_cert_chain(certfile=client_cert,
                                                 keyfile=client_key)
        else:
            self.logger.info("SSL support: off")
            self.client_options["scheme"] = "http"

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            self.logger.info("HTTP basic authentication: on")
            self.client_options["http_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
        else:
            self.logger.info("HTTP basic authentication: off")

        if self._is_set(self.client_options, "compressed"):
            console.warn("You set the deprecated client option 'compressed'. Please use 'http_compress' instead.", logger=self.logger)
            self.client_options["http_compress"] = self.client_options.pop("compressed")

        if self._is_set(self.client_options, "http_compress"):
            self.logger.info("HTTP compression: on")
        else:
            self.logger.info("HTTP compression: off")

        if self._is_set(self.client_options, "enable_cleanup_closed"):
            self.client_options["enable_cleanup_closed"] = convert.to_bool(self.client_options.pop("enable_cleanup_closed"))

    @staticmethod
    def _has_only_hostnames(hosts):
        logger = logging.getLogger(__name__)
        has_ip, has_hostname = False, False
        for host in hosts:
            if is_ipaddress(host["host"]):
                has_ip = True
            else:
                has_hostname = True

        if has_ip and has_hostname:
            console.warn("Although certificate verification is enabled, "
                "peer hostnames will not be matched since the host list is a mix "
                "of names and IP addresses", logger=logger)
            return False

        return has_hostname

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        if self.provider:
            self.logger.info("Creating OpenSearch client with provider %s", self.provider)
            return self.provider.create_client(self.hosts, self.client_options)

        else:
            return opensearchpy.OpenSearch(hosts=self.hosts, ssl_context=self.ssl_context, **self.client_options)

    def create_async(self):
        # pylint: disable=import-outside-toplevel
        import io
        import aiohttp
        from opensearchpy.serializer import JSONSerializer

        class BenchmarkAsyncOpenSearch(opensearchpy.AsyncOpenSearch, RequestContextHolder):
            pass

        class LazyJSONSerializer(JSONSerializer):
            def loads(self, s):
                meta = BenchmarkAsyncOpenSearch.request_context.get()
                if "raw_response" in meta:
                    return io.BytesIO(s)
                else:
                    return super().loads(s)

        async def on_request_start(session, trace_config_ctx, params):
            BenchmarkAsyncOpenSearch.on_request_start()

        async def on_request_end(session, trace_config_ctx, params):
            BenchmarkAsyncOpenSearch.on_request_end()

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        # ensure that we also stop the timer when a request "ends" with an exception (e.g. a timeout)
        trace_config.on_request_exception.append(on_request_end)

        # override the builtin JSON serializer
        self.client_options["serializer"] = LazyJSONSerializer()
        self.client_options["trace_config"] = trace_config

        if self.provider:
            self.logger.info("Creating OpenSearch Async Client with provider %s", self.provider)
            return self.provider.create_client(self.hosts, self.client_options,
                                               client_class=BenchmarkAsyncOpenSearch, use_async=True)
        else:
            return BenchmarkAsyncOpenSearch(hosts=self.hosts,
                                            connection_class=async_connection.AIOHttpConnection,
                                            ssl_context=self.ssl_context,
                                            **self.client_options)


def wait_for_rest_layer(opensearch, max_attempts=40):
    """
    Waits for ``max_attempts`` until OpenSearch's REST API is available.

    :param opensearch: OpenSearch client to use for connecting.
    :param max_attempts: The maximum number of attempts to check whether the REST API is available.
    :return: True iff OpenSearch's REST API is available.
    """
    # assume that at least the hosts that we expect to contact should be available. Note that this is not 100%
    # bullet-proof as a cluster could have e.g. dedicated masters which are not contained in our list of target hosts
    # but this is still better than just checking for any random node's REST API being reachable.
    expected_node_count = len(opensearch.transport.hosts)
    logger = logging.getLogger(__name__)
    for attempt in range(max_attempts):
        logger.debug("REST API is available after %s attempts", attempt)
        # pylint: disable=import-outside-toplevel
        try:
            # see also WaitForHttpResource in OpenSearch tests. Contrary to the ES tests we consider the API also
            # available when the cluster status is RED (as long as all required nodes are present)
            opensearch.cluster.health(wait_for_nodes=">={}".format(expected_node_count))
            logger.info("REST API is available for >= [%s] nodes after [%s] attempts.", expected_node_count, attempt)
            return True
        except opensearchpy.ConnectionError as e:
            if "SSL: UNKNOWN_PROTOCOL" in str(e):
                raise exceptions.SystemSetupError("Could not connect to cluster via https. Is this an https endpoint?", e)
            else:
                logger.debug("Got connection error on attempt [%s]. Sleeping...", attempt)
                time.sleep(3)
        except opensearchpy.TransportError as e:
            # cluster block, our wait condition is not reached
            if e.status_code in (503, 401, 408):
                logger.debug("Got status code [%s] on attempt [%s]. Sleeping...", e.status_code, attempt)
                time.sleep(3)
            elif e.status_code == 404:
                # Serverless does not support the cluster-health API.  Test with _cat/indices for now.
                catclient = opensearchpy.client.cat.CatClient(opensearch)
                try:
                    catclient.indices()
                    return True
                except Exception as e:
                    logger.warning("Encountered exception %s when attempting to probe endpoint health", e)
                    raise e
            else:
                logger.warning("Got unexpected status code [%s] on attempt [%s].", e.status_code, attempt)
                raise e
    return False


class MessageProducerFactory:
    @staticmethod
    async def create(params):
        """
        Creates and returns a message producer based on the ingestion source.
        Currently supports Kafka. Ingestion source should be a dict like:
            {'type': 'kafka', 'param': {'topic': 'test', 'bootstrap-servers': 'localhost:34803'}}
        """
        ingestion_source = params.get("ingestion-source", {})
        producer_type = ingestion_source.get("type", "kafka").lower()
        if producer_type == "kafka":
            return await KafkaMessageProducer.create(params)
        else:
            raise ValueError(f"Unsupported ingestion source type: {producer_type}")


class GrpcClientFactory:
    """
    Factory for creating gRPC client stubs.
    Note gRPC channels must default `use_local_subchannel_pool` to true.
    Sub channels manage the underlying connection with the server. When the global sub channel pool is used gRPC will
    re-use sub channels and their underlying connections which does not appropriately reflect a multi client scenario.
    """
    def __init__(self, grpc_hosts):
        self.grpc_hosts = grpc_hosts
        self.logger = logging.getLogger(__name__)
        self.grpc_channel_options = [
            ('grpc.use_local_subchannel_pool', 1),
            ('grpc.max_send_message_length', 10 * 1024 * 1024),  # 10 MB
            ('grpc.max_receive_message_length', 10 * 1024 * 1024)  # 10 MB
        ]

    def create_grpc_stubs(self):
        """
        Create gRPC service stubs.
        Returns a dict of {cluster_name: {service_name: stub}} structure.
        """
        stubs = {}

        if len(self.grpc_hosts.all_hosts.items()) > 1:
            raise NotImplementedError("Only one gRPC cluster is supported.")

        if len(self.grpc_hosts.all_hosts["default"]) > 1:
            raise NotImplementedError("Only one gRPC host is supported.")

        host = self.grpc_hosts.all_hosts["default"][0]
        grpc_addr = f"{host['host']}:{host['port']}"

        self.logger.info("Creating gRPC channel for cluster default cluster at %s", grpc_addr)
        channel = grpc.aio.insecure_channel(
            target=grpc_addr,
            options=self.grpc_channel_options,
            compression=None
        )

        # Retain a reference to underlying channel in our stubs dictionary for graceful shutdown.
        stubs["default"] = {
            'document_service': DocumentServiceStub(channel),
            'search_service': SearchServiceStub(channel),
            '_channel': channel
        }

        return stubs


class UnifiedClient:
    """
    Unified client that wraps both OpenSearch REST client and gRPC stubs.
    This provides a single interface for runners to access both protocols.
    Acts as a transparent proxy to the OpenSearch client while adding gRPC capabilities.
    """
    def __init__(self, opensearch_client, grpc_stubs=None):
        self._opensearch = opensearch_client
        self._grpc_stubs = grpc_stubs
        self._logger = logging.getLogger(__name__)

    def __getattr__(self, name):
        """Delegate all unknown attributes to the underlying OpenSearch client."""
        return getattr(self._opensearch, name)

    def document_service(self, cluster_name="default"):
        """Get the gRPC DocumentService stub for the specified cluster."""
        if cluster_name in self._grpc_stubs:
            return self._grpc_stubs[cluster_name].get('document_service')
        else:
            raise exceptions.SystemSetupError(
                "gRPC DocumentService not available. Please configure --grpc-target-hosts.")

    def search_service(self, cluster_name="default"):
        """Get the gRPC SearchService stub for the specified cluster."""
        if cluster_name in self._grpc_stubs:
            return self._grpc_stubs[cluster_name].get('search_service')
        else:
            raise exceptions.SystemSetupError(
                "gRPC SearchService not available. Please configure --grpc-target-hosts.")

    def __del__(self):
        """Close all gRPC channels."""
        for cluster_stubs in self._grpc_stubs.values():
            if '_channel' in cluster_stubs:
                try:
                    cluster_stubs['_channel'].close()
                except Exception as e:
                    self._logger.warning("Error closing gRPC channel: %s", e)
        self._opensearch.close()

    @property
    def opensearch(self):
        """Provide access to the underlying OpenSearch client for explicit access."""
        return self._opensearch


class UnifiedClientFactory:
    """
    Factory that creates UnifiedClient instances with both REST and gRPC support.
    """
    def __init__(self, rest_client_factory, grpc_hosts=None):
        self.rest_client_factory = rest_client_factory
        self.grpc_hosts = grpc_hosts
        self.logger = logging.getLogger(__name__)

    def create(self):
        """Non async client is deprecated."""
        raise NotImplementedError()

    def create_async(self):
        """Create a UnifiedClient with async REST client."""
        opensearch_client = self.rest_client_factory.create_async()
        grpc_stubs = None

        if self.grpc_hosts:
            grpc_factory = GrpcClientFactory(self.grpc_hosts)
            grpc_stubs = grpc_factory.create_grpc_stubs()

        return UnifiedClient(opensearch_client, grpc_stubs)


# ============================================================================
# DatabaseClient Interface Implementation for OpenSearch
# ============================================================================

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

    async def forcemerge(self, index=None, **kwargs):  # pylint: disable=invalid-overridden-method
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

    def __init__(self, hosts, client_options):
        """
        Initialize factory with connection parameters.

        Args:
            hosts: List of host dictionaries with "host" and "port" keys
            client_options: Dictionary of client-specific options
        """
        self.hosts = hosts
        self.client_options = client_options
        self.logger = logging.getLogger(__name__)

    def create_async(self):
        """
        Create an async OpenSearch client that implements DatabaseClient interface.

        Returns:
            OpenSearchDatabaseClient wrapping an async opensearchpy client
        """
        # Use legacy OsClientFactory to create the actual client
        os_factory = OsClientFactory(self.hosts, self.client_options)
        opensearch_client = os_factory.create_async()

        # Wrap it in our DatabaseClient interface
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
