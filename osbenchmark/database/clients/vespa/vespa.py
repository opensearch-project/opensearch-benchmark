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
Vespa database client implementation for OpenSearch Benchmark.

Thin client: connection/session management only. All query translation,
document transformation, and response conversion happens in helpers.py
and the runner layer.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

import requests

from osbenchmark import exceptions

try:
    from vespa.application import Vespa as PyvespaApp
    PYVESPA_AVAILABLE = True
except ImportError:
    PyvespaApp = None
    PYVESPA_AVAILABLE = False
from osbenchmark.context import RequestContextHolder
from osbenchmark.database.interface import (
    DatabaseClient,
    IndicesNamespace,
    ClusterNamespace,
    TransportNamespace,
    NodesNamespace,
)
from osbenchmark.database.clients.vespa.helpers import (
    convert_metrics_to_stats,
    wait_for_vespa,
)


class VespaClientFactory:
    """Factory for creating Vespa client instances."""

    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = dict(client_options)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Creating Vespa client connected to %s with options [%s]", hosts, dict(client_options))

    def create(self):
        """Create a sync-capable Vespa client (for telemetry)."""
        return self.create_async()

    def create_async(self):
        """Create an async Vespa client."""
        if not self.hosts:
            raise exceptions.SystemSetupError("No Vespa hosts configured")

        host_config = self.hosts[0] if isinstance(self.hosts, list) else self.hosts.get("default", [{}])[0]
        host = host_config.get("host", "localhost")
        port = host_config.get("port", 8080)
        scheme = host_config.get("scheme", "http")

        return VespaDatabaseClient(
            endpoint=f"{scheme}://{host}:{port}",
            **self.client_options
        )

    def wait_for_rest_layer(self, max_attempts=40):
        """Wait for Vespa REST API to become available."""
        client = self.create()
        return wait_for_vespa(client, max_attempts)


class VespaDatabaseClient(DatabaseClient, RequestContextHolder):
    """Async Vespa client implementing the DatabaseClient interface.

    Thin HTTP wrapper — all translation logic lives in helpers.py.
    """

    def __init__(self, endpoint: str, **client_options):
        self.endpoint = endpoint.rstrip('/')
        self.client_options = client_options
        self.logger = logging.getLogger(__name__)
        self._session = None
        self._session_initialized = False
        self._app_name = client_options.get("app_name", "default")
        self._namespace = client_options.get("namespace", "benchmark")
        self._cluster = client_options.get("cluster", None)
        self._request_context = {}

        self._pyvespa_app = None
        self._pyvespa_async = None
        self._pyvespa_semaphore = None

        self._indices_ns = VespaIndicesNamespace(self)
        self._cluster_ns = VespaClusterNamespace(self)
        self._transport_ns = VespaTransportNamespace(self)
        self._nodes_ns = VespaNodesNamespace(self)

    # --- Session management ---

    async def _ensure_session(self):
        """Lazy-init aiohttp.ClientSession with trace hooks for timing."""
        if self._session_initialized:
            return

        try:
            import aiohttp

            async def on_request_start(session, trace_config_ctx, params):
                try:
                    VespaDatabaseClient.on_request_start()
                except LookupError:
                    pass

            async def on_request_end(session, trace_config_ctx, params):
                try:
                    VespaDatabaseClient.on_request_end()
                except LookupError:
                    pass

            trace_config = aiohttp.TraceConfig()
            trace_config.on_request_start.append(on_request_start)
            trace_config.on_request_end.append(on_request_end)
            trace_config.on_request_exception.append(on_request_end)

            connector = aiohttp.TCPConnector(limit=100, limit_per_host=100, force_close=False)
            self._session = aiohttp.ClientSession(
                trace_configs=[trace_config],
                connector=connector,
            )
            self._session_initialized = True
        except ImportError:
            self._session_initialized = True
            self.logger.warning("aiohttp not available, using synchronous requests")

    async def __aenter__(self):
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def close(self):
        """Close aiohttp and pyvespa sessions."""
        if self._session:
            await self._session.close()
            self._session = None
            self._session_initialized = False
        if self._pyvespa_async is not None:
            try:
                if hasattr(self._pyvespa_async, '_close_httpx_client'):
                    await self._pyvespa_async._close_httpx_client()
                elif hasattr(self._pyvespa_async, '__aexit__'):
                    await self._pyvespa_async.__aexit__(None, None, None)
            except Exception as e:
                self.logger.warning("Error closing pyvespa session: %s", e)
            self._pyvespa_async = None

    # --- pyvespa session management ---

    async def _ensure_pyvespa_session(self, max_workers=64):
        """Lazy-init pyvespa's VespaAsync with HTTP/2 multiplexing.

        Creates the session once; subsequent calls are no-ops.
        """
        if self._pyvespa_async is not None:
            return

        if not PYVESPA_AVAILABLE:
            raise RuntimeError("pyvespa is not installed")

        # Suppress pyvespa's per-request and CA bundle INFO logging
        logging.getLogger("httpr").setLevel(logging.WARNING)

        self._pyvespa_app = PyvespaApp(url=self.endpoint)
        self._pyvespa_async = self._pyvespa_app.asyncio(connections=1, timeout=180)

        if hasattr(self._pyvespa_async, '_open_httpx_client'):
            result = self._pyvespa_async._open_httpx_client()
            if asyncio.iscoroutine(result):
                await result
        elif hasattr(self._pyvespa_async, '__aenter__'):
            await self._pyvespa_async.__aenter__()

        self._pyvespa_semaphore = asyncio.Semaphore(max_workers)

        self.logger.info(
            "pyvespa async session initialized (endpoint=%s, max_workers=%d)",
            self.endpoint, max_workers,
        )

    async def feed_batch(self, documents: List[Dict], schema: str,
                         namespace: Optional[str] = None,
                         max_workers: int = 32, **kwargs) -> Dict:
        """Feed a batch of documents via pyvespa's VespaAsync (HTTP/2).

        Each document should have '_id' and 'fields' keys.
        Retries connection-level errors up to 3 times with backoff.
        Returns {"errors": int, "responses": list}.
        """
        await self._ensure_pyvespa_session(max_workers)
        namespace = namespace or self._namespace

        feed_kwargs = {}
        if self._cluster:
            feed_kwargs["destinationCluster"] = self._cluster

        errors = 0
        responses = []
        max_retries = 3

        async def _feed_one(doc):
            nonlocal errors
            doc_id = str(doc.get("_id", ""))
            fields = doc.get("fields", {})
            for attempt in range(max_retries + 1):
                try:
                    resp = await self._pyvespa_async.feed_data_point(
                        schema=schema,
                        data_id=doc_id,
                        fields=fields,
                        namespace=namespace,
                        semaphore=self._pyvespa_semaphore,
                        **feed_kwargs,
                    )
                    status = getattr(resp, "status_code", 200)
                    if status >= 400:
                        errors += 1
                        resp_json = getattr(resp, "json", None)
                        if errors <= 3:
                            self.logger.warning(
                                "Vespa feed error for doc %s: status=%d, response=%s",
                                doc_id, status, resp_json)
                    responses.append({"_id": doc_id, "status": status})
                    return
                except (UnicodeEncodeError, UnicodeDecodeError) as e:
                    # Data encoding issues (e.g. lone surrogates) — don't retry
                    if errors <= 3:
                        self.logger.warning(
                            "Vespa feed encoding error for doc %s (skipping): %s",
                            doc_id, e)
                    errors += 1
                    responses.append({"_id": doc_id, "error": str(e)})
                    return
                except Exception as e:
                    if attempt < max_retries:
                        await asyncio.sleep(0.5 * (2 ** attempt))
                        continue
                    self.logger.warning("pyvespa feed error for doc %s (after %d retries): %s",
                                        doc_id, max_retries, e)
                    errors += 1
                    responses.append({"_id": doc_id, "error": str(e)})

        tasks = [_feed_one(doc) for doc in documents]
        await asyncio.gather(*tasks, return_exceptions=False)

        return {"errors": errors, "responses": responses}

    # --- Namespace properties ---

    @property
    def indices(self) -> "VespaIndicesNamespace":
        return self._indices_ns

    @property
    def cluster(self) -> "VespaClusterNamespace":
        return self._cluster_ns

    @property
    def transport(self) -> "VespaTransportNamespace":
        return self._transport_ns

    @property
    def nodes(self) -> "VespaNodesNamespace":
        return self._nodes_ns

    # --- Core document operations ---

    async def bulk(self, body, index=None, doc_type=None, params=None, **kwargs):
        """Post a list of pre-prepared Vespa documents.

        Accepts already-transformed documents (list of dicts with _id, fields).
        Concurrency managed by the caller (runner uses asyncio.Semaphore).
        """
        await self._ensure_session()

        document_type = index or self._app_name
        endpoint = f"{self.endpoint}/document/v1/{self._namespace}/{document_type}/docid"
        timeout_val = kwargs.get("request_timeout", 30)

        query_params = {}
        if self._cluster:
            query_params["destinationCluster"] = self._cluster

        if not isinstance(body, (list, tuple)):
            body = [body]

        items = []
        errors_count = 0

        for i, doc in enumerate(body):
            doc_id = doc.get("_id", f"doc_{i}")
            doc_endpoint = f"{endpoint}/{doc_id}"
            source = doc.get("_source", doc.get("fields", doc))

            if "index" in source:
                source = {k: v for k, v in source.items() if k != "index"}

            vespa_doc = {"fields": source}

            try:
                if self._session:
                    async with self._session.post(
                        doc_endpoint, json=vespa_doc,
                        params=query_params, timeout=timeout_val
                    ) as response:
                        await response.text()
                        if response.status >= 400:
                            items.append({"index": {"_id": doc_id, "status": response.status}})
                            errors_count += 1
                        else:
                            items.append({"index": {"_id": doc_id, "status": 200}})
                else:
                    resp = requests.post(doc_endpoint, json=vespa_doc,
                                         params=query_params, timeout=timeout_val)
                    status = resp.status_code
                    if status >= 400:
                        items.append({"index": {"_id": doc_id, "status": status}})
                        errors_count += 1
                    else:
                        items.append({"index": {"_id": doc_id, "status": 200}})
            except Exception as e:
                self.logger.warning("Error feeding document %s: %s", doc_id, e)
                items.append({"index": {"_id": doc_id, "error": str(e)}})
                errors_count += 1

        return {
            "took": 0,
            "errors": errors_count > 0,
            "items": items,
        }

    async def index(self, index, body, id=None, doc_type=None, **kwargs):
        """Index a single document via POST to /document/v1/."""
        await self._ensure_session()

        document_type = index or self._app_name
        doc_id = id or "doc_0"
        endpoint = f"{self.endpoint}/document/v1/{self._namespace}/{document_type}/docid/{doc_id}"
        timeout_val = kwargs.get("request_timeout", 30)

        query_params = {}
        if self._cluster:
            query_params["destinationCluster"] = self._cluster

        vespa_doc = {"fields": body}

        if self._session:
            async with self._session.post(
                endpoint, json=vespa_doc,
                params=query_params, timeout=timeout_val
            ) as response:
                resp_text = await response.text()
                if response.status >= 400:
                    raise exceptions.BenchmarkError(
                        f"Failed to index document {doc_id}: status={response.status}, body={resp_text}")
                return {"_id": doc_id, "result": "created", "_version": 1}
        else:
            resp = requests.post(endpoint, json=vespa_doc,
                                 params=query_params, timeout=timeout_val)
            if resp.status_code >= 400:
                raise exceptions.BenchmarkError(
                    f"Failed to index document {doc_id}: status={resp.status_code}, body={resp.text}")
            return {"_id": doc_id, "result": "created", "_version": 1}

    async def update(self, index, body, id, doc_type=None, **kwargs):
        """Partial update a document via PUT to /document/v1/.

        Wraps field values with {"assign": value} for Vespa partial update semantics.
        """
        await self._ensure_session()

        document_type = index or self._app_name
        endpoint = f"{self.endpoint}/document/v1/{self._namespace}/{document_type}/docid/{id}"
        timeout_val = kwargs.get("request_timeout", 30)

        query_params = {}
        if self._cluster:
            query_params["destinationCluster"] = self._cluster

        # Wrap fields with assign for partial update
        update_fields = {field: {"assign": value} for field, value in body.items()}
        vespa_doc = {"fields": update_fields}

        if self._session:
            async with self._session.put(
                endpoint, json=vespa_doc,
                params=query_params, timeout=timeout_val
            ) as response:
                resp_text = await response.text()
                if response.status >= 400:
                    raise exceptions.BenchmarkError(
                        f"Failed to update document {id}: status={response.status}, body={resp_text}")
                return {"_id": id, "result": "updated", "_version": 1}
        else:
            resp = requests.put(endpoint, json=vespa_doc,
                                params=query_params, timeout=timeout_val)
            if resp.status_code >= 400:
                raise exceptions.BenchmarkError(
                    f"Failed to update document {id}: status={resp.status_code}, body={resp.text}")
            return {"_id": id, "result": "updated", "_version": 1}

    async def search(self, index=None, body=None, doc_type=None, **kwargs):
        """Send pre-built YQL query to Vespa /search/ endpoint.

        Expects body to contain 'yql' and optional query params,
        or raw params dict. The runner handles DSL→YQL conversion.
        """
        await self._ensure_session()

        endpoint = f"{self.endpoint}/search/"
        timeout_str = kwargs.get("request_timeout", "10s")

        if isinstance(body, dict) and "yql" in body:
            params = dict(body)
            params.setdefault("timeout", timeout_str)
        else:
            params = dict(body) if isinstance(body, dict) else {}
            params["timeout"] = timeout_str

        if "request_params" in kwargs:
            params.update(kwargs["request_params"])

        try:
            async with self._session.get(endpoint, params=params) as response:
                return await response.json()
        except Exception as e:
            self.logger.error("Search failed: %s", e)
            raise

    def info(self, **kwargs):
        """GET /ApplicationStatus — synchronous for setup/init contexts."""
        endpoint = f"{self.endpoint}/ApplicationStatus"
        try:
            response = requests.get(endpoint, timeout=10)
            app_status = response.json()
            version = app_status.get("application", {}).get("vespa", {}).get("version", "unknown")
            return {
                "name": "vespa",
                "cluster_name": self._app_name,
                "cluster_uuid": "vespa-cluster",
                "version": {
                    "number": version,
                    "distribution": "vespa",
                    "build_type": "docker",
                    "build_hash": "unknown",
                    "build_date": "unknown",
                    "build_snapshot": False,
                    "build_flavor": "default",
                    "lucene_version": "unknown",
                    "minimum_wire_compatibility_version": version,
                    "minimum_index_compatibility_version": version,
                },
                "tagline": "The Search Engine for Structured Data",
            }
        except Exception as e:
            self.logger.warning("Could not retrieve cluster info: %s", e)
            return {
                "name": "vespa",
                "cluster_name": self._app_name,
                "version": {"number": "unknown", "distribution": "vespa", "build_hash": "unknown"},
            }

    def return_raw_response(self):
        """Mark that raw responses should be returned."""
        self._request_context["raw_response"] = True


# =============================================================================
# Namespace implementations
# =============================================================================

class VespaIndicesNamespace(IndicesNamespace):
    """Index operations — mostly lightweight HTTP calls or no-ops."""

    def __init__(self, client: VespaDatabaseClient):
        self._client = client

    async def create(self, index, body=None, **kwargs):
        # No-op — Vespa schemas are deployed via application packages
        return {"acknowledged": True, "shards_acknowledged": True, "index": index}

    async def delete(self, index, **kwargs):
        # No-op — Vespa schemas are deployed via application packages
        return {"acknowledged": True}

    async def exists(self, index, **kwargs):
        # No-op — Vespa schemas always exist once deployed
        return True

    async def refresh(self, index=None, **kwargs):
        # No-op — Vespa handles visibility internally
        return {"acknowledged": True, "_shards": {"total": 1, "successful": 1, "failed": 0}}

    async def stats(self, index=None, metric=None, **kwargs):
        """Async — GET /metrics/v2/values, convert via helpers."""
        await self._client._ensure_session()
        endpoint = f"{self._client.endpoint}/metrics/v2/values"
        try:
            async with self._client._session.get(endpoint, timeout=10) as response:
                metrics = await response.json()
                return convert_metrics_to_stats(metrics, index)
        except Exception:
            return {"_all": {"primaries": {}, "total": {}}}

    async def forcemerge(self, index=None, **kwargs):
        """No-op, returns task format if polling mode."""
        wait_for_completion = kwargs.get("wait_for_completion", True)
        if wait_for_completion == "false" or wait_for_completion is False:
            return {"task": "vespa-node:1"}
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}


class VespaClusterNamespace(ClusterNamespace):
    """Cluster operations — health mapping."""

    def __init__(self, client: VespaDatabaseClient):
        self._client = client

    async def health(self, **kwargs):
        await self._client._ensure_session()
        endpoint = f"{self._client.endpoint}/state/v1/health"
        try:
            async with self._client._session.get(endpoint) as response:
                health = await response.json()
                status = health.get("status", {}).get("code", "red")
        except Exception as e:
            self._client.logger.error("Health check failed: %s", e)
            return {"cluster_name": "vespa", "status": "red", "timed_out": False}

        status_map = {"up": "green", "down": "red", "initializing": "yellow"}
        return {
            "cluster_name": "vespa",
            "status": status_map.get(status, "yellow"),
            "timed_out": False,
            "number_of_nodes": 1,
            "number_of_data_nodes": 1,
            "active_primary_shards": 1,
            "active_shards": 1,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
        }

    async def put_settings(self, body, **kwargs):
        return {"acknowledged": True}


class VespaTransportNamespace(TransportNamespace):
    """Generic HTTP via session."""

    def __init__(self, client: VespaDatabaseClient):
        self._client = client

    async def perform_request(self, method, url, params=None, body=None, headers=None):
        await self._client._ensure_session()
        full_url = f"{self._client.endpoint}{url}"
        async with self._client._session.request(method, full_url, params=params, json=body, headers=headers) as resp:
            return await resp.json()

    async def close(self):
        await self._client.close()


class VespaNodesNamespace(NodesNamespace):
    """Stub node stats/info for telemetry compatibility."""

    def __init__(self, client: VespaDatabaseClient):
        self._client = client

    def stats(self, node_id=None, metric=None, **kwargs):
        return {
            "nodes": {
                "vespa-node-1": {
                    "name": "vespa-node-1",
                    "host": self._client.endpoint,
                    "os": {"cpu": {"percent": 0}},
                    "jvm": {
                        "mem": {
                            "heap_used_percent": 0,
                            "pools": {
                                "young": {"peak_used_in_bytes": 0},
                                "old": {"peak_used_in_bytes": 0},
                                "survivor": {"peak_used_in_bytes": 0},
                            },
                        },
                        "gc": {
                            "collectors": {
                                "young": {"collection_time_in_millis": 0, "collection_count": 0},
                                "old": {"collection_time_in_millis": 0, "collection_count": 0},
                            }
                        },
                    },
                }
            }
        }

    def info(self, node_id=None, metric=None, **kwargs):
        return {
            "nodes": {
                "vespa-node-1": {
                    "name": "vespa-node-1",
                    "host": self._client.endpoint,
                    "version": "8.0.0",
                    "os": {"name": "Linux"},
                    "jvm": {
                        "version": "17.0.0",
                        "gc": {
                            "collectors": {
                                "young": "G1 Young Generation",
                                "old": "G1 Old Generation",
                            }
                        },
                    },
                }
            }
        }
