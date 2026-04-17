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
# pylint: disable=not-async-context-manager,protected-access

import asyncio
import concurrent.futures
import logging
from typing import Dict, List, Optional

import requests

from osbenchmark import exceptions
from osbenchmark.context import RequestContextHolder
from osbenchmark.engine.vespa.helpers import (
    convert_metrics_to_stats,
    wait_for_vespa,
)

try:
    from vespa.application import Vespa as PyvespaApp
    from vespa.exceptions import VespaError
    PYVESPA_AVAILABLE = True
except ImportError:
    PyvespaApp = None

    class VespaError(Exception):
        """Stub for when pyvespa is not installed."""

    PYVESPA_AVAILABLE = False


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


class VespaDatabaseClient(RequestContextHolder):
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
        self._sync_session = None
        self._search_executor = None

    # --- Session management ---

    async def _ensure_session(self):
        """Lazy-init aiohttp.ClientSession with trace hooks for timing."""
        if self._session_initialized:
            return

        try:
            import aiohttp  # pylint: disable=import-outside-toplevel

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

    async def close(self):  # pylint: disable=invalid-overridden-method
        """Close aiohttp, pyvespa sync (search), and pyvespa async (feed) sessions."""
        if self._session:
            await self._session.close()
            self._session = None
            self._session_initialized = False
        if self._sync_session is not None:
            try:
                if hasattr(self._sync_session, '_close_httpr_client'):
                    self._sync_session._close_httpr_client()  # pylint: disable=protected-access
                elif hasattr(self._sync_session, '_close_httpx_client'):
                    self._sync_session._close_httpx_client()  # pylint: disable=protected-access
                elif hasattr(self._sync_session, '__exit__'):
                    self._sync_session.__exit__(None, None, None)  # pylint: disable=unnecessary-dunder-call
            except Exception as e:
                self.logger.warning("Error closing pyvespa sync session: %s", e)
            self._sync_session = None
        if self._search_executor is not None:
            self._search_executor.shutdown(wait=False)
            self._search_executor = None
        if self._pyvespa_async is not None:
            try:
                if hasattr(self._pyvespa_async, '_close_httpr_client'):
                    await self._pyvespa_async._close_httpr_client()  # pylint: disable=protected-access
                elif hasattr(self._pyvespa_async, '_close_httpx_client'):
                    await self._pyvespa_async._close_httpx_client()  # pylint: disable=protected-access
                elif hasattr(self._pyvespa_async, '__aexit__'):
                    await self._pyvespa_async.__aexit__(None, None, None)
            except Exception as e:
                self.logger.warning("Error closing pyvespa async session: %s", e)
            self._pyvespa_async = None

    # --- pyvespa sync session for search ---

    def _ensure_sync_session(self):
        """Lazy-init pyvespa sync session with httpr (Rust) for search queries.

        Uses compress=False to avoid redundant json.dumps+gzip in Python,
        letting httpr serialize via serde with the GIL released. Achieves
        ~2,800 QPS vs aiohttp's ~934 QPS at 32 clients.

        Same pattern as Milvus (sync SDK + ThreadPoolExecutor).
        """
        if self._sync_session is not None:
            return

        if not PYVESPA_AVAILABLE:
            raise RuntimeError("pyvespa is not installed")

        # Suppress pyvespa's per-request and CA bundle INFO logging
        logging.getLogger("httpr").setLevel(logging.WARNING)

        if self._pyvespa_app is None:
            self._pyvespa_app = PyvespaApp(url=self.endpoint)

        sync_ctx = self._pyvespa_app.syncio(compress=False)

        # Open the persistent HTTP client. VespaSync is a context manager —
        # __enter__ is the documented way to init the httpr client.
        # Try private methods first (more explicit), fall back to __enter__.
        if hasattr(sync_ctx, '_open_httpr_client'):
            sync_ctx._open_httpr_client()  # pylint: disable=protected-access
        elif hasattr(sync_ctx, '_open_httpx_client'):
            sync_ctx._open_httpx_client()  # pylint: disable=protected-access
        elif hasattr(sync_ctx, '__enter__'):
            sync_ctx.__enter__()  # pylint: disable=unnecessary-dunder-call

        self._sync_session = sync_ctx

        max_workers = int(self.client_options.get("max_workers", 64))
        self._search_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="vespa-search"
        )

        self.logger.info(
            "pyvespa sync search session initialized (endpoint=%s, compress=False, max_workers=%d)",
            self.endpoint, max_workers,
        )

    # --- pyvespa async session for feeding ---

    async def _ensure_pyvespa_session(self, max_workers=64):
        """Lazy-init pyvespa's VespaAsync with HTTP/2 multiplexing for feeding.

        Creates the session once; subsequent calls are no-ops.
        """
        if self._pyvespa_async is not None:
            return

        if not PYVESPA_AVAILABLE:
            raise RuntimeError("pyvespa is not installed")

        # Suppress pyvespa's per-request and CA bundle INFO logging
        logging.getLogger("httpr").setLevel(logging.WARNING)

        if self._pyvespa_app is None:
            self._pyvespa_app = PyvespaApp(url=self.endpoint)
        self._pyvespa_async = self._pyvespa_app.asyncio(connections=1, timeout=180)

        # Try new method name first, then old, then context manager fallback
        if hasattr(self._pyvespa_async, '_open_httpr_client'):
            result = self._pyvespa_async._open_httpr_client()  # pylint: disable=protected-access
            if asyncio.iscoroutine(result):
                await result
        elif hasattr(self._pyvespa_async, '_open_httpx_client'):
            result = self._pyvespa_async._open_httpx_client()  # pylint: disable=protected-access
            if asyncio.iscoroutine(result):
                await result
        elif hasattr(self._pyvespa_async, '__aenter__'):
            await self._pyvespa_async.__aenter__()  # pylint: disable=unnecessary-dunder-call

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
        """Send pre-built YQL query to Vespa via pyvespa syncio (httpr Rust client).

        Uses pyvespa syncio(compress=False) dispatched to a ThreadPoolExecutor.
        The httpr Rust engine serializes JSON via serde with the GIL released,
        enabling true thread parallelism. Same pattern as Milvus (sync SDK +
        ThreadPoolExecutor). Falls back to aiohttp POST if pyvespa unavailable.

        Expects body to contain 'yql' and optional query params,
        or raw params dict. The runner handles DSL→YQL conversion.
        """
        if isinstance(body, dict) and "yql" in body:
            params = dict(body)
        else:
            params = dict(body) if isinstance(body, dict) else {}

        timeout_str = kwargs.get("request_timeout", "10s")
        params.setdefault("timeout", timeout_str)

        if "request_params" in kwargs:
            params.update(kwargs["request_params"])

        if PYVESPA_AVAILABLE:
            self._ensure_sync_session()

            def _do_search():
                try:
                    result = self._sync_session.query(body=params)
                    return result.json
                except VespaError as ve:
                    # pyvespa raises on Vespa backend errors (e.g., sort attribute
                    # warnings with sources *). Return a minimal response matching
                    # the aiohttp passthrough behavior so the runner can handle it
                    # as 0 hits rather than a fatal exception.
                    errors = list(ve.args[0]) if ve.args else []
                    self.logger.warning("Vespa search returned errors (non-fatal): %s", errors)
                    return {
                        "root": {
                            "fields": {"totalCount": 0},
                            "children": [],
                            "errors": errors,
                        }
                    }

            loop = asyncio.get_running_loop()
            try:
                return await loop.run_in_executor(self._search_executor, _do_search)
            except Exception as e:
                self.logger.error("Search failed: %s", e)
                raise
        else:
            # Fallback: aiohttp POST with JSON body
            await self._ensure_session()
            endpoint = f"{self.endpoint}/search/"
            try:
                async with self._session.post(endpoint, json=params) as response:
                    return await response.json()
            except Exception as e:
                self.logger.error("Search failed: %s", e)
                raise

    def info(self, **kwargs):
        """GET /ApplicationStatus — synchronous for setup/init contexts.

        version.number MUST be valid semver (major.minor.patch) — OSB's metrics
        store pipeline validates it via versions.components(). Vespa's normal
        response is e.g. "8.669.29" which passes; the fallback below uses
        "8.0.0" so downstream validation doesn't break when the endpoint is
        temporarily unreachable.
        """
        DEFAULT_VERSION = "8.0.0"
        endpoint = f"{self.endpoint}/ApplicationStatus"
        try:
            response = requests.get(endpoint, timeout=10)
            app_status = response.json()
            version = app_status.get("application", {}).get("vespa", {}).get("version") or DEFAULT_VERSION
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
                "version": {"number": DEFAULT_VERSION, "distribution": "vespa", "build_hash": "unknown"},
            }

    def return_raw_response(self):
        """Mark that raw responses should be returned."""
        self._request_context["raw_response"] = True

