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
Milvus database client implementation for OpenSearch Benchmark.

Uses pymilvus's AsyncMilvusClient (native grpc.aio) for all operations.
The gRPC aio stack runs the I/O loop in C with the GIL released during
select/epoll waits, so a single Python thread can efficiently juggle many
concurrent coroutines without the thread-level GIL contention that caps
the sync + ThreadPoolExecutor pattern. At 32 concurrent clients this
beats sync+threadpool by ~13% (2015 vs 1777 QPS) and scales further past
the 8-client saturation point. See
`Notes/Open Source/Multi DB Project/Comparative Benchmarking/GIL and
Multi-Client Scaling.md` for the measurements.
"""
# pylint: disable=protected-access

import asyncio
import logging
import os
import threading
import time

from osbenchmark import exceptions
from osbenchmark.context import RequestContextHolder

# pymilvus is imported lazily inside _ensure_client(), NOT at module level.
# Importing pymilvus triggers gRPC C-core initialization (threads, channels).
# If this happens in the main process before Thespian forks actor processes,
# the forked children inherit corrupted gRPC state and hang. By deferring the
# import to _ensure_client() (which only runs inside worker processes after
# all forks are complete), we avoid this entirely.
PyAsyncMilvusClient = None
PYMILVUS_AVAILABLE = None  # None = not yet checked, True/False after first check


class MilvusClientFactory:
    """Factory for creating Milvus client instances."""

    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = dict(client_options)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Creating Milvus client connected to %s with options [%s]", hosts, dict(client_options))

    def create(self):
        return self.create_async()

    def create_async(self):
        if not self.hosts:
            raise exceptions.SystemSetupError("No Milvus hosts configured")
        host_config = (self.hosts[0] if isinstance(self.hosts, list)
                       else self.hosts.get("default", [{}])[0])
        host = host_config.get("host", "localhost")
        port = host_config.get("port", 19530)
        return MilvusDatabaseClient(host=host, port=port, **self.client_options)

    def wait_for_rest_layer(self, max_attempts=40):
        """Wait for Milvus to become available.

        Uses a simple HTTP health check instead of creating a full pymilvus
        client. Creating and closing a pymilvus client poisons the shared
        connection manager — the next real client gets a stale closed channel.
        """
        import requests as req  # pylint: disable=import-outside-toplevel
        host_config = (self.hosts[0] if isinstance(self.hosts, list)
                       else self.hosts.get("default", [{}])[0])
        host = host_config.get("host", "localhost")
        health_port = host_config.get("health_port", 9091)
        health_url = f"http://{host}:{health_port}/healthz"

        for attempt in range(max_attempts):
            try:
                resp = req.get(health_url, timeout=5)
                if resp.status_code == 200:
                    return True
            except Exception as e:
                if attempt >= max_attempts - 1:
                    raise exceptions.SystemSetupError(
                        f"Milvus not available after {max_attempts} attempts: {e}"
                    )
            time.sleep(3)


class MilvusDatabaseClient(RequestContextHolder):
    """Async Milvus client implementing the DatabaseClient interface.

    Uses pymilvus AsyncMilvusClient (native grpc.aio). All methods are
    directly awaitable — no ThreadPoolExecutor, no run_in_executor.

    Retry policy:
    - bulk()/insert(): retries transient gRPC errors (3 attempts, exponential backoff)
    - search(): NO retry — retry inflates reported latency
    - Admin ops (flush, compact, load): one-shot except where documented
      (flush has rate-limit retry because Milvus throttles it to 0.1/s)
    """

    def __init__(self, host="localhost", port=19530, **client_options):
        self.host = host
        self.port = port
        self.uri = f"http://{host}:{port}"
        self.client_options = client_options
        self.logger = logging.getLogger(__name__)

        self._client = None
        self._client_initialized = False
        # threading.Lock rather than asyncio.Lock: _ensure_client() may be
        # called from both sync (create_schema, prepare_index_params) and
        # async paths. A threading lock is safe in both contexts.
        self._init_lock = threading.Lock()
        self._collection_name = client_options.get(
            "collection_name",
            client_options.get("app_name", "target_index"),
        )

        self._timeout_insert = int(client_options.get("timeout_insert", 60))
        self._timeout_search = int(client_options.get("timeout_search", 30))
        self._timeout_admin = int(client_options.get("timeout_admin", 300))

        # Namespace proxies — runners call client.indices.create(), etc.
        self.indices = self
        self.cluster = self
        self.transport = self
        self.nodes = self

    # --- Namespace methods (called by runners via self.indices.X, etc.) ---

    async def create(self, index=None, body=None, **kwargs):
        """Create collection + index from schema/index_params in body."""
        self._ensure_client()
        collection_name = index or self._collection_name
        schema = body.get("schema") if body else None
        index_params = body.get("index_params") if body else None

        if schema and index_params:
            if await self._client.has_collection(collection_name=collection_name):
                await self._client.drop_collection(collection_name=collection_name)
                for _ in range(20):
                    if not await self._client.has_collection(collection_name=collection_name):
                        break
                    await asyncio.sleep(0.5)
            await self._client.create_collection(
                collection_name=collection_name, schema=schema, index_params=index_params,
            )
        return {"acknowledged": True, "shards_acknowledged": True, "index": collection_name}

    async def delete(self, index=None, **kwargs):
        self._ensure_client()
        await self._client.drop_collection(collection_name=index or self._collection_name)
        return {"acknowledged": True}

    async def exists(self, index=None, **kwargs):
        self._ensure_client()
        return await self._client.has_collection(collection_name=index)

    async def refresh(self, index=None, **kwargs):
        """Map to Milvus flush(). Retries on rate-limit (Milvus throttles to 0.1/s)."""
        self._ensure_client()
        if not index:
            return {"acknowledged": True, "_shards": {"total": 1, "successful": 1, "failed": 0}}
        for attempt in range(5):
            try:
                await self._client.flush(collection_name=index, timeout=self._timeout_admin)
                break
            except Exception as e:
                if "rate limit" in str(e).lower() and attempt < 4:
                    self.logger.warning("Flush rate-limited, retrying in 10s...")
                    await asyncio.sleep(10)
                    continue
                raise
        return {"acknowledged": True, "_shards": {"total": 1, "successful": 1, "failed": 0}}

    async def stats(self, index=None, metric=None, **kwargs):
        self._ensure_client()
        if index:
            result = await self._client.get_collection_stats(collection_name=index)
            row_count = result.get("row_count", 0)
            return {"_all": {"primaries": {"docs": {"count": row_count}}, "total": {"docs": {"count": row_count}}}}
        return {"_all": {"primaries": {}, "total": {}}}

    async def forcemerge(self, index=None, **kwargs):
        """Map to Milvus compact(). Polls get_compaction_state() for completion."""
        self._ensure_client()
        if not index:
            return {"_shards": {"total": 1, "successful": 1, "failed": 0}}
        job_id = await self._client.compact(collection_name=index, timeout=self._timeout_admin)
        wait = kwargs.get("wait_for_completion", True)
        if wait and wait != "false":
            for i in range(120):
                try:
                    state = await self._client.get_compaction_state(job_id)
                    if state == "Completed":
                        break
                    if i > 0 and i % 15 == 0:
                        self.logger.info("Compaction in progress (%ds, state=%s)...", i, state)
                except Exception:
                    break
                await asyncio.sleep(1)
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}

    async def health(self, **kwargs):
        self._ensure_client()
        try:
            await self._client.list_collections()
            return {
                "cluster_name": "milvus", "status": "green", "timed_out": False,
                "number_of_nodes": 1, "number_of_data_nodes": 1,
                "active_primary_shards": 1, "active_shards": 1,
                "relocating_shards": 0, "initializing_shards": 0, "unassigned_shards": 0,
            }
        except Exception:
            return {"cluster_name": "milvus", "status": "red", "timed_out": False}

    async def put_settings(self, body=None, **kwargs):
        return {"acknowledged": True}

    async def perform_request(self, method, url, params=None, body=None, headers=None):
        return {}

    def _ensure_client(self):
        """Lazy-init pymilvus AsyncMilvusClient with double-checked locking.

        pymilvus is imported HERE (not at module level) to avoid initializing
        gRPC in the main process before Thespian forks. This method only runs
        inside worker processes after all forks are complete.

        AsyncMilvusClient.__init__() is synchronous (stores config, defers
        actual gRPC channel setup to the first awaited call), so no await
        is needed here.
        """
        if self._client_initialized:
            return
        with self._init_lock:
            if self._client_initialized:
                return

            global PyAsyncMilvusClient, PYMILVUS_AVAILABLE  # pylint: disable=global-statement
            if PYMILVUS_AVAILABLE is None:
                # Set gRPC env vars right before first import — guaranteed to
                # take effect since grpc hasn't been imported yet in this process.
                os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")
                os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
                try:
                    from pymilvus import AsyncMilvusClient as _PyAsync  # pylint: disable=import-outside-toplevel,import-error
                    PyAsyncMilvusClient = _PyAsync
                    PYMILVUS_AVAILABLE = True
                except ImportError:
                    PYMILVUS_AVAILABLE = False

            if not PYMILVUS_AVAILABLE:
                raise exceptions.SystemSetupError(
                    "pymilvus not installed or AsyncMilvusClient unavailable. "
                    "Run: pip install 'pymilvus>=2.5.0'"
                )
            try:
                # Nuke any inherited ConnectionManager singleton from a parent
                # process (Thespian fork). The inherited singleton may hold dead
                # gRPC channels. Setting _instance = None forces a fresh singleton
                # on next access. AsyncMilvusClient uses grpc.aio (not the sync
                # ConnectionManager) so this may be a no-op for async, but we
                # keep it defensively since pymilvus code paths are intertwined.
                try:
                    from pymilvus.client.connection_manager import ConnectionManager  # pylint: disable=import-outside-toplevel,import-error
                    ConnectionManager._instance = None
                except ImportError:
                    pass

                self._client = PyAsyncMilvusClient(uri=self.uri, timeout=self._timeout_admin)
                self._client_initialized = True
                self.logger.info("pymilvus AsyncMilvusClient connected to %s", self.uri)
            except Exception as e:
                self.logger.error("Failed to connect to Milvus at %s: %s", self.uri, e)
                raise exceptions.SystemSetupError(
                    f"Cannot connect to Milvus at {self.uri}: {e}"
                )

    async def _with_retry(self, coro_fn, *args, max_retries=3, **kwargs):
        """Call an awaitable function with retry on transient gRPC errors.

        Used for ingestion (bulk/insert). Search deliberately does NOT use this —
        retrying search inflates reported latency.
        """
        for attempt in range(max_retries + 1):
            try:
                return await coro_fn(*args, **kwargs)
            except Exception as e:
                err_str = str(e).lower()
                is_transient = any(t in err_str for t in (
                    "unavailable", "deadline exceeded", "connection",
                    "resource exhausted", "reset by peer", "broken pipe",
                ))
                if is_transient and attempt < max_retries:
                    wait = 0.5 * (2 ** attempt)
                    self.logger.warning(
                        "Transient Milvus error (attempt %d/%d, retrying in %.1fs): %s",
                        attempt + 1, max_retries, wait, e,
                    )
                    await asyncio.sleep(wait)
                    continue
                raise

    # --- Lifecycle ---

    async def __aenter__(self):
        self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def close(self):  # pylint: disable=invalid-overridden-method
        """Close AsyncMilvusClient."""
        with self._init_lock:
            if self._client is not None:
                try:
                    await asyncio.wait_for(self._client.close(), timeout=10)
                except (asyncio.TimeoutError, Exception) as e:
                    self.logger.warning("Error closing pymilvus client: %s", e)
                self._client = None
                self._client_initialized = False

    # --- Schema helpers (sync — pure Python, no network calls) ---

    def create_schema(self):
        """Expose pymilvus create_schema() — pure helper, no gRPC.

        In pymilvus, this is a classmethod that returns a new CollectionSchema
        object. Safe to call without awaiting. AsyncMilvusClient inherits it
        from MilvusClient.
        """
        self._ensure_client()
        return self._client.create_schema()

    def prepare_index_params(self):
        """Expose pymilvus prepare_index_params() — pure helper, no gRPC."""
        self._ensure_client()
        return self._client.prepare_index_params()

    async def load_collection(self, collection_name, timeout=None):
        """Load collection into memory for search. Handles already-loaded state."""
        self._ensure_client()
        timeout = timeout or self._timeout_admin
        try:
            await self._client.load_collection(
                collection_name=collection_name,
                timeout=timeout,
            )
        except Exception as e:
            err_str = str(e).lower()
            if "already loaded" in err_str or "load state: loaded" in err_str:
                self.logger.info("Collection %s already loaded", collection_name)
            else:
                raise

    # --- Core document operations ---

    async def bulk(self, body, index=None, doc_type=None, params=None, **kwargs):
        """Insert a batch of documents. Retries transient errors."""
        self._ensure_client()
        collection_name = index or self._collection_name

        if not isinstance(body, (list, tuple)):
            body = [body]

        try:
            result = await self._with_retry(
                self._client.insert,
                collection_name=collection_name,
                data=body,
                timeout=self._timeout_insert,
            )
            insert_count = result.get("insert_count", 0)
            has_errors = insert_count < len(body)
            if has_errors:
                self.logger.warning(
                    "Milvus partial insert: %d/%d docs succeeded (collection=%s)",
                    insert_count, len(body), collection_name,
                )
            return {
                "took": 0,
                "errors": has_errors,
                "items": [
                    {"index": {"_id": str(i), "status": 200}}
                    for i in range(insert_count)
                ],
            }
        except Exception as e:
            self.logger.warning("Milvus insert failed for %d docs: %s", len(body), e)
            return {
                "took": 0,
                "errors": True,
                "items": [
                    {"index": {"_id": str(i), "status": 500, "error": str(e)}}
                    for i in range(len(body))
                ],
            }

    async def search(self, index=None, body=None, doc_type=None, **kwargs):
        """Execute vector search. NO retry — retry inflates reported latency."""
        self._ensure_client()
        collection_name = index or self._collection_name

        search_kwargs = dict(body) if body else {}
        search_kwargs["collection_name"] = collection_name
        search_kwargs.setdefault("timeout", self._timeout_search)

        return await self._client.search(**search_kwargs)

    async def index(self, index, body, id=None, doc_type=None, **kwargs):
        """Index a single document."""
        self._ensure_client()
        collection_name = index or self._collection_name
        if id is not None:
            body["doc_id"] = int(id)
        await self._client.insert(
            collection_name=collection_name,
            data=[body],
            timeout=self._timeout_insert,
        )
        return {"_id": str(id), "result": "created", "_version": 1}

    def info(self, **kwargs):
        """Get Milvus server version — synchronous, uses HTTP to avoid loading pymilvus.

        This is called by the coordinator process during retrieve_cluster_info()
        BEFORE workers are forked. Using pymilvus here would initialize gRPC in
        the coordinator, poisoning all forked workers. HTTP avoids this.

        version.number MUST be valid semver (major.minor.patch) — OSB's metrics
        store pipeline parses it via versions.components() which enforces the
        pattern ^(\\d+)\\.(\\d+)\\.(\\d+)(?:-(.+))?$. Returning "unknown" or "2.x"
        breaks the datastore push.
        """
        import requests as req  # pylint: disable=import-outside-toplevel
        DEFAULT_VERSION = "2.0.0"
        try:
            resp = req.get(f"{self.uri}/v2/vectordb/collections/list",
                          json={}, timeout=10, headers={"Content-Type": "application/json"})
            version = DEFAULT_VERSION
            if resp.status_code == 200:
                health_url = f"http://{self.host}:9091/api/v1/health"
                try:
                    health_resp = req.get(health_url, timeout=5)
                    if health_resp.status_code == 200:
                        reported = health_resp.json().get("version")
                        if reported:
                            candidate = reported.lstrip("v")
                            if candidate and candidate[0].isdigit() and "." in candidate:
                                version = candidate
                except Exception:
                    pass
            return {
                "name": "milvus",
                "cluster_name": self._collection_name,
                "cluster_uuid": "milvus-cluster",
                "version": {
                    "number": version,
                    "distribution": "milvus",
                    "build_type": "standalone",
                    "build_hash": "unknown",
                    "build_date": "unknown",
                    "build_snapshot": False,
                    "build_flavor": "default",
                    "lucene_version": "unknown",
                    "minimum_wire_compatibility_version": version,
                    "minimum_index_compatibility_version": version,
                },
                "tagline": "The Vector Database for AI",
            }
        except Exception:
            return {
                "name": "milvus",
                "cluster_name": self._collection_name,
                "version": {"number": DEFAULT_VERSION, "distribution": "milvus", "build_hash": "unknown"},
            }

    def return_raw_response(self):
        pass

