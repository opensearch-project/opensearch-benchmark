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

Wraps pymilvus's synchronous MilvusClient, dispatching all calls through
a dedicated ThreadPoolExecutor for non-blocking operation within OSB's
async runner framework.
"""
# pylint: disable=protected-access

import asyncio
import concurrent.futures
import functools
import logging
import os
import threading
import time

from osbenchmark import exceptions
from osbenchmark.context import RequestContextHolder
from osbenchmark.database.interface import (
    DatabaseClient,
    IndicesNamespace,
    ClusterNamespace,
    TransportNamespace,
    NodesNamespace,
)

# pymilvus is imported lazily inside _ensure_client(), NOT at module level.
# Importing pymilvus triggers gRPC C-core initialization (threads, channels).
# If this happens in the main process before Thespian forks actor processes,
# the forked children inherit corrupted gRPC state and hang. By deferring the
# import to _ensure_client() (which only runs inside worker processes after
# all forks are complete), we avoid this entirely.
PyMilvusClient = None
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


class MilvusDatabaseClient(DatabaseClient, RequestContextHolder):
    """Async Milvus client implementing the DatabaseClient interface.

    Threading model:
    - A dedicated ThreadPoolExecutor isolates Milvus gRPC calls from the
      event loop and other async work.
    - run_in_executor() overhead is ~20-80us per call (<1% of gRPC latency).

    Retry policy:
    - bulk()/insert(): retries transient gRPC errors (3 attempts, exponential backoff)
    - search(): NO retry — retry inflates reported latency
    - Admin ops (flush, compact, load): no retry, one-shot
    """

    def __init__(self, host="localhost", port=19530, **client_options):
        self.host = host
        self.port = port
        self.uri = f"http://{host}:{port}"
        self.client_options = client_options
        self.logger = logging.getLogger(__name__)

        self._client = None
        self._client_initialized = False
        self._init_lock = threading.Lock()
        self._collection_name = client_options.get(
            "collection_name",
            client_options.get("app_name", "target_index"),
        )

        max_workers = int(client_options.get("max_workers", 64))
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="milvus",
        )

        self._timeout_insert = int(client_options.get("timeout_insert", 60))
        self._timeout_search = int(client_options.get("timeout_search", 30))
        self._timeout_admin = int(client_options.get("timeout_admin", 300))

        self._indices_ns = MilvusIndicesNamespace(self)
        self._cluster_ns = MilvusClusterNamespace(self)
        self._transport_ns = MilvusTransportNamespace(self)
        self._nodes_ns = MilvusNodesNamespace(self)

    def _ensure_client(self):
        """Lazy-init pymilvus MilvusClient with double-checked locking.

        pymilvus is imported HERE (not at module level) to avoid initializing
        gRPC in the main process before Thespian forks. This method only runs
        inside worker processes after all forks are complete.
        """
        if self._client_initialized:
            return
        with self._init_lock:
            if self._client_initialized:
                return

            global PyMilvusClient, PYMILVUS_AVAILABLE  # pylint: disable=global-statement
            if PYMILVUS_AVAILABLE is None:
                # Set gRPC env vars right before first import — guaranteed to
                # take effect since grpc hasn't been imported yet in this process.
                os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")
                os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
                try:
                    from pymilvus import MilvusClient as _PyMilvusClient  # pylint: disable=import-outside-toplevel,import-error
                    PyMilvusClient = _PyMilvusClient
                    PYMILVUS_AVAILABLE = True
                except ImportError:
                    PYMILVUS_AVAILABLE = False

            if not PYMILVUS_AVAILABLE:
                raise exceptions.SystemSetupError(
                    "pymilvus not installed. Run: pip install 'pymilvus>=2.5.0'"
                )
            try:
                # Nuke any inherited ConnectionManager singleton from a parent
                # process (Thespian fork). The inherited singleton may hold dead
                # gRPC channels. Setting _instance = None forces a fresh singleton
                # on next access. We do NOT call _reset_instance()/close_all()
                # because calling close() on dead channels can hang.
                from pymilvus.client.connection_manager import ConnectionManager  # pylint: disable=import-outside-toplevel,import-error
                ConnectionManager._instance = None

                self._client = PyMilvusClient(uri=self.uri, timeout=self._timeout_admin, dedicated=True)
                self._client_initialized = True
                self.logger.info("pymilvus connected to %s", self.uri)
            except Exception as e:
                self.logger.error("Failed to connect to Milvus at %s: %s", self.uri, e)
                raise exceptions.SystemSetupError(
                    f"Cannot connect to Milvus at {self.uri}: {e}"
                )

    async def _run(self, fn, *args, **kwargs):
        """Run a synchronous pymilvus call in the dedicated thread pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            functools.partial(fn, *args, **kwargs),
        )

    async def _run_with_retry(self, fn, *args, max_retries=3, **kwargs):
        """Run with retry for transient gRPC failures. Used for ingestion only."""
        for attempt in range(max_retries + 1):
            try:
                return await self._run(fn, *args, **kwargs)
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
        """Close pymilvus client and thread pool."""
        with self._init_lock:
            if self._client:
                try:
                    await asyncio.wait_for(self._run(self._client.close), timeout=10)
                except (asyncio.TimeoutError, Exception) as e:
                    self.logger.warning("Error closing pymilvus client: %s", e)
                self._client = None
                self._client_initialized = False
        self._executor.shutdown(wait=False)

    # --- Public pymilvus wrappers (runners never touch _client directly) ---

    def create_schema(self):
        """Expose pymilvus create_schema()."""
        self._ensure_client()
        return self._client.create_schema()

    def prepare_index_params(self):
        """Expose pymilvus prepare_index_params()."""
        self._ensure_client()
        return self._client.prepare_index_params()

    async def load_collection(self, collection_name, timeout=None):
        """Load collection into memory for search. Handles already-loaded state."""
        self._ensure_client()
        timeout = timeout or self._timeout_admin
        try:
            await self._run(
                self._client.load_collection,
                collection_name=collection_name,
                timeout=timeout,
            )
        except Exception as e:
            err_str = str(e).lower()
            if "already loaded" in err_str or "load state: loaded" in err_str:
                self.logger.info("Collection %s already loaded", collection_name)
            else:
                raise

    # --- Namespace properties ---

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

    # --- Core document operations ---

    async def bulk(self, body, index=None, doc_type=None, params=None, **kwargs):
        """Insert a batch of documents. Retries transient errors."""
        self._ensure_client()
        collection_name = index or self._collection_name

        if not isinstance(body, (list, tuple)):
            body = [body]

        try:
            result = await self._run_with_retry(
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

        return await self._run(self._client.search, **search_kwargs)

    async def index(self, index, body, id=None, doc_type=None, **kwargs):
        """Index a single document."""
        self._ensure_client()
        collection_name = index or self._collection_name
        if id is not None:
            body["doc_id"] = int(id)
        await self._run(
            self._client.insert,
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
        """
        import requests as req  # pylint: disable=import-outside-toplevel
        try:
            resp = req.get(f"{self.uri}/v2/vectordb/collections/list",
                          json={}, timeout=10, headers={"Content-Type": "application/json"})
            version = "unknown"
            if resp.status_code == 200:
                # Milvus REST API works — extract version from a separate call if needed
                version = "2.x"  # REST API doesn't expose version; get it from health port
                health_url = f"http://{self.host}:9091/api/v1/health"
                try:
                    health_resp = req.get(health_url, timeout=5)
                    if health_resp.status_code == 200:
                        version = health_resp.json().get("version", "2.x")
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
                "version": {"number": "unknown", "distribution": "milvus", "build_hash": "unknown"},
            }

    def return_raw_response(self):
        pass


# =============================================================================
# Namespace implementations
# =============================================================================

class MilvusIndicesNamespace(IndicesNamespace):
    """Index operations mapped to Milvus collection operations."""

    def __init__(self, client):
        self._client = client

    async def create(self, index, body=None, **kwargs):
        """Create collection + index.

        Body contains {"schema": CollectionSchema, "index_params": IndexParams}.
        Handles drop/create race: Milvus drop_collection is async internally.

        Note: create_collection(schema, index_params) auto-calls load_collection()
        internally in pymilvus. The subsequent warmup runner will be a no-op.
        """
        self._client._ensure_client()
        collection_name = index or self._client._collection_name
        schema = body.get("schema") if body else None
        index_params = body.get("index_params") if body else None

        if schema and index_params:
            if await self._client._run(
                self._client._client.has_collection,
                collection_name=collection_name,
            ):
                await self._client._run(
                    self._client._client.drop_collection,
                    collection_name=collection_name,
                )
                for _ in range(20):
                    if not await self._client._run(
                        self._client._client.has_collection,
                        collection_name=collection_name,
                    ):
                        break
                    await asyncio.sleep(0.5)

            await self._client._run(
                self._client._client.create_collection,
                collection_name=collection_name,
                schema=schema,
                index_params=index_params,
            )
        return {"acknowledged": True, "shards_acknowledged": True, "index": collection_name}

    async def delete(self, index, **kwargs):
        self._client._ensure_client()
        await self._client._run(
            self._client._client.drop_collection,
            collection_name=index or self._client._collection_name,
        )
        return {"acknowledged": True}

    async def exists(self, index, **kwargs):
        self._client._ensure_client()
        return await self._client._run(
            self._client._client.has_collection,
            collection_name=index,
        )

    async def refresh(self, index=None, **kwargs):
        """Map to Milvus flush() — seals growing segments, persists to storage.

        Bypasses pymilvus's flush() to avoid its internal _wait_for_flushed()
        polling loop. That loop can trigger a reconnect cascade that permanently
        kills the gRPC channel (reconnect timeout → close channel → "Cannot
        invoke RPC on closed channel!"). Instead we send the Flush RPC directly
        and poll get_flush_state ourselves with reconnect resilience.
        """
        self._client._ensure_client()
        if not index:
            return {"acknowledged": True, "_shards": {"total": 1, "successful": 1, "failed": 0}}

        timeout = self._client._timeout_admin

        # Step 1: Send the Flush RPC directly via the gRPC stub.
        # Retry on rate limiting — Milvus throttles flush to 0.1/s.
        for attempt in range(5):
            try:
                segment_ids, flush_ts = await self._client._run(
                    self._send_flush_rpc, index, timeout
                )
                break
            except Exception as e:
                if "rate limit" in str(e).lower() and attempt < 4:
                    self._client.logger.warning("Flush rate-limited, retrying in 10s...")
                    await asyncio.sleep(10)
                    continue
                raise

        if not segment_ids:
            return {"acknowledged": True, "_shards": {"total": 1, "successful": 1, "failed": 0}}

        # Step 2: Poll get_flush_state ourselves
        start = time.time()
        while True:
            try:
                handler = self._client._client._get_connection()
                flushed = await self._client._run(
                    handler.get_flush_state,
                    segment_ids, index, flush_ts, timeout=timeout,
                )
                if flushed:
                    break
            except Exception as e:
                elapsed = time.time() - start
                if elapsed > timeout:
                    raise
                err_str = str(e).lower()
                if "closed channel" in err_str or "cannot invoke" in err_str:
                    self._client.logger.warning(
                        "Flush poll hit closed channel (%.0fs), reconnecting...", elapsed
                    )
                    self._client._client_initialized = False
                    self._client._client = None
                    self._client._ensure_client()
                else:
                    raise

            if time.time() - start > timeout:
                raise exceptions.SystemSetupError(
                    f"Flush timed out after {timeout}s for collection {index}"
                )
            await asyncio.sleep(0.5)

        return {"acknowledged": True, "_shards": {"total": 1, "successful": 1, "failed": 0}}

    def _send_flush_rpc(self, collection_name, timeout):
        """Send Flush RPC directly, bypassing pymilvus's _wait_for_flushed()."""
        from pymilvus.client.prepare import Prepare  # pylint: disable=import-outside-toplevel,import-error
        from pymilvus.client.utils import check_status  # pylint: disable=import-outside-toplevel,import-error

        handler = self._client._client._get_connection()
        request = Prepare.flush_param([collection_name])
        response = handler._stub.Flush(request, timeout=timeout)
        check_status(response.status)

        seg_id_array = response.coll_segIDs.get(collection_name)
        segment_ids = list(seg_id_array.data) if seg_id_array else []
        flush_ts = response.coll_flush_ts.get(collection_name, 0)
        return segment_ids, flush_ts

    async def stats(self, index=None, metric=None, **kwargs):  # pylint: disable=invalid-overridden-method
        self._client._ensure_client()
        if index:
            result = await self._client._run(
                self._client._client.get_collection_stats,
                collection_name=index,
            )
            row_count = result.get("row_count", 0)
            return {
                "_all": {
                    "primaries": {"docs": {"count": row_count}},
                    "total": {"docs": {"count": row_count}},
                }
            }
        return {"_all": {"primaries": {}, "total": {}}}

    async def forcemerge(self, index=None, **kwargs):  # pylint: disable=invalid-overridden-method
        """Map to Milvus compact(). Polls get_compaction_state() for completion.

        get_compaction_state() returns a string: "Completed", "Executing",
        or "UndefiedState" (note: Milvus typo in enum name).
        """
        self._client._ensure_client()
        if not index:
            return {"_shards": {"total": 1, "successful": 1, "failed": 0}}

        job_id = await self._client._run(
            self._client._client.compact,
            collection_name=index,
            timeout=self._client._timeout_admin,
        )

        wait = kwargs.get("wait_for_completion", True)
        if wait and wait != "false":
            for i in range(120):
                try:
                    state = await self._client._run(
                        self._client._client.get_compaction_state,
                        job_id,
                    )
                    if state == "Completed":
                        break
                    if i > 0 and i % 15 == 0:
                        self._client.logger.info(
                            "Compaction in progress (%ds, state=%s)...", i, state
                        )
                except Exception:
                    break
                await asyncio.sleep(1)

        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}


class MilvusClusterNamespace(ClusterNamespace):

    def __init__(self, client):
        self._client = client

    async def health(self, **kwargs):
        self._client._ensure_client()
        try:
            await self._client._run(self._client._client.list_collections)
            return {
                "cluster_name": "milvus",
                "status": "green",
                "timed_out": False,
                "number_of_nodes": 1,
                "number_of_data_nodes": 1,
                "active_primary_shards": 1,
                "active_shards": 1,
                "relocating_shards": 0,
                "initializing_shards": 0,
                "unassigned_shards": 0,
            }
        except Exception:
            return {"cluster_name": "milvus", "status": "red", "timed_out": False}

    async def put_settings(self, body, **kwargs):
        return {"acknowledged": True}


class MilvusTransportNamespace(TransportNamespace):
    """Stub — Milvus uses gRPC, not HTTP."""

    def __init__(self, client):
        self._client = client

    async def perform_request(self, method, url, params=None, body=None, headers=None):  # pylint: disable=too-many-positional-arguments
        return {}

    async def close(self):
        # Intentionally a no-op. OSB calls transport.close() between every
        # operation step (in AsyncIoAdapter.run's finally block). Closing the
        # pymilvus client tears down the gRPC channel, and the gRPC C-core
        # retains stale state that poisons the NEXT step's fresh client.
        # Let process exit handle cleanup instead.
        pass


class MilvusNodesNamespace(NodesNamespace):
    """Stub node stats/info for telemetry compatibility."""

    def __init__(self, client):
        self._client = client

    def stats(self, node_id=None, metric=None, **kwargs):
        return {
            "nodes": {
                "milvus-node-1": {
                    "name": "milvus-node-1",
                    "host": self._client.uri,
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
                "milvus-node-1": {
                    "name": "milvus-node-1",
                    "host": self._client.uri,
                    "version": "unknown",
                    "os": {"name": "Linux"},
                    "jvm": {
                        "version": "N/A",
                        "gc": {
                            "collectors": {
                                "young": "N/A",
                                "old": "N/A",
                            }
                        },
                    },
                }
            }
        }
