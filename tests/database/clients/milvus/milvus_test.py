# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Unit tests for osbenchmark.database.clients.milvus.milvus

Tests MilvusClientFactory, MilvusDatabaseClient, and all namespace classes
(MilvusIndicesNamespace, MilvusClusterNamespace, MilvusTransportNamespace,
MilvusNodesNamespace).

The client is backed by pymilvus AsyncMilvusClient (native grpc.aio), so
tests use mock.AsyncMock for all client methods. There is no ThreadPoolExecutor
and no _run helper — operations are directly awaited.
"""
# pylint: disable=protected-access

from unittest import TestCase, mock

from osbenchmark import exceptions
from osbenchmark.database.clients.milvus.milvus import (
    MilvusClientFactory,
    MilvusDatabaseClient,
    MilvusIndicesNamespace,
    MilvusClusterNamespace,
    MilvusTransportNamespace,
    MilvusNodesNamespace,
)
from tests import run_async


# =============================================================================
# Helpers
# =============================================================================

def _make_client(host="localhost", port=19530, **opts):
    """Create a MilvusDatabaseClient with a pre-injected AsyncMock _client.

    Bypasses _ensure_client() so tests never need pymilvus installed.
    All pymilvus methods are async, so AsyncMock makes every attribute access
    return an awaitable that yields the configured return_value / raises the
    configured side_effect.
    """
    client = MilvusDatabaseClient(host=host, port=port, **opts)
    client._client = mock.AsyncMock()
    client._client_initialized = True
    return client


# =============================================================================
# MilvusClientFactory Tests
# =============================================================================

class MilvusClientFactoryTests(TestCase):

    def test_create_from_host_list(self):
        factory = MilvusClientFactory(
            hosts=[{"host": "m1", "port": 19530}],
            client_options={"app_name": "test"},
        )
        client = factory.create_async()
        self.assertEqual("m1", client.host)
        self.assertEqual(19530, client.port)
        self.assertEqual("http://m1:19530", client.uri)

    def test_create_from_hosts_with_default_key(self):
        factory = MilvusClientFactory(
            hosts={"default": [{"host": "m2", "port": 19530}]},
            client_options={},
        )
        client = factory.create_async()
        self.assertEqual("m2", client.host)

    def test_empty_hosts_raises_error(self):
        factory = MilvusClientFactory(hosts=[], client_options={})
        with self.assertRaises(exceptions.SystemSetupError):
            factory.create_async()

    def test_create_delegates_to_create_async(self):
        factory = MilvusClientFactory(
            hosts=[{"host": "m3", "port": 19530}],
            client_options={},
        )
        with mock.patch.object(factory, "create_async") as mock_async:
            factory.create()
            mock_async.assert_called_once()

    @mock.patch("requests.get")
    def test_wait_for_rest_layer_success(self, mock_get):
        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        factory = MilvusClientFactory(
            hosts=[{"host": "m1", "port": 19530}],
            client_options={},
        )
        result = factory.wait_for_rest_layer(max_attempts=3)
        self.assertTrue(result)


# =============================================================================
# MilvusDatabaseClient __init__ Tests
# =============================================================================

class MilvusDatabaseClientInitTests(TestCase):

    def test_init_stores_host_port_uri(self):
        client = MilvusDatabaseClient(host="myhost", port=12345)
        self.assertEqual("myhost", client.host)
        self.assertEqual(12345, client.port)
        self.assertEqual("http://myhost:12345", client.uri)

    def test_init_defaults(self):
        client = MilvusDatabaseClient()
        self.assertEqual("localhost", client.host)
        self.assertEqual(19530, client.port)
        self.assertEqual("target_index", client._collection_name)

    def test_init_custom_timeouts(self):
        client = MilvusDatabaseClient(
            timeout_insert=120, timeout_search=60, timeout_admin=600,
        )
        self.assertEqual(120, client._timeout_insert)
        self.assertEqual(60, client._timeout_search)
        self.assertEqual(600, client._timeout_admin)

    def test_collection_name_from_collection_name_option(self):
        client = MilvusDatabaseClient(collection_name="my_coll")
        self.assertEqual("my_coll", client._collection_name)

    def test_collection_name_falls_back_to_app_name(self):
        client = MilvusDatabaseClient(app_name="app_coll")
        self.assertEqual("app_coll", client._collection_name)

    def test_init_creates_namespace_objects(self):
        client = MilvusDatabaseClient()
        self.assertIsInstance(client._indices_ns, MilvusIndicesNamespace)
        self.assertIsInstance(client._cluster_ns, MilvusClusterNamespace)
        self.assertIsInstance(client._transport_ns, MilvusTransportNamespace)
        self.assertIsInstance(client._nodes_ns, MilvusNodesNamespace)

    def test_init_has_no_executor(self):
        """AsyncMilvusClient refactor eliminated the ThreadPoolExecutor layer."""
        client = MilvusDatabaseClient()
        self.assertFalse(hasattr(client, "_executor"))


# =============================================================================
# Bulk / Insert Tests
# =============================================================================

class MilvusBulkTests(TestCase):

    @run_async
    async def test_bulk_success(self):
        client = _make_client()
        client._client.insert.return_value = {"insert_count": 3}

        result = await client.bulk(body=[{"a": 1}, {"a": 2}, {"a": 3}], index="coll")
        self.assertFalse(result["errors"])
        self.assertEqual(3, len(result["items"]))
        client._client.insert.assert_awaited_once()

    @run_async
    async def test_bulk_partial_failure(self):
        client = _make_client()
        client._client.insert.return_value = {"insert_count": 1}

        result = await client.bulk(body=[{"a": 1}, {"a": 2}], index="coll")
        self.assertTrue(result["errors"])
        self.assertEqual(1, len(result["items"]))

    @run_async
    async def test_bulk_exception_returns_error_items(self):
        client = _make_client()
        client._client.insert.side_effect = RuntimeError("network error")

        result = await client.bulk(body=[{"a": 1}], index="coll")
        self.assertTrue(result["errors"])
        self.assertEqual(500, result["items"][0]["index"]["status"])
        self.assertIn("network error", result["items"][0]["index"]["error"])

    @run_async
    async def test_bulk_single_dict_wrapped_in_list(self):
        """A single dict body is auto-wrapped into [body]."""
        client = _make_client()
        client._client.insert.return_value = {"insert_count": 1}

        result = await client.bulk(body={"a": 1}, index="coll")
        self.assertFalse(result["errors"])
        call_kwargs = client._client.insert.call_args[1]
        self.assertIsInstance(call_kwargs["data"], list)

    @run_async
    async def test_bulk_retries_transient_error(self):
        """Transient gRPC errors trigger retry via _with_retry."""
        client = _make_client()

        call_count = 0

        async def flaky_insert(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("unavailable: server not ready")
            return {"insert_count": 2}

        client._client.insert.side_effect = flaky_insert

        with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
            result = await client.bulk(body=[{"a": 1}, {"a": 2}], index="coll")

        self.assertFalse(result["errors"])
        self.assertEqual(2, call_count)

    @run_async
    async def test_bulk_no_retry_on_non_transient_error(self):
        """Non-transient errors are NOT retried."""
        client = _make_client()

        call_count = 0

        async def bad_insert(**kwargs):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("schema mismatch: field not found")

        client._client.insert.side_effect = bad_insert

        # _with_retry should NOT retry non-transient errors,
        # but the exception is caught by bulk() itself which returns error items
        result = await client.bulk(body=[{"a": 1}], index="coll")
        self.assertTrue(result["errors"])
        self.assertEqual(1, call_count)


# =============================================================================
# Search Tests
# =============================================================================

class MilvusSearchTests(TestCase):

    @run_async
    async def test_search_success_passthrough(self):
        client = _make_client()
        expected = [{"id": 1, "distance": 0.5}]
        client._client.search.return_value = expected

        result = await client.search(
            index="coll",
            body={"data": [[0.1, 0.2]], "limit": 10},
        )
        self.assertEqual(expected, result)
        call_kwargs = client._client.search.call_args[1]
        self.assertEqual("coll", call_kwargs["collection_name"])

    @run_async
    async def test_search_no_retry(self):
        """Search should NOT retry — it inflates reported latency."""
        client = _make_client()

        call_count = 0

        async def fail_search(**kwargs):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("unavailable: server not ready")

        client._client.search.side_effect = fail_search

        with self.assertRaises(RuntimeError):
            await client.search(index="coll", body={"data": [[0.1]]})
        self.assertEqual(1, call_count)

    @run_async
    async def test_search_timeout_propagation(self):
        client = _make_client(timeout_search=42)
        client._client.search.return_value = []

        await client.search(index="coll", body={"data": [[0.1]]})
        call_kwargs = client._client.search.call_args[1]
        self.assertEqual(42, call_kwargs["timeout"])

    @run_async
    async def test_search_uses_collection_name_fallback(self):
        client = _make_client(collection_name="fallback_coll")
        client._client.search.return_value = []

        await client.search(body={"data": [[0.1]]})
        call_kwargs = client._client.search.call_args[1]
        self.assertEqual("fallback_coll", call_kwargs["collection_name"])


# =============================================================================
# Info Tests
# =============================================================================

class MilvusInfoTests(TestCase):

    @mock.patch("requests.get")
    def test_info_uses_http_not_pymilvus(self, mock_get):
        """info() uses HTTP to avoid loading pymilvus in the coordinator process."""
        rest_resp = mock.MagicMock()
        rest_resp.status_code = 200

        health_resp = mock.MagicMock()
        health_resp.status_code = 200
        health_resp.json.return_value = {"version": "2.5.1"}

        mock_get.side_effect = [rest_resp, health_resp]

        client = MilvusDatabaseClient(host="myhost", port=19530)
        result = client.info()

        self.assertEqual("milvus", result["name"])
        self.assertEqual("2.5.1", result["version"]["number"])
        self.assertEqual("milvus", result["version"]["distribution"])
        self.assertEqual(2, mock_get.call_count)

    @mock.patch("requests.get")
    def test_info_fallback_on_error(self, mock_get):
        mock_get.side_effect = ConnectionError("refused")

        client = MilvusDatabaseClient(host="myhost")
        result = client.info()

        self.assertEqual("milvus", result["name"])
        self.assertEqual("unknown", result["version"]["number"])

    @mock.patch("requests.get")
    def test_info_health_port_fallback(self, mock_get):
        """When REST succeeds but health check fails, version falls back to '2.x'."""
        rest_resp = mock.MagicMock()
        rest_resp.status_code = 200

        mock_get.side_effect = [rest_resp, ConnectionError("health check failed")]

        client = MilvusDatabaseClient(host="myhost")
        result = client.info()

        self.assertEqual("2.x", result["version"]["number"])


# =============================================================================
# Indices Namespace Tests
# =============================================================================

class MilvusIndicesNamespaceTests(TestCase):

    @run_async
    async def test_create_with_schema_and_index_params(self):
        client = _make_client()
        schema = mock.MagicMock()
        index_params = mock.MagicMock()
        client._client.has_collection.return_value = False

        result = await client.indices.create(
            index="my_coll",
            body={"schema": schema, "index_params": index_params},
        )
        self.assertTrue(result["acknowledged"])
        self.assertEqual("my_coll", result["index"])
        client._client.create_collection.assert_awaited_once_with(
            collection_name="my_coll", schema=schema, index_params=index_params,
        )

    @run_async
    async def test_create_drops_existing_and_waits(self):
        """When collection already exists, drop it and wait for disappearance."""
        client = _make_client()
        schema = mock.MagicMock()
        index_params = mock.MagicMock()
        # Simulate: exists -> drop -> still exists once -> then gone
        client._client.has_collection.side_effect = [True, True, False]

        with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
            result = await client.indices.create(
                index="my_coll",
                body={"schema": schema, "index_params": index_params},
            )

        self.assertTrue(result["acknowledged"])
        client._client.drop_collection.assert_awaited_once_with(collection_name="my_coll")
        client._client.create_collection.assert_awaited_once()

    @run_async
    async def test_create_without_schema_skips_create(self):
        """If no schema/index_params, just return acknowledged without creating."""
        client = _make_client()

        result = await client.indices.create(index="my_coll", body={})
        self.assertTrue(result["acknowledged"])
        client._client.create_collection.assert_not_called()

    @run_async
    async def test_delete(self):
        client = _make_client()

        result = await client.indices.delete(index="my_coll")
        self.assertTrue(result["acknowledged"])
        client._client.drop_collection.assert_awaited_once_with(collection_name="my_coll")

    @run_async
    async def test_exists_true(self):
        client = _make_client()
        client._client.has_collection.return_value = True

        result = await client.indices.exists(index="my_coll")
        self.assertTrue(result)

    @run_async
    async def test_exists_false(self):
        client = _make_client()
        client._client.has_collection.return_value = False

        result = await client.indices.exists(index="my_coll")
        self.assertFalse(result)

    @run_async
    async def test_refresh_no_index_returns_acknowledged(self):
        """refresh(None) is a no-op shortcut."""
        client = _make_client()

        result = await client.indices.refresh(index=None)
        self.assertTrue(result["acknowledged"])

    @run_async
    async def test_refresh_calls_flush_directly(self):
        """With AsyncMilvusClient we call flush() directly — no raw-stub workaround."""
        client = _make_client()
        client._client.flush.return_value = None

        result = await client.indices.refresh(index="my_coll")
        self.assertTrue(result["acknowledged"])
        client._client.flush.assert_awaited_once_with(
            collection_name="my_coll", timeout=client._timeout_admin,
        )

    @run_async
    async def test_refresh_retries_on_rate_limit(self):
        """Milvus throttles flush to 0.1/s — retry on rate-limit errors."""
        client = _make_client()

        call_count = 0

        async def flaky_flush(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("rate limit exceeded")
            return None

        client._client.flush.side_effect = flaky_flush

        with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
            result = await client.indices.refresh(index="my_coll")

        self.assertTrue(result["acknowledged"])
        self.assertEqual(3, call_count)

    @run_async
    async def test_refresh_propagates_non_rate_limit_errors(self):
        client = _make_client()
        client._client.flush.side_effect = RuntimeError("collection not found")

        with self.assertRaises(RuntimeError):
            await client.indices.refresh(index="my_coll")

    @run_async
    async def test_forcemerge_calls_compact(self):
        """forcemerge maps to Milvus compact()."""
        client = _make_client()
        client._client.compact.return_value = 12345
        client._client.get_compaction_state.return_value = "Completed"

        with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
            result = await client.indices.forcemerge(index="my_coll")

        self.assertEqual(1, result["_shards"]["successful"])
        client._client.compact.assert_awaited_once_with(
            collection_name="my_coll", timeout=client._timeout_admin,
        )

    @run_async
    async def test_forcemerge_string_completed_check(self):
        """get_compaction_state returns the string 'Completed', not a bool."""
        client = _make_client()
        client._client.compact.return_value = 42
        # First call: "Executing", second: "Completed"
        client._client.get_compaction_state.side_effect = ["Executing", "Completed"]

        with mock.patch("asyncio.sleep", new_callable=mock.AsyncMock):
            result = await client.indices.forcemerge(index="my_coll")

        self.assertEqual(1, result["_shards"]["successful"])
        self.assertEqual(2, client._client.get_compaction_state.call_count)

    @run_async
    async def test_stats_returns_row_count(self):
        client = _make_client()
        client._client.get_collection_stats.return_value = {"row_count": 1000}

        result = await client.indices.stats(index="my_coll")
        self.assertEqual(1000, result["_all"]["primaries"]["docs"]["count"])
        self.assertEqual(1000, result["_all"]["total"]["docs"]["count"])

    @run_async
    async def test_stats_no_index_returns_empty(self):
        client = _make_client()

        result = await client.indices.stats(index=None)
        self.assertEqual({}, result["_all"]["primaries"])


# =============================================================================
# Cluster Health Tests
# =============================================================================

class MilvusClusterHealthTests(TestCase):

    @run_async
    async def test_health_green_on_success(self):
        client = _make_client()
        client._client.list_collections.return_value = ["coll1"]

        result = await client.cluster.health()
        self.assertEqual("green", result["status"])
        self.assertFalse(result["timed_out"])

    @run_async
    async def test_health_red_on_failure(self):
        client = _make_client()
        client._client.list_collections.side_effect = RuntimeError("connection refused")

        result = await client.cluster.health()
        self.assertEqual("red", result["status"])

    @run_async
    async def test_put_settings_acknowledged(self):
        client = _make_client()
        result = await client.cluster.put_settings(body={})
        self.assertTrue(result["acknowledged"])


# =============================================================================
# Transport Close Tests
# =============================================================================

class MilvusTransportCloseTests(TestCase):

    @run_async
    async def test_close_is_noop(self):
        """Transport.close() is intentionally a no-op to avoid killing gRPC channels."""
        client = _make_client()
        # Should not raise and should not close the pymilvus client
        await client.transport.close()
        # _client should still be set (not torn down)
        self.assertIsNotNone(client._client)

    @run_async
    async def test_perform_request_returns_empty_dict(self):
        client = _make_client()
        result = await client.transport.perform_request("GET", "/")
        self.assertEqual({}, result)


# =============================================================================
# Load Collection Tests
# =============================================================================

class MilvusLoadCollectionTests(TestCase):

    @run_async
    async def test_load_collection_success(self):
        client = _make_client()
        client._client.load_collection.return_value = None

        # Should not raise
        await client.load_collection("my_coll")
        client._client.load_collection.assert_awaited_once_with(
            collection_name="my_coll", timeout=client._timeout_admin,
        )

    @run_async
    async def test_load_collection_already_loaded(self):
        """'already loaded' exceptions are swallowed gracefully."""
        client = _make_client()
        client._client.load_collection.side_effect = RuntimeError(
            "collection already loaded"
        )

        # Should not raise
        await client.load_collection("my_coll")

    @run_async
    async def test_load_collection_load_state_loaded(self):
        """'load state: loaded' exceptions are also swallowed."""
        client = _make_client()
        client._client.load_collection.side_effect = RuntimeError(
            "load state: loaded"
        )

        # Should not raise
        await client.load_collection("my_coll")

    @run_async
    async def test_load_collection_real_error_propagates(self):
        """Non-already-loaded errors should propagate."""
        client = _make_client()
        client._client.load_collection.side_effect = RuntimeError("out of memory")

        with self.assertRaises(RuntimeError):
            await client.load_collection("my_coll")


# =============================================================================
# Close Tests
# =============================================================================

class MilvusDatabaseClientCloseTests(TestCase):

    @run_async
    async def test_close_awaits_client_close(self):
        client = _make_client()
        # Save the mock reference — close() will null client._client
        mock_inner = client._client
        mock_inner.close.return_value = None

        await client.close()

        mock_inner.close.assert_awaited_once()
        self.assertIsNone(client._client)
        self.assertFalse(client._client_initialized)

    @run_async
    async def test_close_swallows_errors(self):
        """Close errors are logged but don't raise — we want graceful shutdown."""
        client = _make_client()
        client._client.close.side_effect = RuntimeError("channel already closed")

        # Should not raise
        await client.close()
        self.assertIsNone(client._client)

    @run_async
    async def test_close_idempotent_when_client_none(self):
        client = _make_client()
        client._client = None

        # Should not raise even though _client is None
        await client.close()
