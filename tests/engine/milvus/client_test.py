# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Unit tests for osbenchmark.engine.milvus.client

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
from osbenchmark.engine.milvus.client import (
    MilvusClientFactory,
    MilvusDatabaseClient,
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
        # OSB metrics store rejects non-semver version strings, so the info() fallback
        # returns a semver-valid default ("2.0.0") rather than "unknown".
        mock_get.side_effect = ConnectionError("refused")

        client = MilvusDatabaseClient(host="myhost")
        result = client.info()

        self.assertEqual("milvus", result["name"])
        self.assertEqual("2.0.0", result["version"]["number"])

    @mock.patch("requests.get")
    def test_info_health_port_fallback(self, mock_get):
        """When REST succeeds but health check fails, version falls back to DEFAULT_VERSION."""
        rest_resp = mock.MagicMock()
        rest_resp.status_code = 200

        mock_get.side_effect = [rest_resp, ConnectionError("health check failed")]

        client = MilvusDatabaseClient(host="myhost")
        result = client.info()

        # Semver-valid default (see info() docstring: "unknown" and "2.x" break the metrics push).
        self.assertEqual("2.0.0", result["version"]["number"])


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
