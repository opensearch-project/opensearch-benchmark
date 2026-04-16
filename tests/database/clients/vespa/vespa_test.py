# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Unit tests for osbenchmark.database.clients.vespa.vespa

Tests VespaClientFactory, VespaDatabaseClient, and all namespace classes
(VespaIndicesNamespace, VespaClusterNamespace, VespaTransportNamespace,
VespaNodesNamespace).
"""
# pylint: disable=protected-access

import asyncio
from unittest import TestCase, mock

from osbenchmark import exceptions
import osbenchmark.database.clients.vespa.vespa as vespa_mod
from osbenchmark.database.clients.vespa.vespa import (
    VespaClientFactory,
    VespaDatabaseClient,
    VespaIndicesNamespace,
    VespaClusterNamespace,
    VespaTransportNamespace,
    VespaNodesNamespace,
)
from tests import run_async


# =============================================================================
# Helpers for mocking aiohttp responses
# =============================================================================

def _mock_response(status=200, json_data=None, text_data=""):
    """Create a mock aiohttp response with async context manager support."""
    resp = mock.AsyncMock()
    resp.status = status
    resp.text = mock.AsyncMock(return_value=text_data)
    resp.json = mock.AsyncMock(return_value=json_data or {})
    resp.__aenter__ = mock.AsyncMock(return_value=resp)
    resp.__aexit__ = mock.AsyncMock(return_value=False)
    return resp


def _mock_session():
    """Create a mock aiohttp ClientSession.

    aiohttp uses `async with session.post(...)` pattern, so methods must
    return objects with __aenter__/__aexit__, not coroutines.
    We use MagicMock for the session so .post/.get/.put/.request return
    synchronously (the _mock_response has async context manager support).
    """
    session = mock.MagicMock()
    session.close = mock.AsyncMock()
    return session


def _make_client(endpoint="http://localhost:8080", **opts):
    """Create a VespaDatabaseClient with a pre-injected mock session."""
    client = VespaDatabaseClient(endpoint=endpoint, **opts)
    client._session = _mock_session()
    client._session_initialized = True
    return client


# =============================================================================
# VespaClientFactory Tests
# =============================================================================

class VespaClientFactoryTests(TestCase):

    def test_create_from_host_list(self):
        factory = VespaClientFactory(
            hosts=[{"host": "myhost", "port": 8080}],
            client_options={},
        )
        client = factory.create_async()
        self.assertEqual("http://myhost:8080", client.endpoint)

    def test_create_from_hosts_with_default_key(self):
        # OSB passes hosts as {"default": [...]} dict when multiple clusters are configured
        factory = VespaClientFactory(
            hosts={"default": [{"host": "myhost", "port": 9999}]},
            client_options={},
        )
        client = factory.create_async()
        self.assertEqual("http://myhost:9999", client.endpoint)

    def test_empty_hosts_raises_error(self):
        factory = VespaClientFactory(hosts=[], client_options={})
        with self.assertRaises(exceptions.SystemSetupError):
            factory.create_async()

    def test_default_scheme_http(self):
        factory = VespaClientFactory(
            hosts=[{"host": "h"}],
            client_options={},
        )
        client = factory.create_async()
        self.assertTrue(client.endpoint.startswith("http://"))

    def test_https_scheme(self):
        factory = VespaClientFactory(
            hosts=[{"host": "h", "port": 443, "scheme": "https"}],
            client_options={},
        )
        client = factory.create_async()
        self.assertEqual("https://h:443", client.endpoint)

    def test_default_port_8080(self):
        factory = VespaClientFactory(
            hosts=[{"host": "h"}],
            client_options={},
        )
        client = factory.create_async()
        self.assertIn("8080", client.endpoint)

    def test_default_host_localhost(self):
        factory = VespaClientFactory(
            hosts=[{}],
            client_options={},
        )
        client = factory.create_async()
        self.assertIn("localhost", client.endpoint)

    def test_create_delegates_to_create_async(self):
        # create() and create_async() return the same client type -- no separate sync impl
        factory = VespaClientFactory(
            hosts=[{"host": "h", "port": 8080}],
            client_options={},
        )
        sync_client = factory.create()
        async_client = factory.create_async()
        self.assertEqual(sync_client.endpoint, async_client.endpoint)

    def test_client_options_passed_through(self):
        factory = VespaClientFactory(
            hosts=[{"host": "h"}],
            client_options={"app_name": "myapp", "namespace": "ns1", "cluster": "content"},
        )
        client = factory.create_async()
        self.assertEqual("myapp", client._app_name)
        self.assertEqual("ns1", client._namespace)
        self.assertEqual("content", client._cluster)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.wait_for_vespa")
    def test_wait_for_rest_layer_success(self, mock_wait):
        mock_wait.return_value = True
        factory = VespaClientFactory(
            hosts=[{"host": "h"}],
            client_options={},
        )
        result = factory.wait_for_rest_layer()
        self.assertTrue(result)
        mock_wait.assert_called_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.wait_for_vespa")
    def test_wait_for_rest_layer_timeout(self, mock_wait):
        mock_wait.return_value = False
        factory = VespaClientFactory(
            hosts=[{"host": "h"}],
            client_options={},
        )
        result = factory.wait_for_rest_layer(max_attempts=5)
        self.assertFalse(result)


# =============================================================================
# VespaDatabaseClient Init Tests
# =============================================================================

class VespaDatabaseClientInitTests(TestCase):

    def test_init_stores_endpoint(self):
        client = VespaDatabaseClient(endpoint="http://host:8080")
        self.assertEqual("http://host:8080", client.endpoint)

    def test_init_strips_trailing_slash(self):
        client = VespaDatabaseClient(endpoint="http://host:8080/")
        self.assertEqual("http://host:8080", client.endpoint)

    def test_init_default_options(self):
        client = VespaDatabaseClient(endpoint="http://host:8080")
        self.assertEqual("default", client._app_name)
        self.assertEqual("benchmark", client._namespace)
        self.assertIsNone(client._cluster)

    def test_init_custom_options(self):
        client = VespaDatabaseClient(
            endpoint="http://h:8080",
            app_name="myapp", namespace="ns", cluster="content",
        )
        self.assertEqual("myapp", client._app_name)
        self.assertEqual("ns", client._namespace)
        self.assertEqual("content", client._cluster)

    def test_init_session_none(self):
        client = VespaDatabaseClient(endpoint="http://h:8080")
        self.assertIsNone(client._session)
        self.assertFalse(client._session_initialized)
        self.assertIsNone(client._sync_session)
        self.assertIsNone(client._search_executor)

    def test_init_creates_namespace_objects(self):
        client = VespaDatabaseClient(endpoint="http://h:8080")
        self.assertIsInstance(client.indices, VespaIndicesNamespace)
        self.assertIsInstance(client.cluster, VespaClusterNamespace)
        self.assertIsInstance(client.transport, VespaTransportNamespace)
        self.assertIsInstance(client.nodes, VespaNodesNamespace)


# =============================================================================
# VespaDatabaseClient Session Tests
# =============================================================================

class VespaDatabaseClientSessionTests(TestCase):

    @mock.patch("osbenchmark.database.clients.vespa.vespa.aiohttp", create=True)
    @run_async
    async def test_aenter_initializes_session(self, mock_aiohttp):
        mock_aiohttp.TraceConfig.return_value = mock.MagicMock()
        mock_aiohttp.TCPConnector.return_value = mock.MagicMock()
        mock_session_instance = mock.AsyncMock()
        mock_aiohttp.ClientSession.return_value = mock_session_instance

        client = VespaDatabaseClient(endpoint="http://h:8080")
        async with client:
            self.assertTrue(client._session_initialized)
            self.assertIsNotNone(client._session)

    @run_async
    async def test_aexit_closes_session(self):
        client = _make_client()
        self.assertIsNotNone(client._session)
        await client.__aexit__(None, None, None)
        self.assertIsNone(client._session)
        self.assertFalse(client._session_initialized)

    @run_async
    async def test_ensure_session_idempotent(self):
        client = _make_client()
        original_session = client._session
        # Calling _ensure_session again should be a no-op because _session_initialized is True
        await client._ensure_session()
        self.assertIs(original_session, client._session)

    @run_async
    async def test_close_resets_initialized_flag(self):
        client = _make_client()
        self.assertTrue(client._session_initialized)
        await client.close()
        self.assertFalse(client._session_initialized)
        self.assertIsNone(client._session)

    @run_async
    async def test_close_handles_sync_session_with_close_httpr(self):
        # Preferred cleanup path: newer pyvespa exposes _close_httpr_client
        client = _make_client()
        mock_sync = mock.MagicMock()
        mock_sync._close_httpr_client = mock.MagicMock()
        client._sync_session = mock_sync
        mock_executor = mock.MagicMock()
        client._search_executor = mock_executor

        await client.close()
        mock_sync._close_httpr_client.assert_called_once()
        mock_executor.shutdown.assert_called_once_with(wait=False)
        self.assertIsNone(client._sync_session)
        self.assertIsNone(client._search_executor)

    @run_async
    async def test_close_handles_sync_session_with_close_httpx_fallback(self):
        # Fallback for older pyvespa that has _close_httpx_client
        client = _make_client()
        mock_sync = mock.MagicMock()
        del mock_sync._close_httpr_client  # remove so it falls through
        mock_sync._close_httpx_client = mock.MagicMock()
        client._sync_session = mock_sync

        await client.close()
        mock_sync._close_httpx_client.assert_called_once()
        self.assertIsNone(client._sync_session)

    @run_async
    async def test_close_handles_sync_session_with_exit_fallback(self):
        # Fallback when no private methods — uses context manager __exit__
        client = _make_client()
        mock_sync = mock.MagicMock()
        del mock_sync._close_httpr_client
        del mock_sync._close_httpx_client
        mock_sync.__exit__ = mock.MagicMock()
        client._sync_session = mock_sync

        await client.close()
        mock_sync.__exit__.assert_called_once_with(None, None, None)
        self.assertIsNone(client._sync_session)

    @run_async
    async def test_close_handles_pyvespa_async_with_close_httpr(self):
        # Preferred cleanup for async session: newer pyvespa
        client = _make_client()
        mock_pyvespa = mock.AsyncMock()
        mock_pyvespa._close_httpr_client = mock.AsyncMock()
        client._pyvespa_async = mock_pyvespa

        await client.close()
        mock_pyvespa._close_httpr_client.assert_awaited_once()
        self.assertIsNone(client._pyvespa_async)

    @run_async
    async def test_close_handles_pyvespa_async_with_close_httpx_fallback(self):
        # Fallback for older pyvespa with _close_httpx_client
        client = _make_client()
        mock_pyvespa = mock.MagicMock()
        del mock_pyvespa._close_httpr_client
        mock_pyvespa._close_httpx_client = mock.AsyncMock()
        client._pyvespa_async = mock_pyvespa

        await client.close()
        mock_pyvespa._close_httpx_client.assert_awaited_once()
        self.assertIsNone(client._pyvespa_async)

    @run_async
    async def test_close_handles_pyvespa_with_aexit_fallback(self):
        # Fallback for pyvespa versions that lack both _close_httpr_client and _close_httpx_client.
        client = _make_client()
        mock_pyvespa = mock.MagicMock()
        del mock_pyvespa._close_httpr_client
        del mock_pyvespa._close_httpx_client
        mock_pyvespa.__aexit__ = mock.AsyncMock()
        client._pyvespa_async = mock_pyvespa

        await client.close()
        mock_pyvespa.__aexit__.assert_awaited_once_with(None, None, None)
        self.assertIsNone(client._pyvespa_async)

    @run_async
    async def test_close_handles_pyvespa_error(self):
        # Close must never propagate errors — a failed cleanup shouldn't crash the benchmark
        client = _make_client()
        mock_pyvespa = mock.AsyncMock()
        mock_pyvespa._close_httpr_client = mock.AsyncMock(
            side_effect=RuntimeError("close failed")
        )
        client._pyvespa_async = mock_pyvespa

        # Should not raise
        await client.close()
        self.assertIsNone(client._pyvespa_async)

    def test_return_raw_response(self):
        client = VespaDatabaseClient(endpoint="http://h:8080")
        client.return_raw_response()
        self.assertTrue(client._request_context.get("raw_response"))


# =============================================================================
# Pyvespa Session Tests
# =============================================================================

class VespaPyvespaSyncSessionTests(TestCase):
    """Tests for _ensure_sync_session() — pyvespa syncio for search."""

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    def test_ensure_sync_session_creates_session(self, mock_pyvespa_cls):
        mock_sync_ctx = mock.MagicMock()
        mock_sync_ctx._open_httpr_client = mock.MagicMock()
        mock_app = mock.MagicMock()
        mock_app.syncio.return_value = mock_sync_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._ensure_sync_session()

        self.assertIsNotNone(client._sync_session)
        self.assertIsNotNone(client._search_executor)
        self.assertEqual(64, client._search_executor._max_workers)
        mock_app.syncio.assert_called_once_with(compress=False)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    def test_ensure_sync_session_respects_max_workers_option(self, mock_pyvespa_cls):
        mock_sync_ctx = mock.MagicMock()
        mock_sync_ctx._open_httpr_client = mock.MagicMock()
        mock_app = mock.MagicMock()
        mock_app.syncio.return_value = mock_sync_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080", max_workers=128)
        client._ensure_sync_session()

        self.assertEqual(128, client._search_executor._max_workers)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    def test_ensure_sync_session_idempotent(self, mock_pyvespa_cls):
        mock_sync_ctx = mock.MagicMock()
        mock_sync_ctx._open_httpr_client = mock.MagicMock()
        mock_app = mock.MagicMock()
        mock_app.syncio.return_value = mock_sync_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._ensure_sync_session()
        first_session = client._sync_session
        client._ensure_sync_session()
        self.assertIs(first_session, client._sync_session)
        mock_pyvespa_cls.assert_called_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", False)
    def test_ensure_sync_session_not_available_raises(self):
        client = VespaDatabaseClient(endpoint="http://h:8080")
        with self.assertRaises(RuntimeError):
            client._ensure_sync_session()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    def test_ensure_sync_session_uses_open_httpr(self, mock_pyvespa_cls):
        # Preferred init: newer pyvespa with _open_httpr_client
        mock_sync_ctx = mock.MagicMock()
        mock_sync_ctx._open_httpr_client = mock.MagicMock()
        mock_app = mock.MagicMock()
        mock_app.syncio.return_value = mock_sync_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._ensure_sync_session()
        mock_sync_ctx._open_httpr_client.assert_called_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    def test_ensure_sync_session_uses_open_httpx_fallback(self, mock_pyvespa_cls):
        # Fallback for older pyvespa with _open_httpx_client
        mock_sync_ctx = mock.MagicMock()
        del mock_sync_ctx._open_httpr_client
        mock_sync_ctx._open_httpx_client = mock.MagicMock()
        mock_app = mock.MagicMock()
        mock_app.syncio.return_value = mock_sync_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._ensure_sync_session()
        mock_sync_ctx._open_httpx_client.assert_called_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    def test_ensure_sync_session_uses_enter_fallback(self, mock_pyvespa_cls):
        # Fallback when no private methods — uses context manager __enter__
        mock_sync_ctx = mock.MagicMock()
        del mock_sync_ctx._open_httpr_client
        del mock_sync_ctx._open_httpx_client
        mock_sync_ctx.__enter__ = mock.MagicMock(return_value=mock_sync_ctx)
        mock_app = mock.MagicMock()
        mock_app.syncio.return_value = mock_sync_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._ensure_sync_session()
        mock_sync_ctx.__enter__.assert_called_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    def test_ensure_sync_session_reuses_pyvespa_app(self, mock_pyvespa_cls):
        # If _pyvespa_app already exists (e.g. from feed_batch), reuse it
        mock_sync_ctx = mock.MagicMock()
        mock_sync_ctx._open_httpr_client = mock.MagicMock()
        mock_app = mock.MagicMock()
        mock_app.syncio.return_value = mock_sync_ctx

        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._pyvespa_app = mock_app  # pre-existing from feed_batch
        client._ensure_sync_session()

        # Should NOT create a new PyvespaApp
        mock_pyvespa_cls.assert_not_called()
        mock_app.syncio.assert_called_once_with(compress=False)


class VespaPyvespaAsyncSessionTests(TestCase):
    """Tests for _ensure_pyvespa_session() — pyvespa async for feeding."""

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_ensure_pyvespa_session_creates_app(self, mock_pyvespa_cls):
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpr_client = mock.AsyncMock()
        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        await client._ensure_pyvespa_session(max_workers=16)

        self.assertIsNotNone(client._pyvespa_app)
        self.assertIsNotNone(client._pyvespa_async)
        self.assertIsNotNone(client._pyvespa_semaphore)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_ensure_pyvespa_session_idempotent(self, mock_pyvespa_cls):
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpr_client = mock.AsyncMock()
        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        await client._ensure_pyvespa_session()
        first_app = client._pyvespa_app
        await client._ensure_pyvespa_session()
        self.assertIs(first_app, client._pyvespa_app)
        # PyvespaApp constructor called only once
        mock_pyvespa_cls.assert_called_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", False)
    @run_async
    async def test_ensure_pyvespa_not_available_raises(self):
        client = VespaDatabaseClient(endpoint="http://h:8080")
        with self.assertRaises(RuntimeError):
            await client._ensure_pyvespa_session()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_ensure_pyvespa_uses_open_httpr(self, mock_pyvespa_cls):
        # Preferred init path: newer pyvespa with _open_httpr_client
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpr_client = mock.AsyncMock()
        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        await client._ensure_pyvespa_session()
        mock_async_ctx._open_httpr_client.assert_called_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_ensure_pyvespa_uses_open_httpx_fallback(self, mock_pyvespa_cls):
        # Fallback for pyvespa that has _open_httpx_client but not _open_httpr_client
        mock_async_ctx = mock.MagicMock()
        del mock_async_ctx._open_httpr_client
        mock_async_ctx._open_httpx_client = mock.AsyncMock()
        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        await client._ensure_pyvespa_session()
        mock_async_ctx._open_httpx_client.assert_called_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_ensure_pyvespa_uses_aenter_fallback(self, mock_pyvespa_cls):
        # Fallback for oldest pyvespa that lacks both _open_httpr_client and _open_httpx_client
        mock_async_ctx = mock.AsyncMock()
        del mock_async_ctx._open_httpr_client
        del mock_async_ctx._open_httpx_client
        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        await client._ensure_pyvespa_session()
        mock_async_ctx.__aenter__.assert_awaited_once()

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_semaphore_created_with_max_workers(self, mock_pyvespa_cls):
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpr_client = mock.AsyncMock()
        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        await client._ensure_pyvespa_session(max_workers=42)
        self.assertIsInstance(client._pyvespa_semaphore, asyncio.Semaphore)
        # Semaphore internal value
        self.assertEqual(42, client._pyvespa_semaphore._value)


# =============================================================================
# Index Tests
# =============================================================================

class VespaIndexTests(TestCase):

    @run_async
    async def test_index_success_with_session(self):
        client = _make_client()
        resp = _mock_response(status=200, text_data='{"result":"created"}')
        client._session.post.return_value = resp

        result = await client.index(index="mytype", body={"field": "val"}, id="doc1")
        self.assertEqual("doc1", result["_id"])
        self.assertEqual("created", result["result"])
        self.assertEqual(1, result["_version"])

    @run_async
    async def test_index_http_error_raises_benchmark_error(self):
        client = _make_client()
        resp = _mock_response(status=400, text_data='{"error":"bad request"}')
        client._session.post.return_value = resp

        with self.assertRaises(exceptions.BenchmarkError):
            await client.index(index="mytype", body={"f": "v"}, id="doc1")

    @run_async
    async def test_index_uses_correct_endpoint(self):
        client = _make_client(endpoint="http://host:8080", namespace="ns")
        resp = _mock_response(status=200, text_data='{}')
        client._session.post.return_value = resp

        await client.index(index="mytype", body={}, id="doc1")
        call_args = client._session.post.call_args
        url = call_args[0][0]
        self.assertEqual("http://host:8080/document/v1/ns/mytype/docid/doc1", url)

    @run_async
    async def test_index_sends_fields_wrapper(self):
        # Vespa document API requires body wrapped in {"fields": ...} envelope
        client = _make_client()
        resp = _mock_response(status=200, text_data='{}')
        client._session.post.return_value = resp

        await client.index(index="mytype", body={"title": "hello"}, id="doc1")
        call_args = client._session.post.call_args
        self.assertEqual({"fields": {"title": "hello"}}, call_args[1]["json"])

    @run_async
    async def test_index_with_cluster_param(self):
        client = _make_client(cluster="content")
        resp = _mock_response(status=200, text_data='{}')
        client._session.post.return_value = resp

        await client.index(index="mytype", body={}, id="doc1")
        call_args = client._session.post.call_args
        self.assertEqual({"destinationCluster": "content"}, call_args[1]["params"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.requests")
    @run_async
    async def test_index_sync_fallback(self, mock_requests):
        # When aiohttp is unavailable, session is None and we fall back to sync requests
        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._session = None
        client._session_initialized = True

        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"result":"created"}'
        mock_requests.post.return_value = mock_resp

        result = await client.index(index="t", body={"f": "v"}, id="d1")
        self.assertEqual("created", result["result"])
        mock_requests.post.assert_called_once()


# =============================================================================
# Update Tests
# =============================================================================

class VespaUpdateTests(TestCase):

    @run_async
    async def test_update_wraps_with_assign(self):
        # Vespa partial updates require each field wrapped as {"assign": value}
        client = _make_client()
        resp = _mock_response(status=200, text_data='{}')
        client._session.put.return_value = resp

        await client.update(index="mytype", body={"title": "new"}, id="doc1")
        call_args = client._session.put.call_args
        expected = {"fields": {"title": {"assign": "new"}}}
        self.assertEqual(expected, call_args[1]["json"])

    @run_async
    async def test_update_success(self):
        client = _make_client()
        resp = _mock_response(status=200, text_data='{}')
        client._session.put.return_value = resp

        result = await client.update(index="mytype", body={"f": "v"}, id="doc1")
        self.assertEqual("doc1", result["_id"])
        self.assertEqual("updated", result["result"])

    @run_async
    async def test_update_http_error_raises(self):
        client = _make_client()
        resp = _mock_response(status=400, text_data='error')
        client._session.put.return_value = resp

        with self.assertRaises(exceptions.BenchmarkError):
            await client.update(index="t", body={"f": "v"}, id="d1")

    @run_async
    async def test_update_uses_correct_endpoint(self):
        client = _make_client(endpoint="http://host:8080", namespace="ns")
        resp = _mock_response(status=200, text_data='{}')
        client._session.put.return_value = resp

        await client.update(index="mytype", body={}, id="doc1")
        url = client._session.put.call_args[0][0]
        self.assertEqual("http://host:8080/document/v1/ns/mytype/docid/doc1", url)

    @run_async
    async def test_update_with_cluster_param(self):
        client = _make_client(cluster="content")
        resp = _mock_response(status=200, text_data='{}')
        client._session.put.return_value = resp

        await client.update(index="t", body={}, id="d1")
        call_args = client._session.put.call_args
        self.assertEqual({"destinationCluster": "content"}, call_args[1]["params"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.requests")
    @run_async
    async def test_update_sync_fallback(self, mock_requests):
        # When aiohttp is unavailable, session is None and we fall back to sync requests
        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._session = None
        client._session_initialized = True

        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"result":"updated"}'
        mock_requests.put.return_value = mock_resp

        result = await client.update(index="t", body={"f": "v"}, id="d1")
        self.assertEqual("updated", result["result"])
        mock_requests.put.assert_called_once()


# =============================================================================
# Search Tests
# =============================================================================

class VespaSearchTests(TestCase):
    """Tests for search() — pyvespa syncio primary path + aiohttp fallback."""

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @run_async
    async def test_search_sends_yql_via_pyvespa(self):
        client = _make_client()
        mock_sync = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.json = {"root": {"children": []}}
        mock_sync.query.return_value = mock_result
        client._sync_session = mock_sync
        client._search_executor = mock.MagicMock()

        # run_in_executor calls the function directly in tests
        async def fake_executor(executor, fn):
            return fn()
        with mock.patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = fake_executor
            await client.search(body={"yql": "select * from t where true"})

        call_args = mock_sync.query.call_args
        body = call_args[1]["body"]
        self.assertIn("yql", body)
        self.assertEqual("select * from t where true", body["yql"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @run_async
    async def test_search_default_timeout(self):
        client = _make_client()
        mock_sync = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.json = {}
        mock_sync.query.return_value = mock_result
        client._sync_session = mock_sync
        client._search_executor = mock.MagicMock()

        async def fake_executor(executor, fn):
            return fn()
        with mock.patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = fake_executor
            await client.search(body={"yql": "select * from t where true"})

        body = mock_sync.query.call_args[1]["body"]
        self.assertEqual("10s", body["timeout"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @run_async
    async def test_search_with_request_params_kwarg(self):
        client = _make_client()
        mock_sync = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.json = {}
        mock_sync.query.return_value = mock_result
        client._sync_session = mock_sync
        client._search_executor = mock.MagicMock()

        async def fake_executor(executor, fn):
            return fn()
        with mock.patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = fake_executor
            await client.search(
                body={"yql": "select * from t where true"},
                request_params={"ranking": "bm25"},
            )

        body = mock_sync.query.call_args[1]["body"]
        self.assertEqual("bm25", body["ranking"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @run_async
    async def test_search_returns_json_response(self):
        client = _make_client()
        expected = {"root": {"children": [{"id": "1"}]}}
        mock_sync = mock.MagicMock()
        mock_result = mock.MagicMock()
        mock_result.json = expected
        mock_sync.query.return_value = mock_result
        client._sync_session = mock_sync
        client._search_executor = mock.MagicMock()

        async def fake_executor(executor, fn):
            return fn()
        with mock.patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = fake_executor
            result = await client.search(body={"yql": "select * from t where true"})

        self.assertEqual(expected, result)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @run_async
    async def test_search_error_raises(self):
        client = _make_client()
        mock_sync = mock.MagicMock()
        mock_sync.query.side_effect = RuntimeError("connection failed")
        client._sync_session = mock_sync
        client._search_executor = mock.MagicMock()

        async def fake_executor(executor, fn):
            return fn()
        with mock.patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = fake_executor
            with self.assertRaises(RuntimeError):
                await client.search(body={"yql": "select * from t where true"})

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @run_async
    async def test_search_vespa_error_returns_empty_response(self):
        """VespaError (e.g., sort attribute warnings) returns empty response instead of raising."""
        client = _make_client()
        mock_sync = mock.MagicMock()
        errors = [{"code": 8, "message": "sort spec: Attribute vector 'timestamp' is not valid"}]
        mock_sync.query.side_effect = vespa_mod.VespaError(errors)
        client._sync_session = mock_sync
        client._search_executor = mock.MagicMock()

        async def fake_executor(executor, fn):
            return fn()
        with mock.patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = fake_executor
            result = await client.search(body={"yql": "select * from t where true"})

        self.assertEqual(0, result["root"]["fields"]["totalCount"])
        self.assertEqual([], result["root"]["children"])
        self.assertEqual(errors, result["root"]["errors"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", False)
    @run_async
    async def test_search_aiohttp_fallback_sends_post(self):
        """When pyvespa unavailable, falls back to aiohttp POST with JSON body."""
        client = _make_client()
        resp = _mock_response(json_data={"root": {"children": []}})
        client._session.post.return_value = resp

        await client.search(body={"yql": "select * from t where true"})
        call_args = client._session.post.call_args
        url = call_args[0][0]
        self.assertIn("/search/", url)
        json_body = call_args[1]["json"]
        self.assertEqual("select * from t where true", json_body["yql"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", False)
    @run_async
    async def test_search_aiohttp_fallback_returns_json(self):
        client = _make_client()
        expected = {"root": {"children": [{"id": "1"}]}}
        resp = _mock_response(json_data=expected)
        client._session.post.return_value = resp

        result = await client.search(body={"yql": "select * from t where true"})
        self.assertEqual(expected, result)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", False)
    @run_async
    async def test_search_aiohttp_fallback_error_raises(self):
        client = _make_client()
        client._session.post.side_effect = RuntimeError("connection failed")

        with self.assertRaises(RuntimeError):
            await client.search(body={"yql": "select * from t where true"})


# =============================================================================
# Bulk Tests
# =============================================================================

class VespaBulkTests(TestCase):

    @run_async
    async def test_bulk_indexes_multiple_docs(self):
        client = _make_client()
        resp = _mock_response(status=200)
        client._session.post.return_value = resp

        docs = [
            {"_id": "1", "fields": {"title": "a"}},
            {"_id": "2", "fields": {"title": "b"}},
        ]
        result = await client.bulk(body=docs, index="mytype")
        self.assertEqual(2, len(result["items"]))
        self.assertEqual(2, client._session.post.call_count)

    @run_async
    async def test_bulk_returns_opensearch_format(self):
        # bulk() returns OpenSearch-compatible response shape so runners don't need special casing
        client = _make_client()
        resp = _mock_response(status=200)
        client._session.post.return_value = resp

        result = await client.bulk(body=[{"_id": "1", "fields": {}}])
        self.assertIn("took", result)
        self.assertIn("errors", result)
        self.assertIn("items", result)
        self.assertFalse(result["errors"])

    @run_async
    async def test_bulk_error_on_http_400(self):
        client = _make_client()
        resp = _mock_response(status=400)
        client._session.post.return_value = resp

        result = await client.bulk(body=[{"_id": "1", "fields": {}}])
        self.assertTrue(result["errors"])
        self.assertEqual(400, result["items"][0]["index"]["status"])

    @run_async
    async def test_bulk_exception_on_doc(self):
        # Per-doc exceptions are captured in the response, not propagated — bulk never throws
        client = _make_client()
        client._session.post.side_effect = RuntimeError("connection error")

        result = await client.bulk(body=[{"_id": "1", "fields": {}}])
        self.assertTrue(result["errors"])
        self.assertIn("error", result["items"][0]["index"])

    @run_async
    async def test_bulk_single_dict_wrapped_in_list(self):
        # Callers may pass a single doc dict instead of a list; bulk normalizes it
        client = _make_client()
        resp = _mock_response(status=200)
        client._session.post.return_value = resp

        result = await client.bulk(body={"_id": "1", "fields": {"f": "v"}})
        self.assertEqual(1, len(result["items"]))

    @run_async
    async def test_bulk_strips_index_key_from_source(self):
        # OpenSearch bulk format includes "index" as metadata; strip it so Vespa
        # doesn't try to store it as a document field
        client = _make_client()
        resp = _mock_response(status=200)
        client._session.post.return_value = resp

        doc = {"_id": "1", "_source": {"title": "a", "index": "should_strip"}}
        await client.bulk(body=[doc])
        call_args = client._session.post.call_args
        json_body = call_args[1]["json"]
        self.assertNotIn("index", json_body["fields"])

    @run_async
    async def test_bulk_with_cluster_param(self):
        client = _make_client(cluster="content")
        resp = _mock_response(status=200)
        client._session.post.return_value = resp

        await client.bulk(body=[{"_id": "1", "fields": {}}])
        call_args = client._session.post.call_args
        self.assertEqual({"destinationCluster": "content"}, call_args[1]["params"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.requests")
    @run_async
    async def test_bulk_sync_fallback(self, mock_requests):
        # When aiohttp is unavailable, session is None and we fall back to sync requests
        client = VespaDatabaseClient(endpoint="http://h:8080")
        client._session = None
        client._session_initialized = True

        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_requests.post.return_value = mock_resp

        result = await client.bulk(body=[{"_id": "1", "fields": {}}])
        self.assertEqual(1, len(result["items"]))
        mock_requests.post.assert_called_once()


# =============================================================================
# Feed Batch Tests
# =============================================================================

class VespaFeedBatchTests(TestCase):

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_feed_batch_calls_feed_data_point(self, mock_pyvespa_cls):
        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_resp.json = {}

        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpx_client = mock.AsyncMock()
        mock_async_ctx.feed_data_point = mock.AsyncMock(return_value=mock_resp)

        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        docs = [
            {"_id": "1", "fields": {"title": "a"}},
            {"_id": "2", "fields": {"title": "b"}},
        ]
        result = await client.feed_batch(docs, schema="mytype", max_workers=4)
        self.assertEqual(0, result["errors"])
        self.assertEqual(2, len(result["responses"]))
        self.assertEqual(2, mock_async_ctx.feed_data_point.call_count)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_feed_batch_retries_on_connection_error(self, mock_pyvespa_cls):
        # pyvespa's built-in retry handles 429/503 but NOT connection-level errors,
        # so we added manual retry with exponential backoff for those
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpx_client = mock.AsyncMock()
        # Fail twice, then succeed
        mock_good_resp = mock.MagicMock()
        mock_good_resp.status_code = 200
        mock_async_ctx.feed_data_point = mock.AsyncMock(
            side_effect=[ConnectionError("fail"), ConnectionError("fail"), mock_good_resp]
        )

        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = await client.feed_batch(
            [{"_id": "1", "fields": {}}], schema="t", max_workers=1,
        )
        self.assertEqual(0, result["errors"])
        self.assertEqual(3, mock_async_ctx.feed_data_point.call_count)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_feed_batch_no_retry_on_encoding_error(self, mock_pyvespa_cls):
        # Encoding errors are data issues (e.g. lone surrogates), not transient -- skip immediately
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpx_client = mock.AsyncMock()
        mock_async_ctx.feed_data_point = mock.AsyncMock(
            side_effect=UnicodeEncodeError("utf-8", "", 0, 1, "bad")
        )

        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = await client.feed_batch(
            [{"_id": "1", "fields": {}}], schema="t", max_workers=1,
        )
        self.assertEqual(1, result["errors"])
        # Only called once — no retry
        self.assertEqual(1, mock_async_ctx.feed_data_point.call_count)

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_feed_batch_returns_error_count(self, mock_pyvespa_cls):
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpx_client = mock.AsyncMock()
        # All retries fail
        mock_async_ctx.feed_data_point = mock.AsyncMock(
            side_effect=ConnectionError("always fail")
        )

        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = await client.feed_batch(
            [{"_id": "1", "fields": {}}, {"_id": "2", "fields": {}}],
            schema="t", max_workers=1,
        )
        self.assertEqual(2, result["errors"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_feed_batch_status_400_counts_as_error(self, mock_pyvespa_cls):
        # A successful HTTP response with status >= 400 is still an error (e.g. bad schema)
        mock_resp = mock.MagicMock()
        mock_resp.status_code = 400
        mock_resp.json = {"message": "bad doc"}

        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpx_client = mock.AsyncMock()
        mock_async_ctx.feed_data_point = mock.AsyncMock(return_value=mock_resp)

        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = await client.feed_batch(
            [{"_id": "1", "fields": {}}], schema="t", max_workers=1,
        )
        self.assertEqual(1, result["errors"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_feed_batch_with_cluster_destination(self, mock_pyvespa_cls):
        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpx_client = mock.AsyncMock()
        mock_async_ctx.feed_data_point = mock.AsyncMock(return_value=mock_resp)
        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080", cluster="content")
        await client.feed_batch([{"_id": "1", "fields": {}}], schema="t")
        call_kwargs = mock_async_ctx.feed_data_point.call_args[1]
        self.assertEqual("content", call_kwargs["destinationCluster"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.database.clients.vespa.vespa.PyvespaApp")
    @run_async
    async def test_feed_batch_with_custom_namespace(self, mock_pyvespa_cls):
        mock_resp = mock.MagicMock()
        mock_resp.status_code = 200
        mock_async_ctx = mock.MagicMock()
        mock_async_ctx._open_httpx_client = mock.AsyncMock()
        mock_async_ctx.feed_data_point = mock.AsyncMock(return_value=mock_resp)
        mock_app = mock.MagicMock()
        mock_app.asyncio.return_value = mock_async_ctx
        mock_pyvespa_cls.return_value = mock_app

        client = VespaDatabaseClient(endpoint="http://h:8080")
        await client.feed_batch(
            [{"_id": "1", "fields": {}}], schema="t", namespace="custom_ns",
        )
        call_kwargs = mock_async_ctx.feed_data_point.call_args[1]
        self.assertEqual("custom_ns", call_kwargs["namespace"])


# =============================================================================
# Indices Namespace Tests
# =============================================================================

class VespaIndicesNamespaceTests(TestCase):

    @run_async
    async def test_create_returns_acknowledged(self):
        client = _make_client()
        resp = _mock_response(status=200)
        client._session.get.return_value = resp

        result = await client.indices.create(index="myindex")
        self.assertTrue(result["acknowledged"])
        self.assertTrue(result["shards_acknowledged"])
        self.assertEqual("myindex", result["index"])

    @run_async
    async def test_delete_returns_acknowledged(self):
        client = _make_client()
        resp = _mock_response(status=200)
        client._session.get.return_value = resp

        result = await client.indices.delete(index="myindex")
        self.assertTrue(result["acknowledged"])

    @run_async
    async def test_exists_returns_true(self):
        client = _make_client()
        resp = _mock_response(status=200)
        client._session.get.return_value = resp

        result = await client.indices.exists(index="myindex")
        self.assertTrue(result)

    @run_async
    async def test_refresh_returns_shards(self):
        client = _make_client()
        resp = _mock_response(status=200)
        client._session.get.return_value = resp

        result = await client.indices.refresh(index="myindex")
        self.assertTrue(result["acknowledged"])
        self.assertEqual(1, result["_shards"]["total"])
        self.assertEqual(1, result["_shards"]["successful"])
        self.assertEqual(0, result["_shards"]["failed"])

    @run_async
    async def test_stats_calls_metrics_endpoint(self):
        # Vespa uses /metrics/v2/values instead of OpenSearch's _stats API
        client = _make_client()
        metrics_data = {"nodes": []}
        resp = _mock_response(json_data=metrics_data)
        client._session.get.return_value = resp

        result = await client.indices.stats(index="myindex")
        url = client._session.get.call_args[0][0]
        self.assertIn("/metrics/v2/values", url)
        self.assertIn("_all", result)

    @run_async
    async def test_stats_exception_returns_default(self):
        # Metrics fetch failure returns empty stub so telemetry doesn't crash
        client = _make_client()
        client._session.get.side_effect = RuntimeError("fail")

        result = await client.indices.stats()
        self.assertEqual({}, result["_all"]["primaries"])
        self.assertEqual({}, result["_all"]["total"])

    @run_async
    async def test_forcemerge_wait_for_completion_true(self):
        client = _make_client()
        result = await client.indices.forcemerge()
        self.assertIn("_shards", result)
        self.assertEqual(1, result["_shards"]["total"])

    @run_async
    async def test_forcemerge_wait_for_completion_false(self):
        client = _make_client()
        result = await client.indices.forcemerge(wait_for_completion=False)
        self.assertEqual("vespa-node:1", result["task"])

    @run_async
    async def test_forcemerge_wait_for_completion_string_false(self):
        # OSB may pass "false" as a string from params; must handle both str and bool
        client = _make_client()
        result = await client.indices.forcemerge(wait_for_completion="false")
        self.assertEqual("vespa-node:1", result["task"])


# =============================================================================
# Cluster Namespace Tests
# =============================================================================

class VespaClusterNamespaceTests(TestCase):

    @run_async
    async def test_health_maps_up_to_green(self):
        client = _make_client()
        resp = _mock_response(json_data={"status": {"code": "up"}})
        client._session.get.return_value = resp

        result = await client.cluster.health()
        self.assertEqual("green", result["status"])

    @run_async
    async def test_health_maps_down_to_red(self):
        client = _make_client()
        resp = _mock_response(json_data={"status": {"code": "down"}})
        client._session.get.return_value = resp

        result = await client.cluster.health()
        self.assertEqual("red", result["status"])

    @run_async
    async def test_health_maps_initializing_to_yellow(self):
        client = _make_client()
        resp = _mock_response(json_data={"status": {"code": "initializing"}})
        client._session.get.return_value = resp

        result = await client.cluster.health()
        self.assertEqual("yellow", result["status"])

    @run_async
    async def test_health_unknown_status_defaults_yellow(self):
        # Unrecognized Vespa status codes (e.g. "maintenance") map to yellow rather than red
        client = _make_client()
        resp = _mock_response(json_data={"status": {"code": "maintenance"}})
        client._session.get.return_value = resp

        result = await client.cluster.health()
        self.assertEqual("yellow", result["status"])

    @run_async
    async def test_health_exception_returns_red(self):
        # Connection failure is treated as cluster down, not propagated as an exception
        client = _make_client()
        client._session.get.side_effect = RuntimeError("connection refused")

        result = await client.cluster.health()
        self.assertEqual("red", result["status"])

    @run_async
    async def test_health_includes_all_keys(self):
        # Ensures the response has all keys that OSB telemetry/scheduler expect from OpenSearch
        client = _make_client()
        resp = _mock_response(json_data={"status": {"code": "up"}})
        client._session.get.return_value = resp

        result = await client.cluster.health()
        self.assertIn("cluster_name", result)
        self.assertIn("status", result)
        self.assertIn("timed_out", result)
        self.assertIn("number_of_nodes", result)
        self.assertIn("number_of_data_nodes", result)
        self.assertIn("active_primary_shards", result)
        self.assertIn("relocating_shards", result)
        self.assertFalse(result["timed_out"])
        self.assertEqual(0, result["relocating_shards"])

    @run_async
    async def test_put_settings_noop(self):
        client = _make_client()
        result = await client.cluster.put_settings(body={})
        self.assertTrue(result["acknowledged"])


# =============================================================================
# Transport Namespace Tests
# =============================================================================

class VespaTransportNamespaceTests(TestCase):

    @run_async
    async def test_perform_request_calls_session(self):
        client = _make_client()
        resp = _mock_response(json_data={"result": "ok"})
        client._session.request.return_value = resp

        await client.transport.perform_request("GET", "/test/path")
        client._session.request.assert_called_once()
        call_args = client._session.request.call_args
        self.assertEqual("GET", call_args[0][0])
        self.assertIn("/test/path", call_args[0][1])

    @run_async
    async def test_perform_request_returns_json(self):
        client = _make_client()
        expected = {"status": "ok"}
        resp = _mock_response(json_data=expected)
        client._session.request.return_value = resp

        result = await client.transport.perform_request("GET", "/path")
        self.assertEqual(expected, result)

    @run_async
    async def test_close_delegates_to_client(self):
        client = _make_client()
        await client.transport.close()
        self.assertIsNone(client._session)


# =============================================================================
# Nodes Namespace Tests
# =============================================================================

class VespaNodesNamespaceTests(TestCase):

    def test_stats_returns_stub_structure(self):
        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = client.nodes.stats()
        self.assertIn("nodes", result)
        node = result["nodes"]["vespa-node-1"]
        self.assertIn("os", node)
        self.assertIn("jvm", node)
        self.assertIn("mem", node["jvm"])
        self.assertIn("gc", node["jvm"])

    def test_stats_includes_endpoint(self):
        client = VespaDatabaseClient(endpoint="http://myhost:8080")
        result = client.nodes.stats()
        node = result["nodes"]["vespa-node-1"]
        self.assertEqual("http://myhost:8080", node["host"])

    def test_info_returns_stub_structure(self):
        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = client.nodes.info()
        node = result["nodes"]["vespa-node-1"]
        self.assertEqual("8.0.0", node["version"])
        self.assertIn("os", node)
        self.assertIn("jvm", node)


# =============================================================================
# Info Tests
# =============================================================================

class VespaInfoTests(TestCase):

    @mock.patch("osbenchmark.database.clients.vespa.vespa.requests")
    def test_info_returns_version(self, mock_requests):
        mock_resp = mock.MagicMock()
        mock_resp.json.return_value = {
            "application": {"vespa": {"version": "8.300.1"}}
        }
        mock_requests.get.return_value = mock_resp

        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = client.info()
        self.assertEqual("8.300.1", result["version"]["number"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.requests")
    def test_info_returns_distribution_vespa(self, mock_requests):
        mock_resp = mock.MagicMock()
        mock_resp.json.return_value = {
            "application": {"vespa": {"version": "8.0.0"}}
        }
        mock_requests.get.return_value = mock_resp

        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = client.info()
        self.assertEqual("vespa", result["version"]["distribution"])
        self.assertEqual("vespa", result["name"])

    @mock.patch("osbenchmark.database.clients.vespa.vespa.requests")
    def test_info_error_returns_default_version(self, mock_requests):
        # info() is called during setup; must not crash if Vespa is still starting.
        # Must return valid semver — OSB's metrics store parses version.number via
        # versions.components() which rejects non-semver strings like "unknown".
        mock_requests.get.side_effect = RuntimeError("connection refused")

        client = VespaDatabaseClient(endpoint="http://h:8080")
        result = client.info()
        self.assertEqual("8.0.0", result["version"]["number"])
