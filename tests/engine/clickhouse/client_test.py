# SPDX-License-Identifier: Apache-2.0
"""Tests for osbenchmark.engine.clickhouse.client."""
# pylint: disable=protected-access,no-name-in-module,import-outside-toplevel,unused-import,no-self-argument,unused-variable

import asyncio
from unittest import TestCase, IsolatedAsyncioTestCase, mock

from osbenchmark import exceptions
from osbenchmark.engine.clickhouse import client as ch_client_mod  # noqa: F401
from osbenchmark.engine.clickhouse.client import (
    ClickHouseClientFactory,
    ClickHouseDatabaseClient,
    _NodesProxy,
)


def _summary(elapsed_ns="1000000"):
    """Build a mock QuerySummary-like object."""
    s = mock.MagicMock()
    s.summary = {"elapsed_ns": elapsed_ns}
    return s


def _query_result(rows=None, columns=None, elapsed_ns="1000000"):
    r = mock.MagicMock()
    r.result_rows = rows or []
    r.column_names = tuple(columns or [])
    r.summary = {"elapsed_ns": elapsed_ns}
    return r


class ClickHouseClientFactoryTests(TestCase):

    def test_hosts_list_of_dicts(self):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], {})
        self.assertEqual(f.hosts, [{"host": "h", "port": 8123}])

    def test_none_client_options_becomes_dict(self):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], None)
        self.assertEqual(f.client_options, {})

    def test_create_returns_client(self):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], {})
        c = f.create()
        self.assertIsInstance(c, ClickHouseDatabaseClient)

    def test_create_async_returns_client(self):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], {})
        c = f.create_async()
        self.assertIsInstance(c, ClickHouseDatabaseClient)

    def test_create_and_async_produce_distinct_instances(self):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], {})
        c1 = f.create()
        c2 = f.create_async()
        self.assertIsNot(c1, c2)

    def test_ssl_verify_passed_through(self):
        f = ClickHouseClientFactory([{"host": "h", "port": 8443}], {"ssl_verify": False})
        c = f.create()
        self.assertEqual(c.client_options["ssl_verify"], False)

    @mock.patch("osbenchmark.engine.clickhouse.helpers.wait_for_clickhouse", return_value=True)
    def test_wait_for_rest_layer_delegates(self, mock_wait):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], {})
        self.assertTrue(f.wait_for_rest_layer(max_attempts=3))
        mock_wait.assert_called_once()
        # first arg is an ephemeral client
        first_arg = mock_wait.call_args[0][0]
        self.assertIsInstance(first_arg, ClickHouseDatabaseClient)
        self.assertEqual(mock_wait.call_args.kwargs["max_attempts"], 3)

    @mock.patch("osbenchmark.engine.clickhouse.helpers.wait_for_clickhouse", return_value=False)
    def test_wait_for_rest_layer_timeout(self, _mock_wait):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], {})
        self.assertFalse(f.wait_for_rest_layer(max_attempts=1))


class ClickHouseClientInitTests(TestCase):

    def _client(self, **opts):
        return ClickHouseDatabaseClient([{"host": "h", "port": 8123}], opts)

    def test_namespace_proxies(self):
        c = self._client()
        self.assertIs(c.indices, c)
        self.assertIs(c.cluster, c)
        self.assertIs(c.transport, c)
        self.assertIsInstance(c.nodes, _NodesProxy)

    def test_nodes_proxy_stats_returns_dict(self):
        c = self._client()
        stats = c.nodes.stats()
        self.assertIn("nodes", stats)
        self.assertIn("cluster_name", stats)

    def test_initial_client_state(self):
        c = self._client()
        self.assertIsNone(c._client)
        self.assertIsNone(c._sync_client)
        self.assertIsNone(c._database)

    def test_lock_is_asyncio_lock(self):
        c = self._client()
        self.assertIsInstance(c._client_lock, asyncio.Lock)

    def test_endpoint_derived_from_hosts(self):
        c = self._client()
        self.assertEqual(c.endpoint, "http://h:8123")
        secure = ClickHouseDatabaseClient([{"host": "h", "port": 8443}], {})
        self.assertEqual(secure.endpoint, "https://h:8443")


class ClickHouseClientLifecycleTests(IsolatedAsyncioTestCase):

    async def test_aenter_returns_self(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        async with c as ctx:
            self.assertIs(ctx, c)

    async def test_aexit_calls_close(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        with mock.patch.object(c, "close", new=mock.AsyncMock()) as m:
            async with c:
                pass
            m.assert_awaited_once()

    async def test_ensure_client_missing_extra_raises(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        with mock.patch("osbenchmark.engine.clickhouse.client.CLICKHOUSE_CONNECT_AVAILABLE", False):
            with self.assertRaises(exceptions.SystemSetupError):
                await c._ensure_client()

    async def test_ensure_client_idempotent(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        fake = mock.AsyncMock()
        c._client = fake
        got = await c._ensure_client()
        self.assertIs(got, fake)

    async def test_concurrent_ensure_client_creates_one_client(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        call_count = 0

        async def slow_get_async_client(**_kwargs):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.01)  # simulate handshake delay
            return mock.AsyncMock()

        fake_mod = mock.MagicMock()
        fake_mod.get_async_client = slow_get_async_client
        with mock.patch("osbenchmark.engine.clickhouse.client.clickhouse_connect", fake_mod):
            with mock.patch("osbenchmark.engine.clickhouse.client.CLICKHOUSE_CONNECT_AVAILABLE", True):
                await asyncio.gather(*(c._ensure_client() for _ in range(50)))
        self.assertEqual(call_count, 1)

    async def test_close_resets_state(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        c._client = mock.AsyncMock()
        c._client.close = mock.AsyncMock()
        c._sync_client = mock.MagicMock()
        await c.close()
        self.assertIsNone(c._client)
        self.assertIsNone(c._sync_client)

    async def test_close_swallows_exceptions(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        c._client = mock.AsyncMock()
        c._client.close = mock.AsyncMock(side_effect=RuntimeError("kaboom"))
        # Should not raise
        await c.close()
        self.assertIsNone(c._client)

    async def test_close_honors_timeout(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})

        # Build a fake AsyncClient whose close hangs forever
        class _HangingClient:
            async def close(self_inner):
                await asyncio.sleep(30)

        c._client = _HangingClient()
        # Patch wait_for to trigger a TimeoutError quickly
        original_wait_for = asyncio.wait_for

        async def fast_wait(coro, timeout):  # pylint: disable=unused-argument
            # cancel the coro to avoid warnings
            task = asyncio.ensure_future(coro)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            raise asyncio.TimeoutError()

        with mock.patch("osbenchmark.engine.clickhouse.client.asyncio.wait_for", side_effect=fast_wait):
            await c.close()
        self.assertIsNone(c._client)


class ClickHouseBulkTests(IsolatedAsyncioTestCase):

    def _client_with_mock(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        return c, m

    async def test_native_path_multi_row(self):
        c, m = self._client_with_mock()
        m.insert.return_value = _summary("5000000")
        body = [
            {"index": {"_id": "1"}}, {"a": 1, "b": "x"},
            {"index": {"_id": "2"}}, {"a": 2, "b": "y"},
        ]
        result = await c.bulk(body=body, index="t", params={"column-names": ["a", "b"]})
        self.assertFalse(result["errors"])
        self.assertEqual(result["took"], 5)
        self.assertEqual(len(result["items"]), 2)
        m.insert.assert_awaited_once()

    async def test_missing_columns_triggers_json_fallback(self):
        c, m = self._client_with_mock()
        m.command.return_value = _summary("2000000")
        body = [{"index": {"_id": "1"}}, {"a": 1}]
        result = await c.bulk(body=body, index="t", params={})
        self.assertFalse(result["errors"])
        m.command.assert_awaited_once()
        m.insert.assert_not_called()

    async def test_extra_keys_triggers_json_fallback(self):
        c, m = self._client_with_mock()
        m.command.return_value = _summary("2000000")
        body = [{"index": {"_id": "1"}}, {"a": 1, "extra": 9}]
        result = await c.bulk(body=body, index="t", params={"column-names": ["a"]})
        self.assertFalse(result["errors"])
        m.command.assert_awaited_once()
        m.insert.assert_not_called()

    async def test_insert_mode_json_forces_fallback(self):
        c, m = self._client_with_mock()
        m.command.return_value = _summary()
        body = [{"index": {"_id": "1"}}, {"a": 1}]
        result = await c.bulk(body=body, index="t",
                              params={"column-names": ["a"], "insert-mode": "json"})
        self.assertFalse(result["errors"])
        m.command.assert_awaited_once()
        m.insert.assert_not_called()

    async def test_empty_body_returns_empty(self):
        c, m = self._client_with_mock()
        result = await c.bulk(body=[], index="t", params={"column-names": ["a"]})
        self.assertEqual(result, {"took": 0, "errors": False, "items": []})
        m.insert.assert_not_called()

    async def test_insert_exception_returns_single_error(self):
        c, m = self._client_with_mock()
        m.insert.side_effect = RuntimeError("connection reset")
        body = [{"index": {"_id": "1"}}, {"a": 1, "b": "x"}]
        result = await c.bulk(body=body, index="t", params={"column-names": ["a", "b"]})
        self.assertTrue(result["errors"])
        self.assertEqual(result["items"], [])
        self.assertIn("connection reset", result["_bulk_error"])

    async def test_strict_mismatch_raises(self):
        c, _m = self._client_with_mock()
        body = [{"index": {"_id": "1"}}, {"a": 1}]
        # column-names ["a","b"] but doc has only "a" and no extra keys -> strict Native path
        with self.assertRaises(exceptions.BenchmarkError):
            await c.bulk(body=body, index="t", params={"column-names": ["a", "b"]})

    async def test_delete_action_rejected_at_parse(self):
        c, _m = self._client_with_mock()
        body = [{"delete": {"_id": "1"}}, {"a": 1}]
        with self.assertRaises(exceptions.BenchmarkError):
            await c.bulk(body=body, index="t", params={"column-names": ["a"]})


class ClickHouseIndexTests(IsolatedAsyncioTestCase):

    async def test_index_success(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        result = await c.index(index="t", body={"a": 1}, id="42")
        self.assertEqual(result["_id"], "42")
        self.assertEqual(result["result"], "created")
        m.insert.assert_awaited_once()

    async def test_column_ordering_from_params(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        await c.index(index="t", body={"a": 1, "b": 2}, params={"column-names": ["b", "a"]})
        args, kwargs = m.insert.await_args
        # rows is a list of tuples aligned with column_names
        self.assertEqual(kwargs["column_names"], ["b", "a"])
        self.assertEqual(args[1], [(2, 1)])

    async def test_id_default_empty_string(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        c._client = mock.AsyncMock()
        result = await c.index(index="t", body={"a": 1})
        self.assertEqual(result["_id"], "")


class ClickHouseSearchTests(IsolatedAsyncioTestCase):

    def _client_with_mock(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        return c, m

    async def test_query_executed(self):
        c, m = self._client_with_mock()
        m.query.return_value = _query_result([(1, "a")], ["id", "name"], "3000000")
        resp = await c.search(body={"sql": "SELECT id, name FROM t"})
        self.assertEqual(resp["took"], 3)
        self.assertEqual(len(resp["hits"]["hits"]), 1)
        m.query.assert_awaited_once()

    async def test_missing_sql_raises(self):
        c, _m = self._client_with_mock()
        with self.assertRaises(exceptions.BenchmarkError):
            await c.search(body={})

    async def test_elapsed_string_to_int_took(self):
        c, m = self._client_with_mock()
        m.query.return_value = _query_result([], [], "9000000")
        resp = await c.search(body={"sql": "SELECT 1"})
        self.assertEqual(resp["took"], 9)

    async def test_numpy_parameters_coerced(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("numpy not available")
        c, m = self._client_with_mock()
        m.query.return_value = _query_result([], [])
        await c.search(body={"sql": "SELECT 1", "parameters": {"v": np.array([1.0, 2.0])}})
        _args, kwargs = m.query.await_args
        self.assertEqual(kwargs["parameters"]["v"], [1.0, 2.0])

    async def test_settings_passed(self):
        c, m = self._client_with_mock()
        m.query.return_value = _query_result([], [])
        await c.search(body={"sql": "SELECT 1", "settings": {"max_execution_time": 10}})
        _args, kwargs = m.query.await_args
        self.assertEqual(kwargs["settings"], {"max_execution_time": 10})

    async def test_unknown_keys_warned(self):
        c, m = self._client_with_mock()
        m.query.return_value = _query_result([], [])
        with self.assertLogs("osbenchmark.engine.clickhouse.client", level="WARNING") as cm:
            await c.search(body={"sql": "SELECT 1", "bogus": "x"})
        self.assertTrue(any("bogus" in msg for msg in cm.output))


class ClickHouseIndicesTests(IsolatedAsyncioTestCase):

    def _client_with_mock(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        return c, m

    async def test_create_runs_ddl(self):
        c, m = self._client_with_mock()
        ddl = "CREATE TABLE t (a UInt64) ENGINE=MergeTree ORDER BY a"
        result = await c.indices.create(index="t", body={"ddl": ddl})
        self.assertTrue(result["acknowledged"])
        m.command.assert_awaited_once_with(ddl)

    async def test_create_ddl_file_resolved(self, ):
        c, m = self._client_with_mock()
        import tempfile
        import os as _os
        with tempfile.TemporaryDirectory() as td:
            path = _os.path.join(td, "table.sql")
            with open(path, "w", encoding="utf-8") as fp:
                fp.write("CREATE TABLE t (a UInt64) ENGINE=MergeTree ORDER BY a")
            # relative path -> uses workload-path
            rel = "table.sql"
            await c.indices.create(index="t", body={"ddl-file": rel},
                                   params={"workload-path": td})
            m.command.assert_awaited_once()
            self.assertIn("CREATE TABLE", m.command.await_args.args[0])

    async def test_create_missing_ddl_raises(self):
        c, _m = self._client_with_mock()
        with self.assertRaises(exceptions.BenchmarkError):
            await c.indices.create(index="t", body={})

    async def test_delete_runs_drop(self):
        c, m = self._client_with_mock()
        await c.indices.delete(index="t")
        m.command.assert_awaited_once()
        self.assertIn("DROP TABLE", m.command.await_args.args[0])
        self.assertIn("`t`", m.command.await_args.args[0])

    async def test_exists_with_dot_syntax(self):
        c, m = self._client_with_mock()
        m.query.return_value = mock.MagicMock(result_rows=[(1,)])
        result = await c.indices.exists(index="mydb.mytable")
        self.assertTrue(result)
        _args, kwargs = m.query.await_args
        self.assertEqual(kwargs["parameters"]["db"], "mydb")
        self.assertEqual(kwargs["parameters"]["name"], "mytable")

    async def test_exists_with_explicit_database_kwarg(self):
        c, m = self._client_with_mock()
        m.query.return_value = mock.MagicMock(result_rows=[(0,)])
        result = await c.indices.exists(index="t", database="mydb")
        self.assertFalse(result)
        _args, kwargs = m.query.await_args
        self.assertEqual(kwargs["parameters"]["db"], "mydb")

    async def test_refresh_no_op(self):
        c, m = self._client_with_mock()
        result = await c.indices.refresh(index="t")
        self.assertIn("_shards", result)
        m.command.assert_not_called()

    async def test_forcemerge_runs_optimize(self):
        c, m = self._client_with_mock()
        await c.indices.forcemerge(index="t")
        m.command.assert_awaited_once()
        self.assertIn("OPTIMIZE TABLE", m.command.await_args.args[0])
        self.assertIn("FINAL", m.command.await_args.args[0])


class ClickHouseHealthTests(IsolatedAsyncioTestCase):

    async def test_ping_true_green(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        m.ping.return_value = True
        c._client = m
        result = await c.cluster.health()
        self.assertEqual(result["status"], "green")

    async def test_ping_false_red(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        m.ping.return_value = False
        c._client = m
        result = await c.cluster.health()
        self.assertEqual(result["status"], "red")

    async def test_cluster_name_option(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {"cluster_name": "my-cluster"})
        m = mock.AsyncMock()
        m.ping.return_value = True
        c._client = m
        result = await c.cluster.health()
        self.assertEqual(result["cluster_name"], "my-cluster")


class ClickHouseStatsTests(IsolatedAsyncioTestCase):

    async def test_named_table(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        m.query.return_value = mock.MagicMock(result_rows=[(100, 4096)])
        stats = await c.indices.stats(index="t")
        self.assertEqual(stats["_all"]["primaries"]["docs"]["count"], 100)
        self.assertIn("t", stats["indices"])

    async def test_all_tables(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        m.query.return_value = mock.MagicMock(result_rows=[(0, 0)])
        stats = await c.indices.stats()
        self.assertEqual(stats["indices"], {})

    async def test_explicit_database_kwarg(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        m.query.return_value = mock.MagicMock(result_rows=[(10, 100)])
        await c.indices.stats(index="t", database="mydb")
        _args, kwargs = m.query.await_args
        self.assertEqual(kwargs["parameters"]["db"], "mydb")

    async def test_envelope_shape(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        m.query.return_value = mock.MagicMock(result_rows=[(5, 50)])
        stats = await c.indices.stats(index="metrics")
        self.assertEqual(set(stats.keys()), {"_all", "indices"})
        self.assertEqual(set(stats["_all"].keys()), {"primaries", "total"})


class ClickHouseInfoTests(TestCase):

    def test_version_extraction(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        sync_mock = mock.MagicMock()
        sync_mock.query.return_value = mock.MagicMock(result_rows=[("24.8.1.2684",)])
        with mock.patch.object(c, "_ensure_sync_client", return_value=sync_mock):
            info = c.info()
        self.assertEqual(info["version"]["number"], "24.8.1")
        self.assertEqual(info["version"]["distribution"], "clickhouse")

    def test_exception_uses_fallback(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        with mock.patch.object(c, "_ensure_sync_client", side_effect=RuntimeError("net")):
            with self.assertLogs("osbenchmark.engine.clickhouse.client", level="WARNING") as cm:
                info = c.info()
        self.assertEqual(info["version"]["number"], "24.8.0")
        self.assertTrue(any("info()" in m for m in cm.output))

    def test_malformed_version_falls_back(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        sync_mock = mock.MagicMock()
        sync_mock.query.return_value = mock.MagicMock(result_rows=[("head-fcbd7a4",)])
        with mock.patch.object(c, "_ensure_sync_client", return_value=sync_mock):
            info = c.info()
        self.assertEqual(info["version"]["number"], "24.8.0")


class WaitForRestLayerTests(TestCase):

    @mock.patch("osbenchmark.engine.clickhouse.helpers.wait_for_clickhouse", return_value=True)
    def test_success(self, mock_wait):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], {})
        self.assertTrue(f.wait_for_rest_layer(max_attempts=5))
        self.assertEqual(mock_wait.call_args.kwargs["max_attempts"], 5)

    @mock.patch("osbenchmark.engine.clickhouse.helpers.wait_for_clickhouse", return_value=False)
    def test_timeout(self, _mock_wait):
        f = ClickHouseClientFactory([{"host": "h", "port": 8123}], {})
        self.assertFalse(f.wait_for_rest_layer(max_attempts=1))

    @mock.patch("osbenchmark.engine.clickhouse.helpers.wait_for_clickhouse", return_value=True)
    def test_delegates_with_endpoint(self, mock_wait):
        f = ClickHouseClientFactory([{"host": "h", "port": 8443}], {"ssl_verify": False})
        f.wait_for_rest_layer(max_attempts=1)
        ephemeral = mock_wait.call_args[0][0]
        self.assertEqual(ephemeral.endpoint, "https://h:8443")
        self.assertEqual(ephemeral.client_options["ssl_verify"], False)


class NodesStatsSyncTests(TestCase):

    def test_sync_stub_returns_envelope(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {"cluster_name": "abc"})
        stats = c.nodes.stats()
        self.assertEqual(stats["nodes"], {})
        self.assertEqual(stats["cluster_name"], "abc")


class PerformRequestTests(IsolatedAsyncioTestCase):

    async def test_get_shape_returns_empty(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        result = await c.perform_request("GET", "/some/path")
        self.assertEqual(result, {})
        m.command.assert_not_called()

    async def test_body_carrying_routes_to_command(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        m = mock.AsyncMock()
        c._client = m
        await c.perform_request("POST", "/", body="SELECT 1")
        m.command.assert_awaited_once_with("SELECT 1")


class PutSettingsTests(IsolatedAsyncioTestCase):

    async def test_no_op(self):
        c = ClickHouseDatabaseClient([{"host": "h", "port": 8123}], {})
        result = await c.cluster.put_settings(body={"a": 1})
        self.assertEqual(result, {"acknowledged": True})
