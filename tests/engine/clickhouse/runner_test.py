# SPDX-License-Identifier: Apache-2.0
"""Tests for osbenchmark.engine.clickhouse.runners."""

from unittest import TestCase, IsolatedAsyncioTestCase, mock

from osbenchmark import exceptions
from osbenchmark.engine.clickhouse import runners as ch_runners


class _RunnerCase(IsolatedAsyncioTestCase):
    """Base for runner tests. Patches request_context_holder so the ContextVar
    lookups inside on_*_request_* don't LookupError in an isolated test."""

    def setUp(self):
        self._ctx_patcher = mock.patch(
            "osbenchmark.engine.clickhouse.runners.request_context_holder"
        )
        self.mock_ctx = self._ctx_patcher.start()
        self.addCleanup(self._ctx_patcher.stop)


# -----------------------------------------------------------------
# Shared fixtures (inline, no conftest.py)
# -----------------------------------------------------------------

def _make_client(**overrides):
    """Build a mock ClickHouse client for runner tests."""
    client = mock.AsyncMock()
    # duck-type the namespace proxies
    client.indices = client
    client.cluster = client
    client._client = mock.MagicMock()  # for the _client = None reset assertions
    # execute_query returns an AsyncMock so it can be awaited
    client.execute_query = mock.AsyncMock()
    for k, v in overrides.items():
        setattr(client, k, v)
    return client


def _query_result(rows=None, columns=None, elapsed_ns="1000000"):
    """Note: elapsed_ns is a STRING to match ClickHouse's real behavior."""
    r = mock.MagicMock()
    r.result_rows = rows or []
    r.column_names = tuple(columns or [])
    r.summary = {"elapsed_ns": elapsed_ns}
    return r


class ClickHouseBulkIndexTests(_RunnerCase):

    async def test_success(self):
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        client.bulk.return_value = {"errors": False, "took": 5, "items": [{}, {}]}
        result = await r(client, {"index": "t", "body": [{"a": 1}], "bulk-size": 100,
                                  "column-names": ["a"]})
        self.assertTrue(result["success"])
        self.assertEqual(result["weight"], 100)
        self.assertEqual(result["error-count"], 0)
        self.assertEqual(result["took"], 5)

    async def test_error_count_is_one_on_failure(self):
        # ClickHouse INSERT is all-or-nothing: 1 error, not bulk_size.
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        client.bulk.return_value = {"errors": True, "took": 0, "items": []}
        result = await r(client, {"index": "t", "body": [{"a": 1}], "bulk-size": 100})
        self.assertFalse(result["success"])
        self.assertEqual(result["error-count"], 1)

    async def test_zero_doc_guard(self):
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        result = await r(client, {"index": "t", "body": [], "bulk-size": 50})
        self.assertFalse(result["success"])
        self.assertEqual(result["error-count"], 1)
        client.bulk.assert_not_called()

    async def test_client_reset_on_exception(self):
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        client.bulk.side_effect = RuntimeError("boom")
        result = await r(client, {"index": "t", "body": [{"a": 1}], "bulk-size": 1})
        self.assertFalse(result["success"])
        self.assertIsNone(client._client)

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseBulkIndex()), "clickhouse-bulk-index")


class ClickHouseQueryTests(_RunnerCase):

    async def test_success(self):
        r = ch_runners.ClickHouseQuery()
        client = _make_client()
        client.search.return_value = {
            "took": 7, "timed_out": False,
            "hits": {"total": {"value": 3, "relation": "eq"}, "hits": []},
        }
        result = await r(client, {"body": {"sql": "SELECT 1"}})
        self.assertTrue(result["success"])
        self.assertEqual(result["hits"], 3)
        self.assertEqual(result["took"], 7)

    async def test_missing_sql_raises(self):
        r = ch_runners.ClickHouseQuery()
        client = _make_client()
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"body": {}})

    async def test_client_reset_on_exception(self):
        r = ch_runners.ClickHouseQuery()
        client = _make_client()
        client.search.side_effect = RuntimeError("boom")
        result = await r(client, {"body": {"sql": "SELECT 1"}})
        self.assertFalse(result["success"])
        self.assertIsNone(client._client)

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseQuery()), "clickhouse-query")


class ClickHouseVectorSearchTests(_RunnerCase):

    async def test_success_via_execute_query(self):
        r = ch_runners.ClickHouseVectorSearch()
        client = _make_client()
        client.execute_query.return_value = _query_result(
            [(1, 0.9), (2, 0.7)], ["id", "score"]
        )
        result = await r(client, {"body": {"sql": "SELECT id, score FROM t"}})
        self.assertTrue(result["success"])
        self.assertEqual(result["hits"], 2)
        client.execute_query.assert_awaited_once()

    async def test_numpy_parameters_coerced(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("numpy not available")
        r = ch_runners.ClickHouseVectorSearch()
        client = _make_client()
        client.execute_query.return_value = _query_result([], [])
        await r(client, {"body": {"sql": "SELECT 1", "parameters": {"v": np.array([1.0])}}})
        kwargs = client.execute_query.await_args.kwargs
        self.assertEqual(kwargs["parameters"]["v"], [1.0])

    async def test_ef_search_sets_setting(self):
        r = ch_runners.ClickHouseVectorSearch()
        client = _make_client()
        client.execute_query.return_value = _query_result([], [])
        await r(client, {"body": {"sql": "SELECT 1"}, "hnsw_ef_search": 200, "k": 10})
        kwargs = client.execute_query.await_args.kwargs
        self.assertEqual(kwargs["settings"]["hnsw_candidate_list_size_for_search"], 200)

    async def test_recall_int_normalization(self):
        r = ch_runners.ClickHouseVectorSearch()
        client = _make_client()
        # Returned rows have ID '1' as int; truth has '00001' as zero-padded string.
        # Integer normalization should match them.
        client.execute_query.return_value = _query_result([(1, 0.9)], ["id", "score"])
        result = await r(client, {
            "body": {"sql": "SELECT 1"},
            "k": 1,
            "calculate-recall": True,
            "neighbors": ["00001"],
        })
        self.assertEqual(result["recall@k"], 1.0)
        self.assertEqual(result["recall@1"], 1.0)

    async def test_missing_sql_raises(self):
        r = ch_runners.ClickHouseVectorSearch()
        client = _make_client()
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"body": {}})

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseVectorSearch()), "clickhouse-vector-search")


class ClickHouseScrollQueryTests(_RunnerCase):

    async def test_requires_placeholders(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"body": {"sql": "SELECT id FROM t ORDER BY id"}})

    async def test_requires_order_by(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"body": {"sql": "SELECT id FROM t LIMIT {limit:UInt32} OFFSET {offset:UInt32}"}})

    async def test_pages_actual_matches_loop_count(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        client.execute_query.return_value = _query_result([(1,)] * 5, ["id"])
        result = await r(client, {
            "body": {"sql": "SELECT id FROM t ORDER BY id "
                            "LIMIT {limit:UInt32} OFFSET {offset:UInt32}"},
            "pages": 3, "results-per-page": 5,
        })
        self.assertEqual(result["pages_actual"], 3)
        self.assertEqual(result["hits"], 15)

    async def test_early_exit_on_partial_page(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        # First call returns full 5 rows, second returns 2 -> exit loop after 2 pages.
        client.execute_query.side_effect = [
            _query_result([(1,)] * 5, ["id"]),
            _query_result([(2,)] * 2, ["id"]),
        ]
        result = await r(client, {
            "body": {"sql": "SELECT id FROM t ORDER BY id "
                            "LIMIT {limit:UInt32} OFFSET {offset:UInt32}"},
            "pages": 10, "results-per-page": 5,
        })
        self.assertEqual(result["pages_actual"], 2)

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseScrollQuery()), "clickhouse-scroll-query")


class ClickHouseBulkVectorDataSetTests(_RunnerCase):

    async def test_alternating_pairs(self):
        r = ch_runners.ClickHouseBulkVectorDataSet()
        client = _make_client()
        body = [{"index": {"_id": "1"}}, {"id": 1, "vec": [0.1, 0.2]}]
        size, unit = await r(client, {"index": "t", "size": 1, "body": body})
        self.assertEqual(size, 1)
        self.assertEqual(unit, "docs")
        client.bulk.assert_awaited_once()

    async def test_numpy_vector_conversion(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("numpy not available")
        r = ch_runners.ClickHouseBulkVectorDataSet()
        client = _make_client()
        body = [{"index": {"_id": "1"}},
                {"id": 1, "vec": np.array([0.1, 0.2], dtype=np.float32)}]
        await r(client, {"index": "t", "size": 1, "body": body})
        called_body = client.bulk.await_args.kwargs["body"]
        # Vector should be a list, not an ndarray
        self.assertIsInstance(called_body[1]["vec"], list)

    async def test_returns_tuple(self):
        r = ch_runners.ClickHouseBulkVectorDataSet()
        client = _make_client()
        result = await r(client, {"index": "t", "size": 42, "body": []})
        self.assertEqual(result, (42, "docs"))


class ClickHouseCreateTableTests(_RunnerCase):

    async def test_single_index(self):
        r = ch_runners.ClickHouseCreateTable()
        client = _make_client()
        result = await r(client, {"index": "t", "body": {"ddl": "CREATE TABLE t (a UInt64) ENGINE=MergeTree ORDER BY a"}})
        self.assertTrue(result["success"])
        self.assertEqual(result["weight"], 1)
        client.indices.create.assert_awaited_once()

    async def test_multi_index(self):
        r = ch_runners.ClickHouseCreateTable()
        client = _make_client()
        result = await r(client, {"indices": [("t1", {"ddl": "..."}), ("t2", {"ddl": "..."})]})
        self.assertEqual(result["weight"], 2)
        self.assertEqual(client.indices.create.await_count, 2)

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseCreateTable()), "clickhouse-create-table")


class ClickHouseDropTableTests(_RunnerCase):

    async def test_delete(self):
        r = ch_runners.ClickHouseDropTable()
        client = _make_client()
        result = await r(client, {"index": "t"})
        self.assertEqual(result["weight"], 1)
        client.indices.delete.assert_awaited_once_with(index="t")

    async def test_only_if_exists(self):
        r = ch_runners.ClickHouseDropTable()
        client = _make_client()
        client.indices.exists = mock.AsyncMock(return_value=False)
        result = await r(client, {"index": "t", "only-if-exists": True})
        self.assertEqual(result["weight"], 0)
        client.indices.delete.assert_not_called()

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseDropTable()), "clickhouse-drop-table")


class ClickHouseSystemPartsTests(_RunnerCase):

    async def test_returns_os_shape(self):
        r = ch_runners.ClickHouseSystemParts()
        client = _make_client()
        stats_payload = {
            "_all": {"primaries": {"docs": {"count": 5}}, "total": {"docs": {"count": 5}}},
            "indices": {},
        }
        client.indices.stats = mock.AsyncMock(return_value=stats_payload)
        result = await r(client, {"index": "t"})
        self.assertEqual(result["stats"], stats_payload)
        self.assertEqual(result["index"], "t")
        self.assertEqual(result["primaries"], {"docs": {"count": 5}})

    async def test_all_default_index(self):
        r = ch_runners.ClickHouseSystemParts()
        client = _make_client()
        client.indices.stats = mock.AsyncMock(return_value={"_all": {"primaries": {}, "total": {}}})
        result = await r(client, {})
        self.assertEqual(result["index"], "_all")

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseSystemParts()), "clickhouse-system-parts")


class ClickHouseClusterHealthTests(_RunnerCase):

    async def test_green(self):
        r = ch_runners.ClickHouseClusterHealth()
        client = _make_client()
        client.cluster.health = mock.AsyncMock(return_value={"status": "green", "relocating_shards": 0})
        result = await r(client, {})
        self.assertTrue(result["success"])
        self.assertEqual(result["cluster-status"], "green")

    async def test_red_not_success(self):
        r = ch_runners.ClickHouseClusterHealth()
        client = _make_client()
        client.cluster.health = mock.AsyncMock(return_value={"status": "red"})
        result = await r(client, {})
        self.assertFalse(result["success"])

    async def test_relocating_passthrough(self):
        r = ch_runners.ClickHouseClusterHealth()
        client = _make_client()
        client.cluster.health = mock.AsyncMock(
            return_value={"status": "yellow", "relocating_shards": 7}
        )
        result = await r(client, {})
        self.assertEqual(result["relocating-shards"], 7)

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseClusterHealth()), "clickhouse-cluster-health")


class ClickHouseRefreshNoOpTests(_RunnerCase):
    """Refresh operation type maps to ClickHouseNoOp('refresh')."""

    async def test_success(self):
        r = ch_runners.ClickHouseNoOp("refresh")
        client = _make_client()
        result = await r(client, {})
        self.assertTrue(result["success"])

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseNoOp("refresh")),
                         "clickhouse-noop(refresh)")


class ClickHouseOptimizeTableTests(_RunnerCase):

    async def test_success(self):
        r = ch_runners.ClickHouseOptimizeTable()
        client = _make_client()
        client.indices.forcemerge = mock.AsyncMock(
            return_value={"_shards": {"total": 1, "successful": 1, "failed": 0}}
        )
        result = await r(client, {"index": "t"})
        self.assertTrue(result["success"])
        self.assertEqual(result["shards"]["successful"], 1)

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseOptimizeTable()),
                         "clickhouse-optimize-table")


class ClickHouseNoOpTests(_RunnerCase):

    async def test_logs_skip(self):
        r = ch_runners.ClickHouseNoOp("put-pipeline")
        client = _make_client()
        with self.assertLogs("osbenchmark.worker_coordinator.runner", level="DEBUG") as cm:
            await r(client, {})
        self.assertTrue(any("put-pipeline" in msg for msg in cm.output))

    async def test_returns_success(self):
        r = ch_runners.ClickHouseNoOp("something")
        client = _make_client()
        result = await r(client, {})
        self.assertTrue(result["success"])
        self.assertEqual(result["unit"], "ops")

    def test_repr(self):
        self.assertEqual(repr(ch_runners.ClickHouseNoOp("xyz")), "clickhouse-noop(xyz)")


class RegisterRunnersTests(TestCase):
    """Tests against the engine module's register_runners() function."""

    def setUp(self):
        # Snapshot the runner registry before each test to isolate changes.
        from osbenchmark.worker_coordinator import runner as osb_runner
        # pylint: disable=protected-access
        self._orig = dict(getattr(osb_runner, "_Runner__RUNNERS", {})
                          if hasattr(osb_runner, "_Runner__RUNNERS")
                          else osb_runner.__dict__.get("_Runner__RUNNERS", {}))

    def _capture(self, registrations):
        """Return a mock that records (op_type, runner, kwargs) tuples."""
        def _side_effect(op_type, runner, **kwargs):
            registrations.append((op_type, runner, kwargs))
        return _side_effect

    def test_all_registrations_are_async(self):
        registrations = []
        with mock.patch(
            "osbenchmark.worker_coordinator.runner.register_runner",
            side_effect=self._capture(registrations),
        ):
            from osbenchmark.engine import clickhouse
            clickhouse.register_runners()
        self.assertTrue(registrations)
        for _op, _r, kwargs in registrations:
            self.assertTrue(kwargs.get("async_runner"))

    def test_admin_ops_wrapped_in_retry(self):
        registrations = []
        with mock.patch(
            "osbenchmark.worker_coordinator.runner.register_runner",
            side_effect=self._capture(registrations),
        ):
            from osbenchmark.engine import clickhouse
            clickhouse.register_runners()
        from osbenchmark.worker_coordinator.runner import Retry
        from osbenchmark.workload import workload as wl
        admin_ops = {
            wl.OperationType.CreateIndex, wl.OperationType.DeleteIndex,
            wl.OperationType.ForceMerge, wl.OperationType.ClusterHealth,
            wl.OperationType.IndexStats, wl.OperationType.PutSettings,
        }
        for op, r, _ in registrations:
            if op in admin_ops:
                self.assertIsInstance(r, Retry, f"{op} should be wrapped in Retry")

    def test_data_plane_ops_not_wrapped(self):
        registrations = []
        with mock.patch(
            "osbenchmark.worker_coordinator.runner.register_runner",
            side_effect=self._capture(registrations),
        ):
            from osbenchmark.engine import clickhouse
            clickhouse.register_runners()
        from osbenchmark.worker_coordinator.runner import Retry
        from osbenchmark.workload import workload as wl
        data_plane_ops = {
            wl.OperationType.Bulk, wl.OperationType.Search,
            wl.OperationType.PaginatedSearch, wl.OperationType.ScrollSearch,
            wl.OperationType.VectorSearch, wl.OperationType.BulkVectorDataSet,
        }
        for op, r, _ in registrations:
            if op in data_plane_ops:
                self.assertNotIsInstance(r, Retry, f"{op} should NOT be wrapped in Retry")

    def test_warmup_knn_indices_registered_as_string(self):
        registrations = []
        with mock.patch(
            "osbenchmark.worker_coordinator.runner.register_runner",
            side_effect=self._capture(registrations),
        ):
            from osbenchmark.engine import clickhouse
            clickhouse.register_runners()
        keys = [op for op, _, _ in registrations]
        self.assertIn("warmup-knn-indices", keys)

    def test_registration_count(self):
        registrations = []
        with mock.patch(
            "osbenchmark.worker_coordinator.runner.register_runner",
            side_effect=self._capture(registrations),
        ):
            from osbenchmark.engine import clickhouse
            clickhouse.register_runners()
        # 6 data-plane + 6 admin + 3 CreateSearchPipeline/PutPipeline/DeletePipeline
        # + 1 PutSettings + 1 warmup-knn-indices = 17
        self.assertEqual(len(registrations), 17)


class ClickHouseRunnerSqlContractTests(_RunnerCase):
    """Contract: SQL-requiring runners raise BenchmarkError without a sql key."""

    async def test_query_requires_sql(self):
        r = ch_runners.ClickHouseQuery()
        with self.assertRaises(exceptions.BenchmarkError):
            await r(_make_client(), {"body": {}})

    async def test_vector_search_requires_sql(self):
        r = ch_runners.ClickHouseVectorSearch()
        with self.assertRaises(exceptions.BenchmarkError):
            await r(_make_client(), {"body": {}})

    async def test_four_timing_methods_driven(self):
        """Each real runner drives on_client_request_start/end + on_request_start/end."""
        r = ch_runners.ClickHouseQuery()
        client = _make_client()
        client.search.return_value = {"took": 0, "timed_out": False,
                                      "hits": {"total": {"value": 0}, "hits": []}}
        with mock.patch("osbenchmark.engine.clickhouse.runners.request_context_holder") as ctx:
            await r(client, {"body": {"sql": "SELECT 1"}})
            ctx.on_client_request_start.assert_called_once()
            ctx.on_request_start.assert_called_once()
            ctx.on_request_end.assert_called_once()
            ctx.on_client_request_end.assert_called_once()
