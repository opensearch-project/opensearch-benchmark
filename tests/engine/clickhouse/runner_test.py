# SPDX-License-Identifier: Apache-2.0
"""Tests for osbenchmark.engine.clickhouse.runners."""
# pylint: disable=protected-access,import-outside-toplevel,no-name-in-module

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
        result = await r(client, {"index": "t",
                                  "body": [{"index": {"_id": "1"}}, {"a": 1}],
                                  "bulk-size": 100, "column-names": ["a"]})
        self.assertTrue(result["success"])
        self.assertEqual(result["weight"], 100)
        self.assertEqual(result["error-count"], 0)
        self.assertEqual(result["took"], 5)

    async def test_error_count_is_one_on_failure(self):
        # ClickHouse INSERT is all-or-nothing: 1 error, not bulk_size.
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        client.bulk.return_value = {"errors": True, "took": 0, "items": []}
        result = await r(client, {"index": "t",
                                  "body": [{"index": {"_id": "1"}}, {"a": 1}],
                                  "bulk-size": 100})
        self.assertFalse(result["success"])
        self.assertEqual(result["error-count"], 1)

    async def test_zero_doc_guard(self):
        # F9: parsed doc count check - 0-doc bulk raises regardless of bulk-size.
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"index": "t", "body": [], "bulk-size": 50})
        client.bulk.assert_not_called()

    async def test_error_returns_dict_without_client_mutation(self):
        # F6: on RPC failure the runner returns an error dict but MUST NOT
        # mutate client._client (race with _ensure_client's lock).
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        client.bulk.side_effect = RuntimeError("boom")
        result = await r(client, {"index": "t",
                                  "body": [{"index": {"_id": "1"}}, {"a": 1}],
                                  "bulk-size": 1})
        self.assertFalse(result["success"])
        self.assertIsNotNone(client._client)

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

    async def test_error_returns_dict_without_client_mutation(self):
        # F6: on RPC failure the runner returns an error dict but MUST NOT
        # mutate client._client (race with _ensure_client's lock).
        r = ch_runners.ClickHouseQuery()
        client = _make_client()
        client.search.side_effect = RuntimeError("boom")
        result = await r(client, {"body": {"sql": "SELECT 1"}})
        self.assertFalse(result["success"])
        self.assertIsNotNone(client._client)

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

    def test_one_shot_admin_ops_not_wrapped_in_retry(self):
        # Retry's exception clauses only catch opensearchpy transport errors,
        # so wrapping is useless for one-shot admin ops that never use
        # retry-until-success. ClusterHealth is intentionally excluded from
        # this set because workloads DO use retry-until-success on it (see
        # test_cluster_health_wrapped_in_retry below).
        registrations = []
        with mock.patch(
            "osbenchmark.worker_coordinator.runner.register_runner",
            side_effect=self._capture(registrations),
        ):
            from osbenchmark.engine import clickhouse
            clickhouse.register_runners()
        from osbenchmark.worker_coordinator.runner import Retry
        from osbenchmark.workload import workload as wl
        one_shot_admin_ops = {
            wl.OperationType.CreateIndex, wl.OperationType.DeleteIndex,
            wl.OperationType.ForceMerge, wl.OperationType.IndexStats,
            wl.OperationType.PutSettings,
        }
        for op, r, _ in registrations:
            if op in one_shot_admin_ops:
                self.assertNotIsInstance(r, Retry,
                                         f"{op} is one-shot admin and MUST NOT be wrapped in Retry")

    def test_cluster_health_wrapped_in_retry(self):
        # ClusterHealth is wrapped in Retry so workloads can set
        # retry-until-success:true to poll for cluster readiness. Retry's
        # return-value-based retry (via .get('success')) works engine-agnostic.
        registrations = []
        with mock.patch(
            "osbenchmark.worker_coordinator.runner.register_runner",
            side_effect=self._capture(registrations),
        ):
            from osbenchmark.engine import clickhouse
            clickhouse.register_runners()
        from osbenchmark.worker_coordinator.runner import Retry
        from osbenchmark.workload import workload as wl
        cluster_health_reg = [(op, r) for op, r, _ in registrations
                              if op == wl.OperationType.ClusterHealth]
        self.assertEqual(len(cluster_health_reg), 1)
        self.assertIsInstance(cluster_health_reg[0][1], Retry,
                              "ClusterHealth MUST be wrapped in Retry so "
                              "retry-until-success works during bootstrap polling.")

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


# -----------------------------------------------------------------
# Regression tests for review findings (P7)
# -----------------------------------------------------------------


class BulkVectorDataSetErrorPathRegressionTests(_RunnerCase):
    """F1: on RPC exception, BulkVectorDataSet must return an error DICT so
    execute_single records success=False. Returning (size, unit) would fabricate
    throughput on ClickHouse crash."""

    async def test_httpx_connect_error_returns_error_dict(self):
        # Import lazily so tests don't fail if httpx isn't installed.
        try:
            import httpx  # pylint: disable=import-outside-toplevel
        except ImportError:  # pragma: no cover - httpx is in the ClickHouse extra
            self.skipTest("httpx not available")
        r = ch_runners.ClickHouseBulkVectorDataSet()
        client = _make_client()
        client.bulk.side_effect = httpx.ConnectError("connection refused")
        body = [{"index": {"_id": "1"}}, {"id": 1, "vec": [0.1, 0.2]}]
        result = await r(client, {"index": "t", "size": 1, "body": body})
        # MUST be a dict (not a tuple) so execute_single sees success=False
        self.assertIsInstance(result, dict)
        self.assertFalse(result["success"])
        self.assertEqual(result["error-count"], 1)
        self.assertEqual(result["error-type"], "clickhouse")
        self.assertIn("connection refused", result["error-description"])
        # And MUST NOT mutate the client (F6-style race prevention)
        self.assertIsNotNone(client._client)

    async def test_success_path_still_returns_tuple(self):
        # Success path is unchanged: still returns (size, "docs").
        r = ch_runners.ClickHouseBulkVectorDataSet()
        client = _make_client()
        body = [{"index": {"_id": "1"}}, {"id": 1, "vec": [0.1, 0.2]}]
        result = await r(client, {"index": "t", "size": 1, "body": body})
        self.assertEqual(result, (1, "docs"))


class ScrollQueryOrderByRegressionTests(_RunnerCase):
    """F8: ORDER BY detection ignores SQL comments and string literals so
    `WHERE label = 'ORDER BYPASS'` without an actual ORDER BY still raises."""

    async def test_order_by_in_string_literal_does_not_pass(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        sql = ("SELECT id FROM t WHERE label = 'ORDER BYPASS' "
               "LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"body": {"sql": sql}})

    async def test_order_by_in_line_comment_does_not_pass(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        sql = ("SELECT id FROM t -- ORDER BY id\n"
               "LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"body": {"sql": sql}})

    async def test_order_by_in_block_comment_does_not_pass(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        sql = ("SELECT id FROM t /* ORDER BY id */ "
               "LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"body": {"sql": sql}})

    async def test_real_order_by_passes(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        client.execute_query.return_value = _query_result([(1,)], ["id"])
        sql = ("SELECT id FROM t WHERE label = 'foo' ORDER BY id "
               "LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
        result = await r(client, {"body": {"sql": sql}, "pages": 1, "results-per-page": 1})
        self.assertTrue(result["success"])

    async def test_lowercase_order_by_passes(self):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        client.execute_query.return_value = _query_result([(1,)], ["id"])
        sql = ("SELECT id FROM t order by id "
               "LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
        result = await r(client, {"body": {"sql": sql}, "pages": 1, "results-per-page": 1})
        self.assertTrue(result["success"])


class VectorEfSearchZeroRegressionTests(_RunnerCase):
    """F14: hnsw_ef_search=0 is a legitimate value (disables candidate expansion)
    and must reach ClickHouse instead of being dropped by a truthiness check."""

    async def test_zero_ef_search_reaches_settings(self):
        r = ch_runners.ClickHouseVectorSearch()
        client = _make_client()
        client.execute_query.return_value = _query_result([], [])
        await r(client, {"body": {"sql": "SELECT 1"}, "hnsw_ef_search": 0, "k": 10})
        kwargs = client.execute_query.await_args.kwargs
        self.assertIn("hnsw_candidate_list_size_for_search", kwargs["settings"])
        self.assertEqual(kwargs["settings"]["hnsw_candidate_list_size_for_search"], 0)


class DropTableCommaSplitRegressionTests(_RunnerCase):
    """F17: comma-separated names in indices must be split so exists() and
    delete() are called per-name, not with the literal 'a,b'."""

    async def test_comma_separated_names_split(self):
        r = ch_runners.ClickHouseDropTable()
        client = _make_client()
        client.indices.exists = mock.AsyncMock(return_value=True)
        result = await r(client, {"indices": ["a,b"], "only-if-exists": True})
        # exists() called twice (once per split name), delete() called twice
        self.assertEqual(client.indices.exists.await_count, 2)
        self.assertEqual(client.indices.delete.await_count, 2)
        self.assertEqual(result["weight"], 2)
        # Verify the split names were passed cleanly (stripped of whitespace)
        called_with = [c.kwargs.get("index") for c in client.indices.delete.await_args_list]
        self.assertEqual(sorted(called_with), ["a", "b"])

    async def test_comma_with_whitespace_handled(self):
        r = ch_runners.ClickHouseDropTable()
        client = _make_client()
        await r(client, {"indices": ["a , b , c"]})
        self.assertEqual(client.indices.delete.await_count, 3)
        # Whitespace must be stripped from each split name.
        called_with = [c.kwargs.get("index") for c in client.indices.delete.await_args_list]
        self.assertEqual(sorted(called_with), ["a", "b", "c"])


class BulkIndexZeroDocRegressionTests(_RunnerCase):
    """F9: zero-doc guard operates on parsed doc count, not the bulk-size param.
    Missing bulk-size (defaults to 0) previously bypassed the guard entirely."""

    async def test_missing_bulk_size_with_empty_body_still_raises(self):
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        # No bulk-size param at all - previously bypassed the guard.
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"index": "t", "body": []})
        client.bulk.assert_not_called()

    async def test_missing_bulk_size_with_bytes_body_of_zero_docs_raises(self):
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        with self.assertRaises(exceptions.BenchmarkError):
            await r(client, {"index": "t", "body": b""})
        client.bulk.assert_not_called()


# ============================================================================
# P8 regression tests (second-pass review findings)
# ============================================================================

class ParseErrorSampleFailureRegressionTests(_RunnerCase):
    """P8: parse_bulk_body raises BenchmarkError on data corruption. That MUST
    be caught in ClickHouseBulkIndex and reported as a per-sample failure, not
    propagated as an actor crash that aborts the whole benchmark."""

    async def test_malformed_ndjson_returns_error_dict_not_raises(self):
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        # NDJSON with 1 malformed line - parse_bulk_body raises BenchmarkError
        malformed = b'{"index":{"_id":"1"}}\nnot-json-at-all\n'
        result = await r(client, {"index": "t", "body": malformed, "bulk-size": 1})
        self.assertEqual(result["success"], False)
        self.assertEqual(result["error-count"], 1)
        self.assertIn("error-description", result)
        client.bulk.assert_not_called()


class DoubleParseRegressionTests(_RunnerCase):
    """P8: parse_bulk_body was called TWICE per bulk (once for zero-doc guard,
    once inside client.bulk). Fixed by threading parsed_docs kwarg through."""

    async def test_bulk_index_threads_parsed_docs_to_client(self):
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        client.bulk = mock.AsyncMock(return_value={"took": 5, "errors": False})
        body = b'{"index":{"_id":"1"}}\n{"a":1}\n'
        await r(client, {"index": "t", "body": body, "bulk-size": 1})
        # client.bulk should have received parsed_docs kwarg (avoiding re-parse)
        client.bulk.assert_awaited_once()
        call = client.bulk.await_args
        self.assertIn("parsed_docs", call.kwargs)
        self.assertEqual(len(call.kwargs["parsed_docs"]), 1)

    async def test_parse_bulk_body_invoked_exactly_once(self):
        # The complementary end-to-end assertion: patch parse_bulk_body itself
        # so we can COUNT invocations. The runner MUST parse once and the
        # client MUST NOT reparse when parsed_docs is passed. Without this
        # test, dropping parsed_docs from the client.bulk() signature would
        # silently double the parse cost on every bulk op with no test failure.
        from osbenchmark.engine.clickhouse import helpers as ch_helpers
        original = ch_helpers.parse_bulk_body
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        client.bulk = mock.AsyncMock(return_value={"took": 5, "errors": False})
        body = b'{"index":{"_id":"1"}}\n{"a":1}\n'
        with mock.patch(
            "osbenchmark.engine.clickhouse.helpers.parse_bulk_body",
            wraps=original,
        ) as spy:
            await r(client, {"index": "t", "body": body, "bulk-size": 1})
        self.assertEqual(spy.call_count, 1,
                         f"parse_bulk_body must be called exactly once per "
                         f"bulk op; got {spy.call_count}")


class OrderByRegexRegressionTests(_RunnerCase):
    """P8: F8's ORDER BY regex was too strict - `\\bORDER\\s+BY\\b` failed to
    match 'ORDER BYPASS' (which is what triggered the false-positive originally
    since BY was followed by P not \\W). The fix uses lookahead for whitespace
    or paren after BY, so 'ORDER BYPASS_COL' is correctly rejected."""

    async def _run_scroll(self, sql):
        r = ch_runners.ClickHouseScrollQuery()
        client = _make_client()
        client.execute_query = mock.AsyncMock(
            return_value=mock.MagicMock(result_rows=[]))
        body = {"sql": sql, "parameters": {}}
        return await r(client, {"body": body, "pages": 1, "results-per-page": 10})

    async def test_valid_order_by_accepted(self):
        result = await self._run_scroll(
            "SELECT id FROM t ORDER BY id LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
        self.assertTrue(result["success"])

    async def test_order_by_in_string_literal_rejected(self):
        # 'ORDER BYPASS' inside a string literal, no real ORDER BY clause
        with self.assertRaises(exceptions.BenchmarkError) as ctx:
            await self._run_scroll(
                "SELECT id FROM t WHERE label = 'ORDER BYPASS' "
                "LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
        self.assertIn("ORDER BY", str(ctx.exception))

    async def test_order_by_identifier_variant_rejected(self):
        # 'ORDER BYPASS_COL' - a space between ORDER and BYPASS is what the
        # new regex must reject; using an underscore ('ORDER_BYPASS_COL') would
        # have been rejected by the OLD regex too and wouldn't exercise the fix.
        with self.assertRaises(exceptions.BenchmarkError):
            await self._run_scroll(
                "SELECT id FROM t ORDER BYPASS_COL LIMIT {limit:UInt32} OFFSET {offset:UInt32}")

    async def test_double_dash_inside_string_literal_preserved(self):
        # 'foo-- bar' inside a string literal must NOT be stripped as a line
        # comment. The whitespace AFTER `--` is required for the SQL comment
        # regex to match, so this literal would (incorrectly) look like a
        # comment if the string-literal strip did not run first. Do NOT use
        # 'foo--bar' here: without a following whitespace char the regex would
        # never match, so the test would silently pass under both orderings.
        result = await self._run_scroll(
            "SELECT id FROM t WHERE label = 'foo-- bar' "
            "ORDER BY id LIMIT {limit:UInt32} OFFSET {offset:UInt32}")
        self.assertTrue(result["success"])


class BulkIndexUnexpectedParseErrorTests(_RunnerCase):
    """P8: broad `except Exception` on parse-body path used to swallow
    programmer errors. Now: BenchmarkError = sample failure, others re-raise."""

    async def test_unexpected_exception_reraised(self):
        r = ch_runners.ClickHouseBulkIndex()
        client = _make_client()
        # Force parse_bulk_body to raise something other than BenchmarkError
        with mock.patch("osbenchmark.engine.clickhouse.helpers.parse_bulk_body",
                        side_effect=RuntimeError("dev-time bug")):
            with self.assertRaises(RuntimeError):
                await r(client, {"index": "t", "body": b"x", "bulk-size": 1})
