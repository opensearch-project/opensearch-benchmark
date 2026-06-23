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
Unit tests for the CloudWatch datastore read path: the Insights helper,
MetricsStore read methods, and CloudWatchTestRunStore listing/lookup
plus the FileBackedCompositeTestRunStore fallback semantics.
"""
import datetime
import json

import botocore.exceptions
import pytest

from osbenchmark import exceptions
from osbenchmark.metrics import SampleType
from osbenchmark.metrics_stores.cloudwatch import insights
from osbenchmark.metrics_stores.cloudwatch.insights import (
    InsightsQueryError,
    _flatten_rows,
    run_query,
    to_float,
)
from osbenchmark.metrics_stores.cloudwatch.metrics_store import (
    CloudWatchMetricsStore,
)
from osbenchmark.metrics_stores.cloudwatch.test_run_store import (
    CloudWatchTestRunStore,
    FileBackedCompositeTestRunStore,
)

from .conftest import make_insights_rows


# ----------------------------------------------------------------- Insights helper


class TestInsightsHelper:
    def test_happy_path_flattens_rows(self, fake_logs_client):
        rows = make_insights_rows([
            {"task": "term", "p99": "12.3"},
            {"task": "phrase", "p99": "20.1"},
        ])
        # _flatten_rows tested separately; here run_query end-to-end
        fake_logs_client.queue_query_results(rows)
        result = run_query(fake_logs_client, "lg", "stats pct(x, 99)",
                           start_time=1000, end_time=2000, limit=50)
        assert result == [{"task": "term", "p99": "12.3"},
                          {"task": "phrase", "p99": "20.1"}]
        # Time params and limit propagate
        sq = fake_logs_client.start_query_calls[0]
        assert sq["startTime"] == 1000
        assert sq["endTime"] == 2000
        assert sq["limit"] == 50

    def test_polls_until_complete(self, fake_logs_client):
        rows = make_insights_rows([{"x": "1"}])
        fake_logs_client.queue_query_results(
            rows, status_sequence=["Running", "Scheduled", "Complete"])
        result = run_query(fake_logs_client, "lg", "q", 1, 2)
        assert result == [{"x": "1"}]

    def test_unknown_status_is_transient(self, fake_logs_client):
        # Unknown is documented as "we don't know yet" — must not be
        # treated as terminal. Commit-10 regression.
        rows = make_insights_rows([{"x": "1"}])
        fake_logs_client.queue_query_results(
            rows, status_sequence=["Unknown", "Unknown", "Complete"])
        result = run_query(fake_logs_client, "lg", "q", 1, 2)
        assert result == [{"x": "1"}]

    @pytest.mark.parametrize("status", ["Failed", "Cancelled", "Timeout"])
    def test_terminal_failure_status_raises(self, status, fake_logs_client):
        fake_logs_client.queue_query_results([], status_sequence=[status])
        with pytest.raises(InsightsQueryError, match=status):
            run_query(fake_logs_client, "lg", "q", 1, 2)

    def test_start_query_error_wrapped(self, fake_logs_client):
        # Replace start_query with a raising version
        def boom(**kw):
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "no"}}, "StartQuery")
        fake_logs_client.start_query = boom
        with pytest.raises(InsightsQueryError, match="start_query failed"):
            run_query(fake_logs_client, "lg", "q", 1, 2)

    def test_get_query_results_throttle_retried(self, fake_logs_client):
        # First two get_query_results calls throttle, third succeeds
        calls = {"n": 0}
        rows = make_insights_rows([{"x": "1"}])
        original = fake_logs_client.get_query_results
        def maybe_throttle(queryId):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "ThrottlingException"}}, "GetQueryResults")
            return original(queryId)
        fake_logs_client.queue_query_results(rows)
        fake_logs_client.get_query_results = maybe_throttle
        result = run_query(fake_logs_client, "lg", "q", 1, 2)
        assert result == [{"x": "1"}]

    def test_float_epoch_coerced_to_int(self, fake_logs_client):
        fake_logs_client.queue_query_results([])
        run_query(fake_logs_client, "lg", "q", 1.7, 2.3)
        sq = fake_logs_client.start_query_calls[0]
        assert sq["startTime"] == 1
        assert sq["endTime"] == 2

    def test_insights_query_error_is_benchmark_error(self):
        e = InsightsQueryError("test")
        assert isinstance(e, exceptions.BenchmarkError)


class TestFlattenRows:
    def test_drops_ptr_field(self):
        rows = [[
            {"field": "task", "value": "term"},
            {"field": "@ptr", "value": "abc"},
        ]]
        assert _flatten_rows(rows) == [{"task": "term"}]

    def test_skips_missing_field(self):
        rows = [[
            {"field": "x", "value": "1"},
            {"value": "orphan"},
        ]]
        assert _flatten_rows(rows) == [{"x": "1"}]


class TestToFloat:
    @pytest.mark.parametrize("inp,expected", [
        ("12.3", 12.3),
        ("0", 0.0),
        (None, None),
        ("", None),
        ("abc", None),
    ])
    def test_coercion(self, inp, expected):
        assert to_float(inp) == expected


# ----------------------------------------------------- CloudWatchMetricsStore reads


class _MetricsCfg:
    def __init__(self):
        self._o = {("system", "env.name"): "default",
                   ("workload", "params"): {},
                   ("test_run", "user.tag"): ""}
    def opts(self, section, key, default_value=None, mandatory=True):
        return self._o.get((section, key), default_value)


def _make_factory(fake_client):
    class _F:
        def __init__(self, cw_cfg):
            self._client = fake_client
        def probe_caller_identity(self): pass
        def logs_client(self): return self._client
    return _F


@pytest.fixture
def open_store(fake_logs_client, cw_config):
    """Helper: returns a function that opens a CloudWatchMetricsStore for writes."""
    def _open():
        store = CloudWatchMetricsStore(
            cfg=_MetricsCfg(),
            client_factory_class=_make_factory(fake_logs_client),
            config_loader=lambda cfg: cw_config,
        )
        store.open(
            test_run_id="abc-123",
            test_run_timestamp=datetime.datetime(2026, 6, 22, 12, 0, 0),
            workload_name="big5",
            test_procedure_name="p",
            cluster_config_name="c",
            create=True,
        )
        return store
    return _open


class TestMetricsStoreReads:
    def test_get_stats_happy_path(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(make_insights_rows([
            {"min": "1.0", "max": "10.0", "avg": "5.5",
             "sum": "55.0", "count": "10"}
        ]))
        store = open_store()
        stats = store.get_stats("service_time", task="term")
        assert stats == {"count": 10, "min": 1.0, "max": 10.0,
                         "avg": 5.5, "sum": 55.0}
        q = fake_logs_client.start_query_calls[-1]["queryString"]
        assert 'Task = "term"' in q
        assert 'service_time' in q

    def test_get_stats_empty(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results([])
        store = open_store()
        stats = store.get_stats("x")
        assert stats == {"count": 0, "min": None, "max": None,
                         "avg": None, "sum": None}

    def test_get_percentiles_uses_max_for_p100(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(make_insights_rows([
            {"p_99": "12.3", "p_99_9": "15.7", "p_100": "20.0",
             "count": "1000"}
        ]))
        store = open_store()
        result = store.get_percentiles("service_time")
        assert list(result.keys()) == ["99", "99.9", "100"]
        # Critical: Insights rejects pct(field, 100), must use max()
        q = fake_logs_client.start_query_calls[-1]["queryString"]
        assert "pct(`service_time`, 99)" in q
        assert "max(`service_time`) as `p_100`" in q
        assert "pct(`service_time`, 100)" not in q

    def test_get_percentiles_no_hits_returns_none(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(
            make_insights_rows([{"count": "0"}]))
        store = open_store()
        assert store.get_percentiles("x") is None

    def test_get_percentiles_custom_values(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(make_insights_rows([
            {"p_50": "1", "p_95": "2", "count": "100"}
        ]))
        store = open_store()
        result = store.get_percentiles("x", percentiles=[50, 95])
        assert list(result.keys()) == ["50", "95"]

    def test_get_one_returns_mapped_value(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(make_insights_rows([
            {"service_time": "42.5", "@timestamp": "1234"}
        ]))
        store = open_store()
        val = store.get_one("service_time", task="term")
        assert val == 42.5

    def test_get_one_relative_time_ms_coerced_to_float(self, fake_logs_client, open_store):
        # GlobalStatsCalculator.duration reads doc["relative-time-ms"].
        # Insights returns it as a string — we must coerce to float.
        # Commit-11 regression.
        fake_logs_client.queue_query_results(make_insights_rows([
            {"service_time": "12.3", "Unit": "ms", "RelativeTimeMs": "1234.5",
             "@timestamp": "1000"}
        ]))
        store = open_store()
        val = store.get_one(
            "service_time",
            mapper=lambda doc: doc["relative-time-ms"],
            sort_key="relative-time-ms",
            sort_reverse=True,
        )
        assert val == 1234.5
        assert isinstance(val, float)

    def test_get_unit(self, fake_logs_client, open_store):
        # MetricsStore.get_unit calls _get with mapper=lambda doc: doc["unit"].
        # Commit-11 regression: store must expose Unit in the doc shape.
        fake_logs_client.queue_query_results(make_insights_rows([
            {"service_time": "12.3", "Unit": "ms", "Task": "term",
             "OperationType": "search", "SampleType": "normal"}
        ]))
        store = open_store()
        unit = store.get_unit("service_time", task="term")
        assert unit == "ms"

    def test_get_one_empty(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results([])
        store = open_store()
        assert store.get_one("service_time") is None

    def test_get_error_rate(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(make_insights_rows([
            {"meta.success": "true", "samples": "95"},
            {"meta.success": "false", "samples": "5"},
        ]))
        store = open_store()
        assert abs(store.get_error_rate("term") - 0.05) < 1e-9

    def test_get_error_rate_all_errors(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(make_insights_rows([
            {"meta.success": "false", "samples": "10"}
        ]))
        store = open_store()
        assert store.get_error_rate("term") == 1.0

    def test_get_error_rate_all_success(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(make_insights_rows([
            {"meta.success": "true", "samples": "10"}
        ]))
        store = open_store()
        assert store.get_error_rate("term") == 0.0

    def test_get_returns_raw_values(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results(make_insights_rows([
            {"service_time": "12.3", "Task": "term",
             "OperationType": "search", "SampleType": "normal",
             "meta.success": "true"},
            {"service_time": "15.6", "Task": "term",
             "OperationType": "search", "SampleType": "normal",
             "meta.success": "true"},
        ]))
        store = open_store()
        values = store.get("service_time", task="term")
        assert values == [12.3, 15.6]

    def test_all_queries_filter_by_test_run_id(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results([])
        store = open_store()
        store.get_stats("x")
        q = fake_logs_client.start_query_calls[-1]["queryString"]
        assert 'TestRunId = "abc-123"' in q

    def test_sample_type_enum_lowercased(self, fake_logs_client, open_store):
        fake_logs_client.queue_query_results([])
        store = open_store()
        store.get_stats("x", sample_type=SampleType.Normal)
        q = fake_logs_client.start_query_calls[-1]["queryString"]
        assert 'SampleType = "normal"' in q

    def test_utc_timezone_window(self, fake_logs_client, open_store):
        # Commit-11 regression: time.from_is8601 returns naive datetimes;
        # we must attach UTC explicitly or the Insights window is off by
        # the local timezone offset (potentially hours).
        fake_logs_client.queue_query_results([])
        store = open_store()
        store.get_stats("x")
        sq = fake_logs_client.start_query_calls[-1]
        expected = int(datetime.datetime(
            2026, 6, 22, 12, 0, 0, tzinfo=datetime.timezone.utc
        ).timestamp())
        # Window is [expected-60, now+60]; allow ±5s slop for clock skew
        assert abs(sq["startTime"] - (expected - 60)) < 5

    def test_insights_error_degrades_to_empty(self, fake_logs_client, open_store):
        # Real-world E2E: IAM role has logs:PutLogEvents but not
        # logs:StartQuery. Reads should fail-soft to empty results so
        # the result-summary path doesn't crash the run. (Same fail-soft
        # contract as FileBackedCompositeTestRunStore.list.)
        from .conftest import make_client_error
        def boom(**kw):
            raise make_client_error("AccessDeniedException", op="StartQuery")
        fake_logs_client.start_query = boom

        store = open_store()
        assert store.get_stats("x") == {"count": 0, "min": None, "max": None,
                                         "avg": None, "sum": None}
        assert store.get_percentiles("x") is None
        assert store.get_error_rate("term") == 0.0
        assert store.get_one("x") is None
        assert store.get("x") == []

    def test_input_escaping(self, fake_logs_client, open_store):
        # Backticks and quotes must not break out of the literal.
        fake_logs_client.queue_query_results([])
        store = open_store()
        store.get_stats("service_time", task='evil"; drop; `')
        q = fake_logs_client.start_query_calls[-1]["queryString"]
        # The Task literal must not contain raw " or `
        task_part = q[q.index("Task ="):q.index("Task =") + 60]
        assert "`" not in task_part[task_part.index('"') + 1:task_part.rindex('"')]


# ------------------------------------------------------ CloudWatchTestRunStore reads


class _StoreCfg:
    def __init__(self):
        self._o = {("system", "env.name"): "default",
                   ("system", "list.test_runs.max_results"): 20}
    def opts(self, section, key, default_value=None, mandatory=True):
        return self._o.get((section, key), default_value)


_VALID_TEST_DOC = {
    "benchmark-version": "2.3.0",
    "test-run-id": "run-abc",
    "test-run-timestamp": "20260622T120000Z",
    "environment": "default",
    "pipeline": "benchmark-only",
    "workload": "geonames",
    "cluster-config-instance": "c",
}


class TestCloudWatchTestRunStoreReads:
    def _store(self, fake_logs_client, cw_config):
        return CloudWatchTestRunStore(
            cfg=_StoreCfg(),
            client_factory_class=_make_factory(fake_logs_client),
            config_loader=lambda cfg: cw_config,
        )

    def test_list_roundtrips(self, fake_logs_client, cw_config):
        fake_logs_client.queue_query_results([
            [{"field": "@message", "value": json.dumps(_VALID_TEST_DOC)}]
        ])
        store = self._store(fake_logs_client, cw_config)
        runs = store.list()
        assert len(runs) == 1
        assert runs[0].test_run_id == "run-abc"
        q = fake_logs_client.start_query_calls[-1]["queryString"]
        assert 'environment = "default"' in q
        assert "limit 20" in q

    def test_list_empty(self, fake_logs_client, cw_config):
        fake_logs_client.queue_query_results([])
        store = self._store(fake_logs_client, cw_config)
        assert store.list() == []

    def test_list_skips_malformed(self, fake_logs_client, cw_config):
        fake_logs_client.queue_query_results([
            [{"field": "@message", "value": json.dumps(_VALID_TEST_DOC)}],
            [{"field": "@message", "value": "{not json"}],
            [{"field": "@message", "value": json.dumps({"bad": "shape"})}],
        ])
        store = self._store(fake_logs_client, cw_config)
        runs = store.list()
        assert len(runs) == 1

    def test_find_by_test_run_id_roundtrips(self, fake_logs_client, cw_config):
        fake_logs_client.queue_query_results([
            [{"field": "@message", "value": json.dumps(_VALID_TEST_DOC)}]
        ])
        store = self._store(fake_logs_client, cw_config)
        run = store.find_by_test_run_id("run-abc")
        assert run.test_run_id == "run-abc"
        q = fake_logs_client.start_query_calls[-1]["queryString"]
        assert '`test-run-id` = "run-abc"' in q

    def test_find_by_test_run_id_miss_raises_with_os_wording(
            self, fake_logs_client, cw_config):
        # Empty results in both the 7-day and 90-day window queries.
        fake_logs_client.queue_query_results([])
        store = self._store(fake_logs_client, cw_config)
        with pytest.raises(exceptions.NotFound,
                           match=r"No test_run with test_run id"):
            store.find_by_test_run_id("nope")


# ---------------------------------------------------- FileBackedCompositeTestRunStore


class _FakeFile:
    def __init__(self):
        self.runs = {}
        self.stored = []
    def store_test_run(self, run): self.stored.append(run)
    def find_by_test_run_id(self, tid):
        if tid in self.runs:
            return self.runs[tid]
        raise exceptions.NotFound("not local")
    def list(self): return list(self.runs.values())
    def store_html_results(self, run): pass


class _FakeCW:
    def __init__(self):
        self.list_result = []
        self.list_called = False
        self.find_result = None
        self.find_called = False
    def store_test_run(self, run): pass
    def list(self):
        self.list_called = True
        return self.list_result
    def find_by_test_run_id(self, tid):
        self.find_called = True
        if self.find_result is not None:
            return self.find_result
        raise exceptions.NotFound("not cw")


class _Run:
    def __init__(self, test_run_id): self.test_run_id = test_run_id


class TestFileBackedComposite:
    def test_find_prefers_file_when_present(self):
        f = _FakeFile()
        f.runs["abc"] = _Run("abc")
        cw = _FakeCW()
        c = FileBackedCompositeTestRunStore(cw, f)
        result = c.find_by_test_run_id("abc")
        assert result.test_run_id == "abc"
        assert not cw.find_called

    def test_find_falls_back_to_cw_on_miss(self):
        cw = _FakeCW()
        cw.find_result = _Run("abc")
        c = FileBackedCompositeTestRunStore(cw, _FakeFile())
        result = c.find_by_test_run_id("abc")
        assert result.test_run_id == "abc"
        assert cw.find_called

    def test_find_both_miss_raises_notfound(self):
        c = FileBackedCompositeTestRunStore(_FakeCW(), _FakeFile())
        with pytest.raises(exceptions.NotFound):
            c.find_by_test_run_id("nope")

    def test_list_merges_and_dedupes(self):
        f = _FakeFile()
        f.runs["a"] = _Run("a")
        f.runs["b"] = _Run("b")
        cw = _FakeCW()
        cw.list_result = [_Run("b"), _Run("c")]  # b dup'd, c CW-only
        c = FileBackedCompositeTestRunStore(cw, f)
        ids = [r.test_run_id for r in c.list()]
        assert ids == ["a", "b", "c"]

    def test_list_degrades_gracefully_on_cw_error(self):
        class _FailingCW:
            def list(self): raise RuntimeError("boom (any exception)")
        f = _FakeFile()
        f.runs["a"] = _Run("a")
        c = FileBackedCompositeTestRunStore(_FailingCW(), f)
        ids = [r.test_run_id for r in c.list()]
        assert ids == ["a"]  # CW failure didn't break the file-store list

    def test_write_fans_out(self):
        f = _FakeFile()
        cw = _FakeCW()
        cw.stored = []
        # Add `stored` to _FakeCW for the test
        cw_stored = []
        def store(run): cw_stored.append(run)
        cw.store_test_run = store
        c = FileBackedCompositeTestRunStore(cw, f)
        c.store_test_run(_Run("x"))
        assert len(f.stored) == 1
        assert len(cw_stored) == 1
