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
Unit tests for the CloudWatch datastore write path: config parsing,
EMF document builders, log-stream writer (chunking + retry), and the
three store classes' write flows.
"""
import datetime
import json

import botocore.exceptions
import pytest

from osbenchmark import exceptions, metrics
from osbenchmark.metrics_stores.cloudwatch import config as cw_config_mod
from osbenchmark.metrics_stores.cloudwatch import emf
from osbenchmark.metrics_stores.cloudwatch.config import CloudWatchConfig
from osbenchmark.metrics_stores.cloudwatch.log_streams import (
    LogStreamWriter,
    _MAX_EVENTS_PER_BATCH,
    _MAX_EVENT_BYTES,
)
from osbenchmark.metrics_stores.cloudwatch.metrics_store import (
    CloudWatchMetricsStore,
)
from osbenchmark.metrics_stores.cloudwatch.results_store import (
    CloudWatchResultsStore,
)
from osbenchmark.metrics_stores.cloudwatch.test_run_store import (
    CloudWatchTestRunStore,
)

from .conftest import make_client_error


# --------------------------------------------------------------------- config


class _Cfg:
    """Tiny Config double — `opts(section, key, default_value=..., mandatory=...)`."""
    def __init__(self, opts):
        self._o = opts

    def opts(self, section, key, default_value=None, mandatory=True):
        return self._o.get((section, key), default_value)


class TestConfigLoad:
    def test_defaults(self):
        cfg = _Cfg({})
        c = cw_config_mod.load(cfg)
        assert c.region is None  # optional; boto3 resolves from chain
        assert c.namespace == "OSB"
        assert c.metrics_log_group == "benchmark-metrics"
        assert c.test_runs_log_group == "benchmark-test-runs"
        assert c.results_log_group == "benchmark-results"
        assert c.log_retention_days is None
        assert c.spool_enabled is True
        assert c.spool_trigger_failures == 3
        assert c.spool_recheck_seconds == 60

    def test_explicit_overrides(self):
        cfg = _Cfg({
            ("reporting", "datastore.region"): "us-west-2",
            ("reporting", "datastore.namespace"): "TeamX",
            ("reporting", "datastore.log_retention_days"): 30,
            ("reporting", "datastore.profile"): "prod",
            ("reporting", "datastore.cloudwatch.spool.enabled"): "false",
            ("reporting", "datastore.cloudwatch.spool.trigger_failures"): 5,
        })
        c = cw_config_mod.load(cfg)
        assert c.region == "us-west-2"
        assert c.namespace == "TeamX"
        assert c.log_retention_days == 30
        assert c.profile == "prod"
        assert c.spool_enabled is False
        assert c.spool_trigger_failures == 5

    def test_invalid_retention_rejected(self):
        cfg = _Cfg({("reporting", "datastore.log_retention_days"): 17})
        with pytest.raises(exceptions.SystemSetupError, match="not accepted"):
            cw_config_mod.load(cfg)

    def test_1096_retention_accepted(self):
        # AWS-correctness regression: commit #3 added 1096 (3 years) which
        # was missing from the initial enum.
        cfg = _Cfg({("reporting", "datastore.log_retention_days"): 1096})
        c = cw_config_mod.load(cfg)
        assert c.log_retention_days == 1096

    def test_garbage_bool_rejected(self):
        cfg = _Cfg({
            ("reporting", "datastore.cloudwatch.spool.enabled"): "banana",
        })
        with pytest.raises(exceptions.SystemSetupError, match="boolean"):
            cw_config_mod.load(cfg)

    def test_bool_rejected_for_int_field(self):
        cfg = _Cfg({
            ("reporting", "datastore.cloudwatch.spool.trigger_failures"): True,
        })
        with pytest.raises(exceptions.SystemSetupError, match="integer"):
            cw_config_mod.load(cfg)

    def test_minimum_enforced(self):
        cfg = _Cfg({
            ("reporting", "datastore.cloudwatch.spool.trigger_failures"): 0,
        })
        with pytest.raises(exceptions.SystemSetupError, match=">= 1"):
            cw_config_mod.load(cfg)


# ---------------------------------------------------------------------- EMF


class TestEmfBuildEvent:
    def _doc(self, **overrides):
        doc = {
            "@timestamp": 1709654400000,
            "test-run-id": "abc-123",
            "test-run-timestamp": "20260622T120000Z",
            "environment": "default",
            "workload": "big5",
            "test_procedure": "p",
            "cluster-config-instance": "c",
            "name": "service_time",
            "value": 12.3,
            "unit": "ms",
            "sample-type": "normal",
            "task": "term",
            "operation": "term",
            "operation-type": "search",
            "meta": {"node_name": "node-0", "success": True},
        }
        doc.update(overrides)
        return doc

    def test_full_doc_emits_emf_metric(self):
        evt = emf.build_event(self._doc(), namespace="OSB")
        # Pivot: metric value at top level under its name
        assert evt["service_time"] == 12.3
        # Dimensions populated
        assert evt["Workload"] == "big5"
        assert evt["Task"] == "term"
        assert evt["OperationType"] == "search"
        assert evt["SampleType"] == "normal"
        # Run-identity fields land at top level (queryable, not dimensions)
        assert evt["TestRunId"] == "abc-123"
        assert evt["Environment"] == "default"
        assert evt["Operation"] == "term"
        assert evt["Unit"] == "ms"
        # Meta fanned out with `meta.` prefix
        assert evt["meta.node_name"] == "node-0"
        assert evt["meta.success"] is True
        # _aws block well-formed
        aws = evt["_aws"]
        assert aws["Timestamp"] == 1709654400000
        cw_metrics = aws["CloudWatchMetrics"]
        assert len(cw_metrics) == 1
        directive = cw_metrics[0]
        assert directive["Namespace"] == "OSB"
        assert directive["Dimensions"] == [
            ["Workload", "Task", "OperationType", "SampleType"]
        ]
        assert directive["Metrics"] == [
            {"Name": "service_time", "Unit": "Milliseconds", "StorageResolution": 1}
        ]

    def test_no_dimensions_uses_inner_empty(self):
        # EMF schema needs `Dimensions: [[]]` for the no-dim case, NOT
        # `Dimensions: []`. Regression for the commit-5 fix.
        evt = emf.build_event(
            {"name": "x", "value": 1.0, "@timestamp": 1},
            namespace="OSB")
        assert evt["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [[]]

    def test_partial_dimensions(self):
        evt = emf.build_event(
            {"name": "x", "value": 1.0, "@timestamp": 1,
             "workload": "big5", "sample-type": "normal"},
            namespace="OSB")
        dims = evt["_aws"]["CloudWatchMetrics"][0]["Dimensions"]
        assert dims == [["Workload", "SampleType"]]
        assert "Task" not in evt
        assert "OperationType" not in evt

    def test_non_numeric_value_dropped(self):
        # Strings, None, and booleans (bool is a Real subclass!) must be
        # rejected so a stray non-number can't slip into the metric.
        for bad in (None, "string", True, False):
            doc = {"name": "x", "value": bad, "@timestamp": 1}
            assert emf.build_event(doc, namespace="OSB") is None

    def test_pivot_wins_on_name_collision(self):
        # Metric named "Workload" must NOT be overwritten by dim assignment.
        evt = emf.build_event(
            {"name": "Workload", "value": 99.0, "@timestamp": 1,
             "workload": "big5"},
            namespace="OSB")
        assert evt["Workload"] == 99.0  # pivot wins, not the string "big5"

    def test_missing_timestamp_defaults_to_now(self):
        evt = emf.build_event({"name": "x", "value": 1.0}, namespace="OSB")
        assert isinstance(evt["_aws"]["Timestamp"], int)
        assert evt["_aws"]["Timestamp"] > 0

    def test_unknown_unit_falls_back_to_none(self):
        evt = emf.build_event(
            {"name": "x", "value": 1.0, "@timestamp": 1, "unit": "fortnights"},
            namespace="OSB")
        assert evt["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Unit"] == "None"

    def test_unit_case_insensitive(self):
        evt = emf.build_event(
            {"name": "x", "value": 1.0, "@timestamp": 1, "unit": "MS"},
            namespace="OSB")
        assert evt["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Unit"] == "Milliseconds"


class TestEmfBuildTelemetryEvent:
    def test_flattened_doc_groups_by_prefix(self):
        # NodeStats-like payload — three prefixes: indices_, jvm_, os_.
        doc = {
            "@timestamp": 1,
            "workload": "big5",
            "sample-type": "normal",
            "name": "node-stats",
            "indices_segments_count": 42,
            "indices_docs_count": 1000000,
            "jvm_mem_heap_used_percent": 73,
            "os_mem_used_percent": 41,
        }
        events = emf.build_telemetry_event(doc, namespace="OSB")
        # 4 metrics well under the per-event cap — everything fits in one event.
        assert len(events) == 1
        evt = events[0]
        directives = evt["_aws"]["CloudWatchMetrics"]
        prefixes = {d["Metrics"][0]["Name"].split("_")[0] for d in directives}
        assert prefixes == {"indices", "jvm", "os"}
        # All directives share the same Dimensions
        for d in directives:
            assert d["Dimensions"] == [["Workload", "SampleType"]]

    def test_chunked_when_more_than_100_metrics_per_directive(self):
        # NodeStats can exceed EMF's 100-metric-per-directive cap.
        # Directive chunking splits a single prefix group into ≤100-metric
        # directives; event-level packing (below) additionally distributes
        # directives across multiple log events.
        doc = {"@timestamp": 1, "workload": "w", "sample-type": "normal",
               "name": "stress"}
        for i in range(250):
            doc[f"indices_metric_{i:03d}"] = i
        events = emf.build_telemetry_event(doc, namespace="OSB")
        all_directive_sizes = [
            len(d["Metrics"])
            for e in events
            for d in e["_aws"]["CloudWatchMetrics"]
        ]
        assert all_directive_sizes == [100, 100, 50]
        # 250 metrics > 100 per-event cap, so must produce >1 event.
        assert len(events) >= 3

    def test_split_into_multiple_events_when_over_100_metrics_total(self):
        # CloudWatch caps total metric definitions per log event at ~100
        # (undocumented). 250 metrics across a single prefix group must
        # be split into >=3 log events, none exceeding 100 metrics.
        doc = {"@timestamp": 42, "workload": "geonames",
               "sample-type": "normal", "name": "node-stats"}
        for i in range(250):
            doc[f"jvm_field_{i:03d}"] = i
        events = emf.build_telemetry_event(doc, namespace="OSB")
        assert len(events) >= 3
        for e in events:
            n_metrics = sum(len(d["Metrics"]) for d in e["_aws"]["CloudWatchMetrics"])
            assert n_metrics <= 100
            # Every event carries the same identity + timestamp
            assert e["_aws"]["Timestamp"] == 42
            assert e["Workload"] == "geonames"
            assert e["SampleType"] == "normal"
        # Every declared metric name in every event must exist at top-level.
        for e in events:
            for d in e["_aws"]["CloudWatchMetrics"]:
                for m in d["Metrics"]:
                    assert m["Name"] in e, (
                        f"declared metric {m['Name']!r} missing from event")
        # Union of all declared names covers all 250 fields.
        all_declared = {
            m["Name"]
            for e in events
            for d in e["_aws"]["CloudWatchMetrics"]
            for m in d["Metrics"]
        }
        assert len(all_declared) == 250

    def test_nested_dict_payload_becomes_log_only_event(self):
        # RecoveryStats: {"name": "...", "shard": <dict>} with no
        # numeric leaves. Must still produce an event so Logs Insights
        # can find it; just without an extracted metric.
        doc = {
            "@timestamp": 1,
            "workload": "big5",
            "sample-type": "normal",
            "name": "recovery-stats",
            "shard": {"id": 0, "state": "DONE", "primary": True},
        }
        events = emf.build_telemetry_event(doc, namespace="OSB")
        assert len(events) == 1
        evt = events[0]
        # Nested data preserved at top level
        assert evt["shard"]["state"] == "DONE"
        # NO CloudWatchMetrics directive (no metrics to extract)
        assert "CloudWatchMetrics" not in evt["_aws"]
        # Timestamp still present
        assert evt["_aws"]["Timestamp"] == 1

    def test_bool_fields_preserved_as_log_only(self):
        doc = {"@timestamp": 1, "workload": "w", "sample-type": "normal",
               "name": "x", "is_primary": True, "count": 5}
        events = emf.build_telemetry_event(doc, namespace="OSB")
        assert len(events) == 1
        evt = events[0]
        # Bool preserved as a log field (not a metric)
        assert evt["is_primary"] is True
        # Numeric is the only metric
        metric_names = {
            m["Name"]
            for d in evt["_aws"]["CloudWatchMetrics"]
            for m in d["Metrics"]
        }
        assert metric_names == {"count"}

    def test_metric_names_with_invalid_chars_are_sanitized(self):
        # NodeStats can produce field names like
        # ``jvm_gc_collectors_G1 Concurrent GC_collection_count`` or
        # ``jvm_buffer_pools_mapped - 'non-volatile memory'_count``.
        # CloudWatch's EMF extractor rejects the entire MetricDirective
        # when any Name contains characters outside [A-Za-z0-9_.-], so
        # the sanitizer replaces those with underscores in BOTH the
        # top-level key and the directive's Metrics[].Name so they still
        # line up.
        doc = {
            "@timestamp": 1,
            "workload": "geonames",
            "name": "node-stats",
            "jvm_gc_collectors_G1 Concurrent GC_collection_count": 12,
            "jvm_buffer_pools_mapped - 'non-volatile memory'_count": 0,
            "os_cpu_percent": 41,
        }
        events = emf.build_telemetry_event(doc, namespace="OSB")
        assert len(events) == 1
        evt = events[0]

        # Directive names sanitized:
        declared = {
            m["Name"]
            for d in evt["_aws"]["CloudWatchMetrics"]
            for m in d["Metrics"]
        }
        assert "jvm_gc_collectors_G1_Concurrent_GC_collection_count" in declared
        assert "jvm_buffer_pools_mapped_-__non-volatile_memory__count" in declared
        assert "os_cpu_percent" in declared

        # Top-level keys match the directive names so CW can find the values.
        for name in declared:
            assert name in evt, f"declared metric {name!r} missing from event top-level"


# ------------------------------------------------------------------ LogStreamWriter


class TestLogStreamWriter:
    def test_writes_chronologically(self, fake_logs_client):
        w = LogStreamWriter(fake_logs_client, "g", "s")
        # Out-of-order events
        events = [
            {"timestamp": 3, "message": "c"},
            {"timestamp": 1, "message": "a"},
            {"timestamp": 2, "message": "b"},
        ]
        sent = w.write_batch(events)
        assert sent == 3
        # CloudWatch requires chronological order within a batch
        ts_order = [e["timestamp"] for e in fake_logs_client.put_calls[0]["logEvents"]]
        assert ts_order == [1, 2, 3]

    def test_chunks_by_event_count(self, fake_logs_client):
        w = LogStreamWriter(fake_logs_client, "g", "s")
        events = [{"timestamp": i, "message": "x"} for i in range(_MAX_EVENTS_PER_BATCH + 5)]
        w.write_batch(events)
        chunk_sizes = [len(c["logEvents"]) for c in fake_logs_client.put_calls]
        assert sum(chunk_sizes) == len(events)
        assert all(s <= _MAX_EVENTS_PER_BATCH for s in chunk_sizes)

    def test_oversized_single_event_dropped(self, fake_logs_client, caplog):
        w = LogStreamWriter(fake_logs_client, "g", "s")
        huge = "x" * (_MAX_EVENT_BYTES + 1)
        events = [
            {"timestamp": 1, "message": huge},  # dropped
            {"timestamp": 2, "message": "ok"},  # shipped
        ]
        sent = w.write_batch(events)
        assert sent == 1
        assert "exceeds" in caplog.text

    def test_throttle_retried(self, fake_logs_client):
        # 2 throttles then success
        fake_logs_client.fail_with = make_client_error("ThrottlingException")
        fake_logs_client.fail_count = 2
        w = LogStreamWriter(fake_logs_client, "g", "s")
        sent = w.write_batch([{"timestamp": 1, "message": "x"}])
        assert sent == 1

    def test_auth_error_retried_once_then_bubbles(self, fake_logs_client):
        fake_logs_client.fail_with = make_client_error("ExpiredTokenException")
        fake_logs_client.fail_count = 2
        w = LogStreamWriter(fake_logs_client, "g", "s")
        with pytest.raises(botocore.exceptions.ClientError):
            w.write_batch([{"timestamp": 1, "message": "x"}])

    def test_stream_recreated_on_resource_not_found(self, fake_logs_client):
        from .conftest import _ResourceNotFound
        fake_logs_client.fail_with = _ResourceNotFound("stream gone")
        fake_logs_client.fail_count = 1
        w = LogStreamWriter(fake_logs_client, "g", "s")
        sent = w.write_batch([{"timestamp": 1, "message": "x"}])
        assert sent == 1
        assert ("g", "s") in fake_logs_client.created_streams


# ----------------------------------------------------------- CloudWatchMetricsStore


class _MetricsCfg:
    """OSB Config double the metrics store needs at construction."""
    def __init__(self):
        self._o = {
            ("system", "env.name"): "default",
            ("workload", "params"): {},
            ("test_run", "user.tag"): "",
        }

    def opts(self, section, key, default_value=None, mandatory=True):
        return self._o.get((section, key), default_value)


def _make_fake_factory(fake_logs_client):
    """Build a CloudWatchClientFactory-compatible double."""
    class _Factory:
        def __init__(self, cw_cfg):
            self._client = fake_logs_client
            self.probed = False

        def probe_caller_identity(self):
            self.probed = True

        def logs_client(self):
            return self._client

    return _Factory


class TestCloudWatchMetricsStoreWrite:
    def _open_store(self, fake_logs_client, cw_config, create=True):
        store = CloudWatchMetricsStore(
            cfg=_MetricsCfg(),
            client_factory_class=_make_fake_factory(fake_logs_client),
            config_loader=lambda cfg: cw_config,
        )
        store.open(
            test_run_id="abc-123",
            test_run_timestamp=datetime.datetime(2026, 6, 22, 12, 0, 0),
            workload_name="big5",
            test_procedure_name="p",
            cluster_config_name="c",
            create=create,
        )
        return store

    def test_open_probes_identity_and_provisions(self, fake_logs_client, cw_config):
        store = self._open_store(fake_logs_client, cw_config)
        assert store._client_factory.probed
        assert cw_config.metrics_log_group in fake_logs_client.created_groups

    def test_read_only_open_skips_aws_calls(self, fake_logs_client, cw_config):
        store = self._open_store(fake_logs_client, cw_config, create=False)
        assert not store._client_factory.probed
        assert fake_logs_client.created_groups == set()

    def test_put_value_buffers_and_flushes(self, fake_logs_client, cw_config):
        store = self._open_store(fake_logs_client, cw_config)
        for i in range(3):
            store.put_value_cluster_level(
                name="service_time", value=float(i), unit="ms",
                task="term", operation="term", operation_type="search")
        assert len(store._buffered_events) == 3
        store.flush()
        assert len(fake_logs_client.put_calls) == 1
        assert len(fake_logs_client.put_calls[0]["logEvents"]) == 3
        # Each event is JSON-decodable EMF
        for evt in fake_logs_client.put_calls[0]["logEvents"]:
            parsed = json.loads(evt["message"])
            assert "_aws" in parsed
            assert parsed["service_time"] in (0.0, 1.0, 2.0)

    def test_non_numeric_value_dropped(self, fake_logs_client, cw_config):
        store = self._open_store(fake_logs_client, cw_config)
        store.put_value_cluster_level(name="bad", value=None, unit="ms")
        assert store._buffered_events == []

    def test_telemetry_put_doc_routes_to_telemetry_event(self, fake_logs_client, cw_config):
        store = self._open_store(fake_logs_client, cw_config)
        store.put_doc({
            "name": "node-stats",
            "indices_segments_count": 42,
            "jvm_mem_heap_used_percent": 73,
        }, level=metrics.MetaInfoScope.cluster)
        assert len(store._buffered_events) == 1
        msg = json.loads(store._buffered_events[0]["message"])
        # Multi-directive grouping for telemetry shape
        directives = msg["_aws"]["CloudWatchMetrics"]
        assert len(directives) == 2  # indices_, jvm_

    def test_flush_rebuffers_on_failure(self, fake_logs_client, cw_config):
        # Make AWS reject the first put_log_events; flush should
        # re-buffer the events so the caller can retry instead of
        # losing them.
        fake_logs_client.fail_with = make_client_error("AccessDenied")
        fake_logs_client.fail_count = 10
        store = self._open_store(fake_logs_client, cw_config)
        store.put_value_cluster_level(name="x", value=1.0, unit="ms")
        store.put_value_cluster_level(name="x", value=2.0, unit="ms")
        with pytest.raises(botocore.exceptions.ClientError):
            store.flush()
        assert len(store._buffered_events) == 2

    def test_close_flushes_and_clears(self, fake_logs_client, cw_config):
        store = self._open_store(fake_logs_client, cw_config)
        store.put_value_cluster_level(name="x", value=1.0, unit="ms")
        store.close()
        assert store.opened is False
        assert len(fake_logs_client.put_calls) == 1


# ----------------------------------------------------- TestRun + Results stores


class _StoreCfg:
    """Config double for the test-run/results store constructors."""
    def __init__(self):
        self._o = {
            ("system", "env.name"): "default",
            ("system", "list.test_runs.max_results"): 20,
        }

    def opts(self, section, key, default_value=None, mandatory=True):
        return self._o.get((section, key), default_value)


class _FakeTestRun:
    test_run_id = "abc-123"

    def as_dict(self):
        return {"test-run-id": "abc-123", "workload": "big5", "duration": 10.5}

    def to_result_dicts(self):
        return [
            {"test-run-id": "abc-123", "metric": "latency.mean", "value": 12.3},
            {"test-run-id": "abc-123", "metric": "throughput.mean", "value": 1000.0},
        ]


class TestCloudWatchTestRunStoreWrite:
    def test_store_test_run_emits_one_event(self, fake_logs_client, cw_config):
        store = CloudWatchTestRunStore(
            cfg=_StoreCfg(),
            client_factory_class=_make_fake_factory(fake_logs_client),
            config_loader=lambda cfg: cw_config,
        )
        store.store_test_run(_FakeTestRun())
        assert len(fake_logs_client.put_calls) == 1
        call = fake_logs_client.put_calls[0]
        assert call["logGroupName"] == cw_config.test_runs_log_group
        assert len(call["logEvents"]) == 1
        parsed = json.loads(call["logEvents"][0]["message"])
        assert parsed["test-run-id"] == "abc-123"


class TestCloudWatchResultsStoreWrite:
    def test_store_results_explodes_into_records(self, fake_logs_client, cw_config):
        store = CloudWatchResultsStore(
            cfg=_StoreCfg(),
            client_factory_class=_make_fake_factory(fake_logs_client),
            config_loader=lambda cfg: cw_config,
        )
        store.store_results(_FakeTestRun())
        assert len(fake_logs_client.put_calls) == 1
        events = fake_logs_client.put_calls[0]["logEvents"]
        assert len(events) == 2  # one per result dict
        metric_names = {json.loads(e["message"])["metric"] for e in events}
        assert metric_names == {"latency.mean", "throughput.mean"}

    def test_empty_results_does_not_provision(self, fake_logs_client, cw_config):
        class _Empty:
            test_run_id = "x"
            def to_result_dicts(self): return []
        store = CloudWatchResultsStore(
            cfg=_StoreCfg(),
            client_factory_class=_make_fake_factory(fake_logs_client),
            config_loader=lambda cfg: cw_config,
        )
        store.store_results(_Empty())
        assert fake_logs_client.put_calls == []
        # Writer wasn't constructed at all → no log group created
        assert cw_config.results_log_group not in fake_logs_client.created_groups
