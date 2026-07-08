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
CloudWatchMetricsStore — concrete MetricsStore that ships every sample to
CloudWatch Logs as an EMF event.

Buffers samples in memory and flushes to CloudWatch Logs via PutLogEvents
when the batch reaches CloudWatch's per-call limits (10,000 events or 1 MiB
of payload, accounting for the per-event overhead) or when an explicit
flush is requested. Read methods (get_one, get_stats, get_percentiles,
get_error_rate, get_unit) execute CloudWatch Logs Insights queries
against the configured metrics log group.
"""
import collections
import datetime as _datetime
import json
import os
import re
import time as _time

from osbenchmark import time
from osbenchmark.metrics import MetricsStore
from osbenchmark.metrics_stores.cloudwatch import emf, insights
from osbenchmark.metrics_stores.cloudwatch.client import CloudWatchClientFactory
from osbenchmark.metrics_stores.cloudwatch.config import (
    CloudWatchConfig,
    load as load_cw_config,
)
from osbenchmark.metrics_stores.cloudwatch.log_streams import (
    LogStreamWriter,
    ensure_log_group,
    ensure_log_stream,
    _MAX_BATCH_BYTES,
    _MAX_EVENTS_PER_BATCH,
)


# Source-of-truth for flush thresholds lives in log_streams.py; re-import
# rather than duplicating the numeric values here.
_MAX_BUFFERED_EVENTS = _MAX_EVENTS_PER_BATCH
_MAX_BUFFERED_BYTES = _MAX_BATCH_BYTES

# CloudWatch Logs stream names are restricted to this character set; we
# substitute anything outside it with `_` so user-supplied workload names
# (which can contain spaces or other characters) don't fail at provisioning.
_VALID_STREAM_CHARS = re.compile(r"[^\w\-\./#]+")


class CloudWatchMetricsStore(MetricsStore):
    """
    A metrics store backed by Amazon CloudWatch Logs (via EMF).

    Accumulates EMF-formatted log events in memory and ships them to a
    configurable CloudWatch Logs group on ``flush()`` or whenever the
    in-memory buffer reaches one of CloudWatch's per-call limits.

    Read methods (``get_one``, ``get_stats``, ``get_percentiles``,
    ``get_error_rate``, ``get_unit``) execute CloudWatch Logs Insights
    queries against the configured metrics log group; return shapes
    match :class:`osbenchmark.metrics.OsMetricsStore` so callers
    (publisher, aggregator, ``GlobalStatsCalculator``) don't need to
    care which backend they're talking to.
    """

    def __init__(self, cfg,
                 client_factory_class=CloudWatchClientFactory,
                 config_loader=load_cw_config,
                 clock=time.Clock, meta_info=None):
        """
        :param cfg: OSB Config object. Mandatory.
        :param client_factory_class: Optional override for tests — a
            factory whose ``logs_client()`` returns an object compatible
            with the boto3 CloudWatch Logs client interface used by
            ``LogStreamWriter``.
        :param config_loader: Optional override for tests — a callable
            mapping ``cfg`` to a :class:`CloudWatchConfig`.
        :param clock: Optional clock override (for tests).
        :param meta_info: Optional previously-serialized meta-info dict.
        """
        MetricsStore.__init__(self, cfg=cfg, clock=clock, meta_info=meta_info)
        self._cw_config: CloudWatchConfig = config_loader(cfg)
        self._client_factory = client_factory_class(self._cw_config)
        # Defer client / log-group provisioning until open(create=True) so
        # read-only opens (list / compare) don't trigger AWS calls.
        self._logs_client = None
        self._writer = None
        # In-memory buffer of pre-formatted ``{"timestamp", "message"}`` dicts.
        self._buffered_events = []
        self._buffered_bytes = 0

    def open(self, test_run_id=None, test_run_timestamp=None,
             workload_name=None, test_procedure_name=None,
             cluster_config_name=None, ctx=None, create=False):
        MetricsStore.open(
            self, test_run_id, test_run_timestamp,
            workload_name, test_procedure_name,
            cluster_config_name, ctx, create)

        # Only do AWS work when we are actually opening for writes. A
        # read-only open (e.g. `osbenchmark list test-runs`) shouldn't
        # require any CloudWatch permissions or pay for the STS probe.
        if not create:
            return

        # Probe identity once per writable open so the user sees which AWS
        # account and IAM principal is being written under before any data
        # ships. Raises ConfigError (from client.py) on cred / region /
        # endpoint problems.
        self._client_factory.probe_caller_identity()
        self._logs_client = self._client_factory.logs_client()

        stream_name = self._log_stream_name()
        ensure_log_group(
            self._logs_client,
            self._cw_config.metrics_log_group,
            retention_days=self._cw_config.log_retention_days)
        ensure_log_stream(
            self._logs_client,
            self._cw_config.metrics_log_group,
            stream_name)

        self._writer = LogStreamWriter(
            self._logs_client,
            self._cw_config.metrics_log_group,
            stream_name)

    def _log_stream_name(self) -> str:
        """
        Build a per-worker CloudWatch Logs stream name. CloudWatch only
        accepts [.\\-_/#A-Za-z0-9] in stream names, so user-supplied
        workload names are sanitized rather than passed through verbatim.
        """
        workload = self._workload or "unknown"
        safe_workload = _VALID_STREAM_CHARS.sub("_", workload)
        return f"{safe_workload}/{self._test_run_id}/{os.getpid()}"

    # ------------------------------------------------------------------ writes

    def _add(self, doc):
        """
        Transform an OSB metric document into one or more EMF events and buffer them.

        Routes between the single-metric ``put_value_*_level`` shape (doc
        has both ``name`` and ``value``, produces one event) and the
        multi-metric telemetry ``put_doc`` shape (many numeric fields;
        may produce multiple events because CloudWatch caps the total
        metrics-per-log-event at ~100).

        Flushes synchronously when the in-memory batch reaches CloudWatch's
        per-call limits so the writer never has to subdivide a single
        flush.
        """
        if "value" in doc:
            event = emf.build_event(doc, namespace=self._cw_config.namespace)
            if event is None:
                # Non-numeric single-metric value — already logged at WARNING.
                return
            events = [event]
        else:
            events = emf.build_telemetry_event(
                doc, namespace=self._cw_config.namespace)

        for e in events:
            message = json.dumps(e, separators=(",", ":"))
            encoded_size = len(message.encode("utf-8"))
            timestamp = e["_aws"]["Timestamp"]

            # If appending this event would push us over a CloudWatch limit,
            # flush first so the buffer always fits in a single PutLogEvents.
            if self._buffered_events and (
                len(self._buffered_events) >= _MAX_BUFFERED_EVENTS
                or self._buffered_bytes + encoded_size > _MAX_BUFFERED_BYTES
            ):
                self.flush(refresh=False)

            self._buffered_events.append({"timestamp": timestamp, "message": message})
            self._buffered_bytes += encoded_size

    def flush(self, refresh=True):
        """
        Ship the in-memory buffer to CloudWatch Logs.

        :param refresh: Accepted for interface parity with the OpenSearch
            metrics store; CloudWatch Logs has no index-refresh concept.

        On a transient write failure the buffer is restored so a follow-up
        flush can retry the same events rather than losing them.
        """
        if not self._buffered_events:
            return
        if self._writer is None:
            raise RuntimeError(
                "CloudWatchMetricsStore.flush called before open(create=True)")
        sw = time.StopWatch()
        sw.start()
        events_to_send = self._buffered_events
        prior_bytes = self._buffered_bytes
        self._buffered_events = []
        self._buffered_bytes = 0
        try:
            sent = self._writer.write_batch(events_to_send)
        except Exception:
            # Re-buffer so the events aren't lost on a transient failure;
            # a follow-up flush can retry the same batch.
            self._buffered_events = events_to_send + self._buffered_events
            self._buffered_bytes = prior_bytes + self._buffered_bytes
            raise
        sw.stop()
        self.logger.info(
            "CloudWatch datastore: shipped %d EMF events to %s in %.3fs",
            sent, self._cw_config.metrics_log_group, sw.total_time())

    def to_externalizable(self, clear=False):
        # Buffered events have already been transformed to EMF; we don't
        # ship them across the actor boundary the way InMemoryMetricsStore
        # does. Force a flush so nothing is lost. ``clear`` is accepted
        # for interface parity but has no separate meaning here — flushing
        # is the only externalization step.
        self.flush(refresh=False)
        return None

    # `close()` inherited from MetricsStore — its implementation already
    # calls self.flush(), clears meta-info, and sets opened=False, which
    # is exactly what we want.

    # ------------------------------------------------------------------ reads
    # Backed by CloudWatch Logs Insights against the configured metrics
    # log group. Every query is scoped to the current TestRunId so reads
    # never bleed across runs.

    def _read_logs_client(self):
        """
        Lazily build a CloudWatch Logs client for read-only operations.
        Reuses the writer's client if open() already established one;
        otherwise builds a fresh client (skipping STS probe — reads only
        need logs:* permissions, no need to surface caller identity).
        """
        if self._logs_client is not None:
            return self._logs_client
        self._logs_client = self._client_factory.logs_client()
        return self._logs_client

    def _insights_window(self):
        """
        Time window used for every Insights query on this store. Spans
        the test-run timestamp (epoch seconds) up to "now"; broad enough
        that a slow benchmark plus clock skew still falls inside.

        ``time.from_is8601`` returns a naive datetime — we explicitly
        attach UTC because OSB's ``to_iso8601`` (osbenchmark/time.py:38)
        always serializes UTC, and ``.timestamp()`` on a naive datetime
        would otherwise apply the local timezone and skew the window by
        hours on non-UTC hosts.
        """
        ts = time.from_is8601(self._test_run_timestamp)
        ts_utc = ts.replace(tzinfo=_datetime.timezone.utc)
        start = int(ts_utc.timestamp()) - 60  # 60s grace for clock skew
        end = int(_time.time()) + 60
        return start, end

    @staticmethod
    def _escape_query_value(value):
        """
        Defensive escaping for values interpolated into Insights query
        strings. Backticks and double-quotes would break out of the
        quoted literal; replace them with underscores. OSB inputs
        (workload-defined task / operation names) shouldn't contain
        these in practice, but the workload track files are user-
        authored — defending against accidental query corruption is
        cheap.
        """
        return str(value).replace("`", "_").replace('"', "_")

    def _filter_clause(self, name, task, operation_type, sample_type, node_name):
        """Build the ``filter`` clause shared by every read-side query."""
        safe_name = self._escape_query_value(name)
        parts = [f'TestRunId = "{self._test_run_id}"']
        parts.append(f'ispresent(`{safe_name}`)')
        if task is not None:
            parts.append(f'Task = "{self._escape_query_value(task)}"')
        if operation_type is not None:
            parts.append(f'OperationType = "{self._escape_query_value(operation_type)}"')
        if sample_type is not None:
            parts.append(f'SampleType = "{sample_type.name.lower()}"')
        if node_name is not None:
            parts.append(f'`meta.node_name` = "{self._escape_query_value(node_name)}"')
        return " and ".join(parts)

    def _run_insights(self, query: str, limit: int = 10_000):
        """Wrap insights.run_query with this store's log group / window.

        Returns ``[]`` if the query fails for any reason (Insights
        timeout, permission denied, log group missing, throttle
        exhaustion). The result-summary path
        (``test_run_orchestrator.calculate_results``) tolerates empty
        results by returning ``None`` / ``0.0`` from the calling
        read methods, so a read-permission gap doesn't crash the
        benchmark. Reads of the test-run / results stores are
        already fail-soft via FileBackedCompositeTestRunStore."""
        start, end = self._insights_window()
        try:
            return insights.run_query(
                self._read_logs_client(),
                self._cw_config.metrics_log_group,
                query, start, end, limit=limit,
            )
        except insights.InsightsQueryError as e:
            self.logger.warning(
                "CloudWatch Logs Insights query failed (%s); falling back "
                "to empty result. Reports for this test run will be blank.",
                e,
            )
            return []

    # Fields that the parent's standard accessors read; we always coerce
    # these to float when materializing docs so callers like
    # ``GlobalStatsCalculator.duration`` (which reads ``relative-time-ms``)
    # don't accidentally get Insights's string-typed values.
    _NUMERIC_DOC_FIELDS = frozenset({"value", "relative-time-ms"})

    def _row_to_doc(self, row, name):
        """
        Materialize an Insights row into the OSB doc shape callers
        expect: ``{value, task, operation, operation-type, sample-type,
        unit, relative-time-ms, meta: {node_name, success}, ...}``.
        ``value`` is taken from the metric-named column (the EMF pivot
        from emf.build_event).
        """
        doc = {"value": insights.to_float(row.get(name))}
        if row.get("Task") is not None:
            doc["task"] = row["Task"]
        if row.get("OperationType") is not None:
            doc["operation-type"] = row["OperationType"]
        if row.get("Operation") is not None:
            doc["operation"] = row["Operation"]
        if row.get("SampleType") is not None:
            doc["sample-type"] = row["SampleType"]
        if row.get("Unit") is not None:
            doc["unit"] = row["Unit"]
        if row.get("RelativeTimeMs") is not None:
            doc["relative-time-ms"] = insights.to_float(row["RelativeTimeMs"])
        meta = {}
        if row.get("meta.node_name") is not None:
            meta["node_name"] = row["meta.node_name"]
        if row.get("meta.success") is not None:
            meta["success"] = row["meta.success"]
        if meta:
            doc["meta"] = meta
        return doc

    # Common set of fields fetched alongside the metric value so the
    # caller's mapper can read any of OSB's standard doc fields without
    # the read path having to guess at intent.
    _DEFAULT_FIELDS_QUERY = (
        "Task, OperationType, Operation, SampleType, Unit, "
        "RelativeTimeMs, `meta.node_name`, `meta.success`"
    )

    def _get(self, name, task, operation_type, sample_type, node_name, mapper):
        filter_ = self._filter_clause(name, task, operation_type, sample_type, node_name)
        safe_name = self._escape_query_value(name)
        query = (
            f"filter {filter_}\n"
            f"| fields `{safe_name}`, {self._DEFAULT_FIELDS_QUERY}\n"
            f"| limit 10000"
        )
        rows = self._run_insights(query)
        return [mapper(self._row_to_doc(row, name)) for row in rows]

    def get_one(self, name, sample_type=None, node_name=None, task=None,
                mapper=lambda doc: doc["value"],
                sort_key=None, sort_reverse=False):
        filter_ = self._filter_clause(name, task, None, sample_type, node_name)
        order = "desc" if sort_reverse else "asc"
        sort_field = sort_key if sort_key else "@timestamp"
        safe_name = self._escape_query_value(name)
        # Always fetch the full default field set so mappers like
        # ``doc["relative-time-ms"]`` or ``doc["unit"]`` resolve to
        # properly-typed values regardless of which sort key the caller
        # supplied. _row_to_doc coerces numerics.
        query = (
            f"filter {filter_}\n"
            f"| fields `{safe_name}`, {self._DEFAULT_FIELDS_QUERY}, `{sort_field}`\n"
            f"| sort `{sort_field}` {order}\n"
            f"| limit 1"
        )
        rows = self._run_insights(query, limit=1)
        if not rows:
            return None
        return mapper(self._row_to_doc(rows[0], name))

    def get_error_rate(self, task, operation_type=None, sample_type=None):
        filter_ = self._filter_clause("service_time", task, operation_type, sample_type, None)
        query = (
            f"filter {filter_}\n"
            f"| stats count(*) as samples by `meta.success`"
        )
        rows = self._run_insights(query)
        success = 0
        errors = 0
        for row in rows:
            count = int(insights.to_float(row.get("samples")) or 0)
            success_val = row.get("meta.success")
            # Insights returns booleans as string "0"/"1" or "true"/"false"
            # depending on how the source field was serialized; accept both.
            if success_val in ("true", "1", "True"):
                success += count
            elif success_val in ("false", "0", "False"):
                errors += count
        if errors == 0:
            return 0.0
        if success == 0:
            return 1.0
        return errors / (errors + success)

    def get_stats(self, name, task=None, operation_type=None, sample_type=None):
        """Return a dict compatible with the OS ``stats`` aggregation:
        ``{count, min, max, avg, sum}``."""
        filter_ = self._filter_clause(name, task, operation_type, sample_type, None)
        safe_name = self._escape_query_value(name)
        query = (
            f"filter {filter_}\n"
            f"| stats min(`{safe_name}`) as min, max(`{safe_name}`) as max, "
            f"avg(`{safe_name}`) as avg, sum(`{safe_name}`) as sum, "
            f"count(*) as count"
        )
        rows = self._run_insights(query)
        if not rows:
            return {"count": 0, "min": None, "max": None,
                    "avg": None, "sum": None}
        row = rows[0]
        return {
            "count": int(insights.to_float(row.get("count")) or 0),
            "min": insights.to_float(row.get("min")),
            "max": insights.to_float(row.get("max")),
            "avg": insights.to_float(row.get("avg")),
            "sum": insights.to_float(row.get("sum")),
        }

    def get_percentiles(self, name, task=None, operation_type=None,
                        sample_type=None, percentiles=None):
        """Return an OrderedDict ``{<percentile>: <value>}`` or ``None``
        when there are no samples (matches OsMetricsStore semantics)."""
        if percentiles is None:
            percentiles = [99, 99.9, 100]
        filter_ = self._filter_clause(name, task, operation_type, sample_type, None)
        safe_name = self._escape_query_value(name)

        # Build the stats list. Insights's pct() doesn't accept 100; use
        # max() for the p100 case so we always return a numeric value.
        stats_parts = []
        for p in percentiles:
            if float(p) >= 100:
                stats_parts.append(f"max(`{safe_name}`) as `p_{_alias(p)}`")
            else:
                stats_parts.append(f"pct(`{safe_name}`, {p}) as `p_{_alias(p)}`")
        # count(*) so we can replicate OS's "no hits → None" behavior.
        stats_parts.append(f"count(*) as count")
        query = (
            f"filter {filter_}\n"
            f"| stats " + ", ".join(stats_parts)
        )
        rows = self._run_insights(query)
        if not rows:
            return None
        row = rows[0]
        if int(insights.to_float(row.get("count")) or 0) == 0:
            return None
        result = collections.OrderedDict()
        for p in sorted(percentiles, key=float):
            value = insights.to_float(row.get(f"p_{_alias(p)}"))
            result[str(p)] = value
        return result

    def __str__(self):
        return "CloudWatch metrics store"


def _alias(percentile) -> str:
    """
    Insights field aliases can't contain dots. Convert ``99.9`` -> ``99_9``
    so the alias survives the ``stats ... as p_<alias>`` rename.
    """
    return str(percentile).replace(".", "_")
