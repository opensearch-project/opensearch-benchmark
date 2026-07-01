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
Embedded Metric Format (EMF) document builder.

Two entry points cover the two OSB metric-document shapes:

* ``build_event(doc, namespace)`` — for ``MetricsStore.put_value_*_level()``
  docs that carry a ``{name, value}`` pair. Pivots the name to a top-level
  key and declares a single metric in the ``_aws.CloudWatchMetrics`` block.

* ``build_telemetry_event(doc, namespace)`` — for
  ``MetricsStore.put_doc(doc, ...)`` payloads (NodeStats, ShardStats,
  RecoveryStats, etc.) where the document carries many numeric fields
  flattened by prefix. Groups numeric fields by their first underscore-
  delimited segment and emits multiple ``CloudWatchMetrics`` directives in
  one log event to stay under EMF's 100-metrics-per-directive cap.

Both transforms are pure — no I/O, no boto3 dependency. Unit-testable in
isolation.
"""
import logging
import numbers
import re
import time as _time
from typing import Any, Dict, List, Optional


_logger = logging.getLogger(__name__)

# Dimension set is hardcoded (kept deliberately low-cardinality so CloudWatch
# custom-metric counts and therefore costs stay bounded — see design.md
# §"Dimension cardinality"). The constant exists in case future commits want
# to iterate it; build_event below references each field by name for clarity.
_DIMENSION_FIELDS = ("Workload", "Task", "OperationType", "SampleType")


# Map OSB-supplied unit strings to the canonical CloudWatch Unit enum.
# Unknown units fall through to "None" (a legal Unit enum value meaning
# "unitless") — CloudWatch publishes the metric without a unit rather than
# rejecting it. See osb_unit_to_cloudwatch below.
#
# CloudWatch's Unit enum stops at Microseconds — there is no "Nanoseconds"
# value — so we deliberately do not list a "ns" entry. A caller that emits
# nanosecond-scale data should either convert to microseconds beforehand or
# accept the "None" fallback.
_UNIT_MAP = {
    "ms": "Milliseconds",
    "s":  "Seconds",
    "us": "Microseconds",
    "byte":   "Bytes",
    "bytes":  "Bytes",
    "kb":  "Kilobytes",
    "mb":  "Megabytes",
    "gb":  "Gigabytes",
    "tb":  "Terabytes",
    "bit":  "Bits",
    "kbit": "Kilobits",
    "mbit": "Megabits",
    "gbit": "Gigabits",
    "tbit": "Terabits",
    "percent":      "Percent",
    "count":        "Count",
    "docs":         "Count",
    "ops":          "Count",
    "docs/s":  "Count/Second",
    "ops/s":   "Count/Second",
    "bytes/s": "Bytes/Second",
    "byte/s":  "Bytes/Second",
    "kb/s":  "Kilobytes/Second",
    "mb/s":  "Megabytes/Second",
    "gb/s":  "Gigabytes/Second",
}


def osb_unit_to_cloudwatch(unit: Optional[str]) -> str:
    """
    Translate an OSB unit string to a CloudWatch Unit enum value.

    CloudWatch requires a Unit; we use "None" (the literal string accepted by
    the API for a unitless metric) for missing or unmappable units rather
    than dropping the field or emitting a value CloudWatch will reject.
    """
    if not isinstance(unit, str) or unit == "":
        return "None"
    return _UNIT_MAP.get(unit.lower(), "None")


def build_event(doc: Dict[str, Any], namespace: str) -> Optional[Dict[str, Any]]:
    """
    Build a single EMF log event from one OSB metric document.

    :param doc: A document produced by ``MetricsStore._put_metric``: must
        contain ``name`` (str) and ``value`` (numeric); typically also
        ``@timestamp``, ``unit``, ``workload``, ``task``, ``operation-type``,
        ``sample-type``, ``meta``, plus the various run-identity fields.
    :param namespace: The CloudWatch Metrics namespace (from
        ``CloudWatchConfig.namespace``).
    :return: A dict suitable for ``json.dumps()`` into a single
        ``PutLogEvents`` ``message``, or ``None`` if the document cannot be
        represented as an EMF event (non-numeric value). The ``_aws`` block
        instructs CloudWatch Logs to auto-extract ``doc["name"]`` as a metric
        under ``namespace``, dimensioned by the subset of
        ``_DIMENSION_FIELDS`` whose values are present in the document.

    The metric name is pivoted to be a top-level JSON key (EMF requirement);
    all other OSB fields are exposed as plain top-level fields so Logs
    Insights can filter on them.
    """
    metric_name = _sanitize_metric_name(doc["name"])
    metric_value = doc["value"]

    # EMF requires the metric target to be a numeric value. Reject non-numeric
    # values (including None) here rather than letting them silently disappear
    # at CloudWatch ingest. bool is a subclass of int in Python — exclude it
    # so a stray boolean doesn't become 0/1 metric data.
    if not isinstance(metric_value, numbers.Real) or isinstance(metric_value, bool):
        _logger.warning(
            "CloudWatch EMF: dropping non-numeric metric %r with value %r",
            metric_name, metric_value)
        return None

    # OSB's @timestamp is already epoch ms (metrics.py:627). Fall back to "now"
    # in epoch ms if the document is missing one — EMF schema marks Timestamp
    # as required, and a missing timestamp falls back to PutLogEvents server
    # time anyway, so a client-side default is just more honest.
    timestamp = doc.get("@timestamp")
    if timestamp is None:
        timestamp = int(_time.time() * 1000)

    event: Dict[str, Any] = {}

    # Populate the dimension fields from the OSB doc, but only include those
    # that actually have non-null values. CloudWatch can't reference a
    # dimension whose top-level field is absent.
    dimensions_present: List[str] = []
    workload = doc.get("workload")
    if workload is not None:
        event["Workload"] = workload
        dimensions_present.append("Workload")

    task = doc.get("task")
    if task is not None:
        event["Task"] = task
        dimensions_present.append("Task")

    op_type = doc.get("operation-type")
    if op_type is not None:
        event["OperationType"] = op_type
        dimensions_present.append("OperationType")

    sample_type = doc.get("sample-type")
    if sample_type is not None:
        event["SampleType"] = sample_type
        dimensions_present.append("SampleType")

    # Top-level fields (queryable via Logs Insights, NOT dimensions — keeps
    # custom-metric cardinality bounded).
    if doc.get("test-run-id") is not None:
        event["TestRunId"] = doc["test-run-id"]
    if doc.get("test-run-timestamp") is not None:
        event["TestRunTimestamp"] = doc["test-run-timestamp"]
    if doc.get("environment") is not None:
        event["Environment"] = doc["environment"]
    if doc.get("test_procedure") is not None:
        event["TestProcedure"] = doc["test_procedure"]
    if doc.get("cluster-config-instance") is not None:
        event["ClusterConfigInstance"] = doc["cluster-config-instance"]
    if doc.get("operation") is not None:
        event["Operation"] = doc["operation"]
    if doc.get("relative-time-ms") is not None:
        event["RelativeTimeMs"] = doc["relative-time-ms"]
    if doc.get("unit") is not None:
        # Surface the OSB unit as a top-level log field so the read path
        # can return it from MetricsStore.get_unit (the CloudWatch Unit
        # enum value inside _aws.CloudWatchMetrics is what CW Metrics
        # uses, but it's not queryable from Logs Insights).
        event["Unit"] = doc["unit"]
    if doc.get("workload-params"):
        # Truthy check matches metrics.py:647 — skip empty dicts.
        event["WorkloadParams"] = doc["workload-params"]

    # Meta fields (node_name, cpu_model, distribution_version, etc.) flow
    # through as top-level fields so they're queryable but don't inflate
    # metric counts. Keys are passed through verbatim — Logs Insights handles
    # dotted/underscored field names just fine.
    meta = doc.get("meta")
    if isinstance(meta, dict):
        for key, value in meta.items():
            if value is None:
                continue
            event[f"meta.{key}"] = value

    # Pivot LAST: assigning event[metric_name] last guards against a metric
    # whose name collides with one of the run-identity fields above (e.g. a
    # metric called "operation"). The numeric value wins; the conflicting
    # identity field is overwritten rather than the metric value being lost.
    event[metric_name] = metric_value

    event["_aws"] = {
        "Timestamp": timestamp,
        "CloudWatchMetrics": [{
            "Namespace": namespace,
            # EMF schema requires Dimensions to have minItems: 1 on the outer
            # array. The spec-correct way to publish a metric with no
            # dimensions is one empty DimensionSet ([[]]); the inner array
            # allows minItems: 0.
            "Dimensions": [dimensions_present] if dimensions_present else [[]],
            "Metrics": [{
                "Name": metric_name,
                "Unit": osb_unit_to_cloudwatch(doc.get("unit")),
                # 1s resolution so short benchmark windows graph as a
                # line at 1-second period instead of a single dot at
                # the default 60s standard-resolution bucket.
                "StorageResolution": 1,
            }],
        }],
    }
    return event


# EMF caps each MetricDirective at 100 metric definitions. NodeStats can
# easily exceed this, so we group by the first underscore-delimited prefix
# and spill overflows into additional directives within the same
# CloudWatchMetrics[] list of the same log event.
_MAX_METRICS_PER_DIRECTIVE = 100

# Undocumented per-event cap enforced by the CloudWatch EMF extractor:
# events declaring more than ~100 distinct metric Names (across ALL
# CloudWatchMetrics directives combined) get silently rejected — no
# metric is extracted, though the log line still lands in Logs. The
# public spec (docs.aws.amazon.com/.../CloudWatch_Embedded_Metric_Format_Specification.html)
# only advertises the 100/directive cap, so this is empirical. We split
# telemetry documents that exceed the cap into multiple log events
# (identical top-level identity + same Timestamp), each below the cap.
_MAX_METRICS_PER_EVENT = 100

# CloudWatch rejects metric names that contain characters outside
# [A-Za-z0-9_.-]. OpenSearch NodeStats flattening can produce names like
# ``jvm_buffer_pools_mapped - 'non-volatile memory'_count`` or
# ``jvm_gc_collectors_G1 Concurrent GC_collection_count`` — the space,
# apostrophe, and dash silently invalidate the whole EMF MetricDirective
# so *no* metric in that directive gets extracted. Sanitize before use.
_INVALID_METRIC_NAME_CHAR = re.compile(r"[^A-Za-z0-9_.\-]")


def _sanitize_metric_name(name: str) -> str:
    """Replace CW-invalid characters in a metric name with underscores."""
    return _INVALID_METRIC_NAME_CHAR.sub("_", name)

# Well-known fields injected by MetricsStore.put_doc (osbenchmark/metrics.py).
# These are identity/metadata strings (or the run-scoped dict), not numeric
# metric candidates, so the telemetry transform skips them when scanning the
# doc for metrics. Anything else that happens to be numeric becomes a metric.
_TELEMETRY_NON_METRIC_FIELDS = frozenset({
    "@timestamp", "relative-time-ms",
    "test-run-id", "test-run-timestamp",
    "environment", "workload", "test_procedure",
    "cluster-config-instance",
    "name",
    "meta", "workload-params",
    # MetricsStore.put_doc passes "task" / "operation" / "operation-type" /
    # "sample-type" through for non-telemetry callers; skip them as metric
    # candidates so they only contribute to dimensions / log fields.
    "task", "operation", "operation-type", "sample-type",
    "unit", "value",
})


def _telemetry_group_key(field_name: str) -> str:
    """First underscore-delimited segment of a flattened telemetry field
    name (``indices_segments_count`` -> ``indices``). Used to partition
    metric definitions into EMF MetricDirectives by subsystem prefix."""
    head, sep, _rest = field_name.partition("_")
    return (head or field_name) if sep else field_name


def build_telemetry_event(doc: Dict[str, Any], namespace: str) -> List[Dict[str, Any]]:
    """
    Build one or more EMF log events from a telemetry-style OSB metric document.

    Returns a **list** of events (rather than a single event) because
    CloudWatch's EMF metric-extractor enforces a per-log-event cap on the
    total number of distinct metrics ingested (empirically ~100 across
    ALL directives, not just per directive as the public spec suggests).
    NodeStats emits ~600 numeric fields; we therefore split the metric
    definitions across multiple log events sharing the same Timestamp
    and top-level identity/dimension fields.

    Telemetry devices fall into two shapes:

    * **Flattened** (NodeStats, IndexStats, etc.): ``{"name": ...,
      "indices_segments_count": 42, "jvm_mem_heap_used_percent": 73,
      ...}``. Numeric leaves become CloudWatch metrics grouped into
      ``CloudWatchMetrics`` directives by their first underscore-
      delimited prefix, then packed into events up to
      ``_MAX_METRICS_PER_EVENT`` metrics each.
    * **Nested** (RecoveryStats, ShardStats): ``{"name": ...,
      "shard": <dict>, ...}``. No numeric fields at the top level. We
      still emit one log event so the data is searchable via Logs
      Insights — the nested payload is serialized under a top-level
      key. No metrics are extracted.

    Each returned dict is ready to be ``json.dumps()`` and shipped as a
    separate PutLogEvents entry.
    """
    # Identify metric fields: numeric, non-bool, non-identity.
    metric_fields: List[str] = []
    nested_fields: List[str] = []
    for key, value in doc.items():
        if key in _TELEMETRY_NON_METRIC_FIELDS:
            continue
        if isinstance(value, bool):
            nested_fields.append(key)
            continue
        if isinstance(value, numbers.Real):
            metric_fields.append(key)
        else:
            # String / list / nested dict — keep as a top-level log field
            # so Logs Insights can query it even though it's not a metric.
            nested_fields.append(key)

    # OSB's @timestamp is already epoch ms; fall back to "now" if missing.
    timestamp = doc.get("@timestamp")
    if timestamp is None:
        timestamp = int(_time.time() * 1000)

    event: Dict[str, Any] = {}

    # Dimensions: same fixed set as the single-metric path. Telemetry events
    # typically only have Workload + SampleType (NodeStats etc. are not
    # request-scoped), but if a doc carries a task it gets dimensioned the
    # same way as a per-request sample.
    dimensions_present: List[str] = []
    for source_key, event_key in (
        ("workload", "Workload"),
        ("task", "Task"),
        ("operation-type", "OperationType"),
        ("sample-type", "SampleType"),
    ):
        value = doc.get(source_key)
        if value is not None:
            event[event_key] = value
            dimensions_present.append(event_key)

    # Top-level fields (queryable via Logs Insights, not dimensions).
    for source_key, event_key in (
        ("test-run-id", "TestRunId"),
        ("test-run-timestamp", "TestRunTimestamp"),
        ("environment", "Environment"),
        ("test_procedure", "TestProcedure"),
        ("cluster-config-instance", "ClusterConfigInstance"),
        ("operation", "Operation"),
        ("relative-time-ms", "RelativeTimeMs"),
        ("name", "Name"),  # e.g. "node-stats" — useful for filtering
    ):
        if doc.get(source_key) is not None:
            event[event_key] = doc[source_key]
    if doc.get("workload-params"):
        event["WorkloadParams"] = doc["workload-params"]

    meta = doc.get("meta")
    if isinstance(meta, dict):
        for key, value in meta.items():
            if value is None:
                continue
            event[f"meta.{key}"] = value

    # Copy non-metric / nested values through to the top level so they're
    # queryable via Logs Insights even though they don't become metrics.
    # Done before metric copy so a numeric field shadows a same-named
    # nested field — Python dict semantics make this deterministic.
    for field in nested_fields:
        event[field] = doc[field]

    # Copy the numeric metric values to the top level (EMF requirement
    # for the keys referenced from CloudWatchMetrics[].Metrics[].Name).
    # Field names produced by ``flatten_stats_fields`` are snake_case
    # while the run-identity fields populated above are PascalCase, so a
    # real collision is unreachable from the current telemetry code, but
    # writing this assignment LAST means a future namespace clash would
    # preserve the numeric metric value rather than the identity string.
    # Sanitize keys so the top-level name matches the sanitized metric
    # Name declared in _aws.CloudWatchMetrics[].Metrics[].
    for field in metric_fields:
        event[_sanitize_metric_name(field)] = doc[field]

    if not metric_fields:
        # Nested-only documents (e.g. RecoveryStats' {"shard": ...}): a
        # single log event with just the Timestamp — data is queryable via
        # Logs Insights but no metric is extracted.
        event["_aws"] = {"Timestamp": timestamp}
        return [event]

    # Group metric fields by prefix (indices_*, jvm_*, etc.) so a chunk
    # boundary aligns with a subsystem boundary when possible. Directives
    # are still capped at _MAX_METRICS_PER_DIRECTIVE per the public spec.
    groups: Dict[str, List[str]] = {}
    for field in metric_fields:
        groups.setdefault(_telemetry_group_key(field), []).append(field)

    dimensions_for_directive = [dimensions_present] if dimensions_present else [[]]

    # Build directives (each ≤ _MAX_METRICS_PER_DIRECTIVE metrics).
    directives: List[Dict[str, Any]] = []
    for _prefix in sorted(groups):
        group_fields = groups[_prefix]
        for chunk_start in range(0, len(group_fields), _MAX_METRICS_PER_DIRECTIVE):
            chunk = group_fields[chunk_start:chunk_start + _MAX_METRICS_PER_DIRECTIVE]
            directives.append({
                "Namespace": namespace,
                "Dimensions": dimensions_for_directive,
                "Metrics": [
                    {"Name": _sanitize_metric_name(f), "Unit": "None", "StorageResolution": 1}
                    for f in chunk
                ],
            })

    # Pack directives into events, respecting _MAX_METRICS_PER_EVENT (the
    # cap on total metric definitions per log event). Every event carries
    # the same top-level identity/dimension fields; only the
    # `_aws.CloudWatchMetrics` list — and consequently which top-level
    # numeric fields are present — varies per event.
    events: List[Dict[str, Any]] = []
    current_directives: List[Dict[str, Any]] = []
    current_metric_count = 0

    def _emit(directives_for_event: List[Dict[str, Any]]) -> None:
        # Collect the metric names in this event so we only include the
        # matching top-level numeric fields (avoids shipping ALL 596
        # numeric values in every one of the 6 sub-events; each event
        # only carries the values its directives reference).
        event_copy: Dict[str, Any] = {k: v for k, v in event.items()}
        names_in_event = {
            m["Name"] for d in directives_for_event for m in d["Metrics"]
        }
        # Trim to just the metric top-level keys used in this event's
        # directives. Identity/dimension fields already present in
        # event_copy are kept regardless.
        for field in metric_fields:
            sanitized = _sanitize_metric_name(field)
            if sanitized not in names_in_event:
                event_copy.pop(sanitized, None)
        event_copy["_aws"] = {
            "Timestamp": timestamp,
            "CloudWatchMetrics": directives_for_event,
        }
        events.append(event_copy)

    for directive in directives:
        d_count = len(directive["Metrics"])
        if current_directives and current_metric_count + d_count > _MAX_METRICS_PER_EVENT:
            _emit(current_directives)
            current_directives = []
            current_metric_count = 0
        current_directives.append(directive)
        current_metric_count += d_count
    if current_directives:
        _emit(current_directives)

    return events
