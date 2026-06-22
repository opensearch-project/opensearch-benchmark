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

Transforms the OSB metric document shape — produced by
``MetricsStore.put_value_*_level()`` and exposed to backends via
``MetricsStore._add(doc)`` — into an EMF log event with an
``_aws.CloudWatchMetrics`` block so CloudWatch Logs auto-extracts the
numeric value as a CloudWatch metric.

Pure transform — no I/O, no boto3 dependency. Unit-testable in isolation.

Telemetry payloads (``MetricsStore.put_doc(doc, ...)``) deliver flattened
``{prefix_field: value}`` dicts rather than the ``{name, value}`` pair handled
here; their multi-directive grouping is added in a subsequent commit.
"""
import logging
import numbers
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
    metric_name = doc["name"]
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
            }],
        }],
    }
    return event
