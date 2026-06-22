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
get_error_rate) currently return empty/zero values so the result-summary
path runs without crashing; CloudWatch Logs Insights wiring lands in a
later commit.
"""
import collections
import json
import os
import re

from osbenchmark import time
from osbenchmark.metrics import MetricsStore
from osbenchmark.metrics_stores.cloudwatch import emf
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

    Read methods inherited from ``MetricsStore`` currently return safe
    empty/zero values so the result-summary path
    (``test_run_orchestrator.calculate_results``) runs without crashing.
    Real Logs Insights queries land in a later commit; until then,
    percentile/stats reports for CloudWatch test runs will be blank.
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
        Transform an OSB metric document into an EMF event and buffer it.

        Flushes synchronously when the in-memory batch reaches CloudWatch's
        per-call limits so the writer never has to subdivide a single
        flush.
        """
        event = emf.build_event(doc, namespace=self._cw_config.namespace)
        if event is None:
            # Non-numeric value — emf.build_event already logged a warning.
            return
        message = json.dumps(event, separators=(",", ":"))
        encoded_size = len(message.encode("utf-8"))
        timestamp = event["_aws"]["Timestamp"]

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
        flush (or the disk-spool path in a later commit) can retry the
        same events rather than losing them.
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
            # Re-buffer so the events aren't lost. The disk-spool path in
            # a later commit can intercept here when persistent.
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
    # Stubbed to safe empty/zero values so result-summary code paths
    # (test_run_orchestrator.calculate_results → GlobalStatsCalculator)
    # don't crash for CloudWatch users until commit #11 wires Logs Insights.

    def _get(self, name, task, operation_type, sample_type, node_name, mapper):
        return []

    def get_error_rate(self, task, operation_type=None, sample_type=None):
        return 0.0

    def get_stats(self, name, task=None, operation_type=None, sample_type=None):
        return {"min": None, "max": None, "avg": None,
                "sum": None, "count": 0}

    def get_percentiles(self, name, task=None, operation_type=None,
                        sample_type=None, percentiles=None):
        # Mirror OsMetricsStore's "no hits" return value.
        return None

    def get_one(self, name, sample_type=None, node_name=None, task=None,
                mapper=lambda doc: doc["value"],
                sort_key=None, sort_reverse=False):
        return None

    def __str__(self):
        return "CloudWatch metrics store"
