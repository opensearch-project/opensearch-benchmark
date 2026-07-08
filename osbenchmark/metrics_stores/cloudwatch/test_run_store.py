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
CloudWatchTestRunStore — persists test-run documents to the configured
CloudWatch Logs group for test runs (see config key
``datastore.log_group.test_runs``) as plain JSON log events with no EMF
metric extraction.

Mirrors the responsibilities of OsTestRunStore. Intended to be wrapped by
CompositeTestRunStore alongside FileTestRunStore so local files continue to
work even when CloudWatch shipping is configured.

``list()`` and ``find_by_test_run_id`` query the test-runs log group via
CloudWatch Logs Insights and deserialize the stored JSON back into the
canonical ``TestRun`` shape consumers expect.
"""
import json
import logging
import time as _time

from osbenchmark import exceptions
from osbenchmark.metrics import TestRun, TestRunStore
from osbenchmark.metrics_stores.cloudwatch import insights
from osbenchmark.metrics_stores.cloudwatch.client import CloudWatchClientFactory
from osbenchmark.metrics_stores.cloudwatch.config import (
    CloudWatchConfig,
    load as load_cw_config,
)
from osbenchmark.metrics_stores.cloudwatch.log_streams import (
    LogStreamWriter,
    ensure_log_group,
    ensure_log_stream,
)


logger = logging.getLogger(__name__)


class CloudWatchTestRunStore(TestRunStore):
    """
    Test-run store that writes one JSON log event per test run to a
    CloudWatch Logs group.

    Unlike the metrics path, each store_test_run call ships a single small
    document via PutLogEvents synchronously — no batching, because writes
    happen at most once per benchmark run.
    """

    # Single shared stream per environment is fine: writes are
    # low-frequency (once per run) and CloudWatch Logs no longer requires
    # sequence-token coordination.
    _STREAM_NAME = "test-runs"

    # Insights query covers up to this far in the past for `list()`. The
    # CloudWatch Logs metrics retention default (commit #3 config) is
    # 30 days, so a 90-day window happily covers everything that still
    # exists while keeping the query cheap.
    _LIST_WINDOW_DAYS = 90

    def __init__(self, cfg,
                 client_factory_class=CloudWatchClientFactory,
                 config_loader=load_cw_config):
        """
        :param cfg: OSB Config object.
        :param client_factory_class: Optional override for tests.
        :param config_loader: Optional override for tests.
        """
        super().__init__(cfg)
        self._cw_config: CloudWatchConfig = config_loader(cfg)
        self._client_factory = client_factory_class(self._cw_config)
        # Provision the log group / stream on first write. Read-only
        # callers (list / find) never trigger an AWS write.
        self._writer = None

    def _ensure_writer(self) -> LogStreamWriter:
        if self._writer is not None:
            return self._writer
        # Probe caller identity once for parity with CloudWatchMetricsStore —
        # surfaces friendly errors for missing creds / region before the
        # first CreateLogGroup call (which gives a less actionable boto
        # error).
        self._client_factory.probe_caller_identity()
        logs_client = self._client_factory.logs_client()
        ensure_log_group(
            logs_client,
            self._cw_config.test_runs_log_group,
            retention_days=self._cw_config.log_retention_days)
        ensure_log_stream(
            logs_client,
            self._cw_config.test_runs_log_group,
            self._STREAM_NAME)
        self._writer = LogStreamWriter(
            logs_client,
            self._cw_config.test_runs_log_group,
            self._STREAM_NAME)
        return self._writer

    def store_test_run(self, test_run) -> None:
        writer = self._ensure_writer()
        doc = test_run.as_dict()
        message = json.dumps(doc, separators=(",", ":"))
        timestamp_ms = int(_time.time() * 1000)
        writer.write_batch([{"timestamp": timestamp_ms, "message": message}])
        logger.info(
            "CloudWatch datastore: stored test run %s to %s",
            getattr(test_run, "test_run_id", "<unknown>"),
            self._cw_config.test_runs_log_group,
        )

    def _logs_client(self):
        """Lazy logs client for read paths (no STS probe — reads only)."""
        return self._client_factory.logs_client()

    def list(self):
        """
        Return the most recent test-run documents for this environment.

        Wraps a Logs Insights query that fetches the stored ``@message``
        for every log event in the test-runs log group filtered by
        ``environment``, then deserialises each JSON message back into a
        ``TestRun`` via the existing ``from_dict`` constructor.
        """
        end = int(_time.time())
        start = end - (self._LIST_WINDOW_DAYS * 86400)
        env_filter = _escape(self.environment_name)
        # CloudWatch Logs Insights auto-discovers top-level JSON keys, so
        # we can filter on `environment` directly without an explicit
        # `parse @message ...` directive. TestRun.as_dict (metrics.py)
        # emits `environment` as a top-level key in the stored event.
        query = (
            f'fields @message, @timestamp\n'
            f'| filter environment = "{env_filter}"\n'
            f'| sort @timestamp desc\n'
            f'| limit {self._max_results()}'
        )
        rows = insights.run_query(
            self._logs_client(),
            self._cw_config.test_runs_log_group,
            query, start, end, limit=self._max_results())
        return [tr for tr in (_parse_test_run(row.get("@message")) for row in rows) if tr is not None]

    def find_by_test_run_id(self, test_run_id):
        """
        Fetch a single test-run by id. Uses a tighter (7 day) window
        than ``list`` because finding a specific run is usually about a
        recent one. Falls through to the wider ``_LIST_WINDOW_DAYS``
        window on miss so a long-ago run is still discoverable.
        """
        for window_days in (7, self._LIST_WINDOW_DAYS):
            end = int(_time.time())
            start = end - (window_days * 86400)
            safe_id = _escape(test_run_id)
            query = (
                f'fields @message\n'
                f'| filter `test-run-id` = "{safe_id}"\n'
                f'| limit 1'
            )
            rows = insights.run_query(
                self._logs_client(),
                self._cw_config.test_runs_log_group,
                query, start, end, limit=1,
                # Tighter poll timeout than the default — find is
                # interactive (osbenchmark compare uses it inline) and
                # 30s is enough to fail fast on a miss without cutting
                # off legitimately slow Insights scheduling.
                poll_timeout_seconds=30)
            if rows:
                parsed = _parse_test_run(rows[0].get("@message"))
                if parsed is not None:
                    return parsed
        # Match OsTestRunStore's exact wording (metrics.py:1749) so log
        # scrapers / external callers can match the same string across
        # both backends.
        raise exceptions.NotFound(
            "No test_run with test_run id [{}]".format(test_run_id))


class FileBackedCompositeTestRunStore:
    """
    Hybrid test-run store used by ``datastore.type = cloudwatch`` that
    fans writes out to BOTH the CloudWatch store and the file store, and
    falls back to the local file store for reads when CloudWatch returns
    nothing or errors out.

    Why fallback rather than pure CloudWatch reads: a freshly-shipped
    test run is not immediately visible to Logs Insights (CW Logs has a
    several-second ingest delay before queries see new events), so
    ``osbenchmark compare $JUST_FINISHED_RUN`` would intermittently fail
    if we relied on CloudWatch alone. The file store always has the
    just-stored record, so we consult it first and use CloudWatch as the
    historical / cross-host backstop.
    """

    def __init__(self, cloudwatch_store: "CloudWatchTestRunStore", file_store):
        self._cw_store = cloudwatch_store
        self._file_store = file_store

    def find_by_test_run_id(self, test_run_id):
        try:
            return self._file_store.find_by_test_run_id(test_run_id)
        except exceptions.NotFound:
            return self._cw_store.find_by_test_run_id(test_run_id)

    def store_test_run(self, test_run):
        self._file_store.store_test_run(test_run)
        self._cw_store.store_test_run(test_run)

    def store_html_results(self, test_run):
        self._file_store.store_html_results(test_run)

    def list(self):
        # Local file is the source of truth for short-term history; the
        # cloudwatch store is consulted for runs the local box never saw
        # (e.g. cross-host benchmarking, recovery after laptop reformat).
        file_runs = self._file_store.list()
        file_ids = {run.test_run_id for run in file_runs}
        try:
            cw_runs = [run for run in self._cw_store.list()
                       if run.test_run_id not in file_ids]
        except Exception as e:  # noqa: BLE001 — see comment below
            # Catch broadly: InsightsQueryError is the expected case, but
            # boto3 ClientError (AccessDenied, ProfileNotFound,
            # ExpiredToken) and credential-resolution errors come from
            # building the boto3 client itself. The docstring promises
            # graceful degradation, so a missing-permissions case should
            # NOT break a previously file-only-working `list test-runs`.
            logger.warning(
                "CloudWatch test-run listing failed (%s); falling back to "
                "file-store results only.", e)
            cw_runs = []
        return file_runs + cw_runs


def _escape(value) -> str:
    """
    Sanitize values interpolated into Logs Insights query string
    literals. CloudWatch Logs Insights wraps string literals in double
    quotes; backticks separately delimit field names. Replacing both
    with underscores prevents accidentally breaking out of the literal
    when a user-supplied environment name or test-run-id contains them.
    Real test_run_ids are UUIDs, but defense in depth is cheap.
    """
    return str(value).replace('"', "_").replace("`", "_")


def _parse_test_run(message):
    """Deserialise a stored test-run JSON log message back into TestRun.

    Returns None if the message cannot be parsed — a malformed line in
    the log group shouldn't crash a list / find call.
    """
    if not message:
        return None
    try:
        doc = json.loads(message)
    except (TypeError, ValueError):
        logger.warning(
            "CloudWatch test-run store: skipping unparseable log event")
        return None
    try:
        return TestRun.from_dict(doc)
    except Exception:  # noqa: BLE001 — TestRun.from_dict may raise various
        logger.warning(
            "CloudWatch test-run store: skipping malformed test-run doc")
        return None
