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

Read paths (``list()`` / ``find_by_test_run_id``) are stubbed in this
commit and will be wired against CloudWatch Logs Insights in a later
commit; until then they return safe empty / not-found values so the
osbenchmark CLI surfaces (``list test-runs``, ``compare``) don't crash
for users on the CloudWatch backend.
"""
import json
import logging
import time as _time

from osbenchmark import exceptions
from osbenchmark.metrics import TestRunStore
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
    document via PutLogEvents synchronously — no batching, no spool,
    because writes happen at most once per benchmark run.
    """

    # Single shared stream per environment is fine: writes are
    # low-frequency (once per run) and CloudWatch Logs no longer requires
    # sequence-token coordination.
    # TODO(commit #12): honor self.environment_name when wiring Logs
    # Insights for list() / find_by_test_run_id().
    _STREAM_NAME = "test-runs"

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

    def list(self):
        # Insights-backed listing lands in commit #12.
        logger.warning(
            "CloudWatch test-run listing is not yet implemented; returning [].")
        return []

    def find_by_test_run_id(self, test_run_id):
        # Insights-backed lookup lands in commit #12.
        raise exceptions.NotFound(
            f"CloudWatch test-run lookup is not yet implemented; cannot "
            f"resolve test_run_id={test_run_id!r}.")


class FileBackedCompositeTestRunStore:
    """
    Hybrid test-run store used by ``datastore.type = cloudwatch`` until
    Logs Insights wiring (commit #12) lands.

    Writes fan out to BOTH the CloudWatch store (ships to AWS) and the
    file store (persists locally). Reads come from the FILE store so that
    ``osbenchmark list test-runs``, ``compare``, and ``aggregate`` keep
    working against the local on-disk records while the cloudwatch read
    path is still stubbed.

    This is intentionally distinct from
    ``osbenchmark.metrics.CompositeTestRunStore`` (which reads from the
    OpenSearch store). When commit #12 wires up Insights-backed reads,
    the cloudwatch backend can swap to the existing CompositeTestRunStore
    with CW as the read source.
    """

    def __init__(self, cloudwatch_store: "CloudWatchTestRunStore", file_store):
        self._cw_store = cloudwatch_store
        self._file_store = file_store

    def find_by_test_run_id(self, test_run_id):
        return self._file_store.find_by_test_run_id(test_run_id)

    def store_test_run(self, test_run):
        self._file_store.store_test_run(test_run)
        self._cw_store.store_test_run(test_run)

    def store_html_results(self, test_run):
        self._file_store.store_html_results(test_run)

    def list(self):
        return self._file_store.list()
