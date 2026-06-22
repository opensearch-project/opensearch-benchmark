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
CloudWatchResultsStore — persists the aggregated results block produced by
GlobalStatsCalculator to the configured CloudWatch Logs group for results
(see config key ``datastore.log_group.results``) as plain JSON log
events.

Mirrors the responsibilities of OsResultsStore. Used by `osbenchmark
compare` and the summary reporting path.
"""
import json
import logging
import time as _time

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


class CloudWatchResultsStore:
    """
    Results store that writes one JSON log event per (test run, result
    record) tuple to a CloudWatch Logs group.

    OsResultsStore explodes the aggregate results into multiple result
    documents via ``test_run.to_result_dicts()``; we do the same and ship
    them as a single PutLogEvents batch since the total volume per run is
    small (a few dozen records).
    """

    _STREAM_NAME = "results"

    def __init__(self, cfg,
                 client_factory_class=CloudWatchClientFactory,
                 config_loader=load_cw_config):
        self._cfg = cfg
        self._cw_config: CloudWatchConfig = config_loader(cfg)
        self._client_factory = client_factory_class(self._cw_config)
        self._writer = None

    def _ensure_writer(self) -> LogStreamWriter:
        if self._writer is not None:
            return self._writer
        # Probe caller identity once for parity with CloudWatchMetricsStore.
        self._client_factory.probe_caller_identity()
        logs_client = self._client_factory.logs_client()
        ensure_log_group(
            logs_client,
            self._cw_config.results_log_group,
            retention_days=self._cw_config.log_retention_days)
        ensure_log_stream(
            logs_client,
            self._cw_config.results_log_group,
            self._STREAM_NAME)
        self._writer = LogStreamWriter(
            logs_client,
            self._cw_config.results_log_group,
            self._STREAM_NAME)
        return self._writer

    def store_results(self, test_run) -> None:
        timestamp_ms = int(_time.time() * 1000)
        events = []
        for record in test_run.to_result_dicts():
            events.append({
                "timestamp": timestamp_ms,
                "message": json.dumps(record, separators=(",", ":")),
            })
        if not events:
            logger.debug(
                "CloudWatch datastore: no result records to ship for test "
                "run %s", getattr(test_run, "test_run_id", "<unknown>"))
            return
        writer = self._ensure_writer()
        writer.write_batch(events)
        logger.info(
            "CloudWatch datastore: shipped %d result records for test run "
            "%s to %s",
            len(events),
            getattr(test_run, "test_run_id", "<unknown>"),
            self._cw_config.results_log_group,
        )
