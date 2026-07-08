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

"""Shared fixtures for the CloudWatch datastore test suite.

These fakes stand in for boto3 wherever the production code would
otherwise hit AWS. Each test gets a fresh ``FakeLogsClient`` so state
between tests stays isolated.
"""
import pytest

import osbenchmark.metrics_stores.cloudwatch.insights as insights_mod
import osbenchmark.metrics_stores.cloudwatch.log_streams as log_streams_mod
from osbenchmark.metrics_stores.cloudwatch.config import CloudWatchConfig


class _ResourceAlreadyExists(Exception):
    pass


class _ResourceNotFound(Exception):
    pass


class FakeLogsClient:
    """Minimal stand-in for the boto3 logs client used across tests.

    Mirrors the surface that ``log_streams`` / ``insights`` call:
    create_log_group, create_log_stream, put_retention_policy,
    put_log_events, start_query, get_query_results, stop_query. Tests
    configure failure injection via ``fail_with`` / ``fail_count`` and
    read the captured AWS calls from public attributes.
    """

    def __init__(self):
        self.put_calls = []
        self.created_groups = set()
        self.created_streams = set()
        self.retention = {}
        self.start_query_calls = []
        self.stop_query_calls = []
        # Failure injection
        self.fail_count = 0
        self.fail_with = None
        # Insights state
        self._next_results = []
        self._status_sequence = ["Complete"]
        self._get_calls = 0

        class _Exc:
            ResourceAlreadyExistsException = _ResourceAlreadyExists
            ResourceNotFoundException = _ResourceNotFound

        self.exceptions = _Exc

    # ----- Log group / stream provisioning -----
    def create_log_group(self, logGroupName):
        if logGroupName in self.created_groups:
            raise _ResourceAlreadyExists(logGroupName)
        self.created_groups.add(logGroupName)

    def create_log_stream(self, logGroupName, logStreamName):
        key = (logGroupName, logStreamName)
        if key in self.created_streams:
            raise _ResourceAlreadyExists(key)
        self.created_streams.add(key)

    def put_retention_policy(self, logGroupName, retentionInDays):
        self.retention[logGroupName] = retentionInDays

    # ----- PutLogEvents -----
    def put_log_events(self, **kw):
        if self.fail_count > 0:
            self.fail_count -= 1
            if self.fail_with is not None:
                raise self.fail_with
        self.put_calls.append(kw)

    # ----- Logs Insights -----
    def queue_query_results(self, rows, status_sequence=None):
        """Configure the rows the next start_query will return."""
        if status_sequence:
            self._status_sequence = list(status_sequence)
        else:
            self._status_sequence = ["Complete"]
        self._next_results = rows
        self._get_calls = 0

    def start_query(self, **kw):
        self.start_query_calls.append(kw)
        return {"queryId": "q-test"}

    def get_query_results(self, queryId):
        self._get_calls += 1
        idx = min(self._get_calls - 1, len(self._status_sequence) - 1)
        status = self._status_sequence[idx]
        return {
            "status": status,
            "results": self._next_results if status == "Complete" else [],
        }

    def stop_query(self, **kw):
        self.stop_query_calls.append(kw)


@pytest.fixture
def fake_logs_client():
    """A fresh FakeLogsClient for each test."""
    return FakeLogsClient()


@pytest.fixture(autouse=True)
def disable_sleeps(monkeypatch):
    """Don't actually sleep in retry / poll loops during tests."""
    monkeypatch.setattr(log_streams_mod, "_safe_sleep", lambda s: None)
    monkeypatch.setattr(insights_mod, "_safe_sleep", lambda s: None)


@pytest.fixture
def cw_config():
    """Default-but-customisable :class:`CloudWatchConfig` for tests."""
    return CloudWatchConfig(
        region="us-east-1",
        namespace="OSB",
        metrics_log_group="benchmark-metrics",
        test_runs_log_group="benchmark-test-runs",
        results_log_group="benchmark-results",
        log_retention_days=None,
        profile=None,
        role_arn=None,
    )


def make_insights_rows(rows):
    """Translate a list of dicts to Insights's [{field,value}] shape."""
    return [
        [{"field": k, "value": str(v)} for k, v in row.items()]
        for row in rows
    ]


def make_client_error(code, op="PutLogEvents"):
    import botocore.exceptions
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "test"}}, op)
