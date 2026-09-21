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
CloudWatch Logs Insights query helpers.

Wraps the boto3 ``logs.start_query`` → poll ``logs.get_query_results``
pattern in a single ``run_query`` call that returns a list of plain
Python dicts (one per result row, keyed by Insights field name).

Insights queries usually complete in 1–5 seconds; the caller blocks for
the duration. Tests stub ``_safe_sleep`` to avoid actual delays.
"""
import logging
import random
import time as _time
from typing import Any, Dict, List, Optional

import botocore.exceptions

from osbenchmark import exceptions


# Insights queries have a hard 60-minute server-side timeout. We cap the
# client-side poll well under that — a metrics-store read that takes a
# minute is unusable anyway; surface the timeout to the caller so
# upstream code can degrade gracefully (return empty results, fall back
# to file store, etc.).
_DEFAULT_POLL_TIMEOUT_SECONDS = 120
# 1s rather than 0.5s keeps several concurrent metric-store reads under
# CloudWatch's account-level GetQueryResults TPS limit (5 TPS in most
# regions). Typical Insights queries take 1–5s anyway; the extra 500ms of
# polling latency is irrelevant against that floor.
_POLL_INTERVAL_SECONDS = 1.0

# Terminal statuses from Logs Insights GetQueryResults. Note that
# ``Unknown`` is documented as "the status of the query is unknown" — it
# is a transient state (usually right after StartQuery, before the
# queryId propagates) rather than a terminal failure, so we treat it as
# "keep polling" alongside ``Scheduled`` / ``Running``.
_COMPLETE_STATUSES = frozenset({"Complete"})
_FAILED_STATUSES = frozenset({"Failed", "Cancelled", "Timeout"})

# Throttle/transient codes that warrant retry on get_query_results. The
# retry budget is small (a few hops) because the outer poll loop is
# already retry-shaped — we just want to swallow the occasional throttle
# rather than fail an entire run.
_RETRYABLE_THROTTLE_CODES = frozenset({
    "ThrottlingException", "Throttling",
    "TooManyRequestsException", "RequestLimitExceeded", "SlowDown",
    "ServiceUnavailableException", "InternalFailure", "InternalServerError",
})
_GET_RESULTS_THROTTLE_RETRIES = 3
_BACKOFF_BASE_SECONDS = 0.25


logger = logging.getLogger(__name__)


class InsightsQueryError(exceptions.BenchmarkError):
    """A CloudWatch Logs Insights query did not return Complete."""


def _safe_sleep(seconds: float) -> None:
    """Indirection point so tests can monkeypatch sleeping."""
    _time.sleep(seconds)


def run_query(logs_client, log_group: str, query: str,
              start_time: int, end_time: int,
              limit: int = 10_000,
              poll_timeout_seconds: int = _DEFAULT_POLL_TIMEOUT_SECONDS,
              ) -> List[Dict[str, Any]]:
    """
    Execute a Logs Insights query and return the rows as a list of dicts.

    :param logs_client: boto3 CloudWatch Logs client.
    :param log_group: Single log group name to query against. Insights
        also supports multiple groups via ``logGroupNames``, but the
        metrics store reads from one group at a time.
    :param query: The Insights query string (see
        https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html).
    :param start_time: Epoch seconds, lower bound of the search window.
    :param end_time: Epoch seconds, upper bound.
    :param limit: Maximum rows to return (Insights cap: 100,000).
    :param poll_timeout_seconds: Client-side cap on how long to wait for
        the server to finish. Raises ``InsightsQueryError`` on timeout.

    :return: List of dicts, one per row. Insights returns rows as a list
        of ``[{"field": <name>, "value": <stringified>}, ...]``; this
        function flattens that into ``{<name>: <value>}``.
    """
    # Insights rejects fractional seconds; coerce in case the caller
    # passed a float from time.time().
    start_time = int(start_time)
    end_time = int(end_time)

    try:
        start_response = logs_client.start_query(
            logGroupName=log_group,
            startTime=start_time,
            endTime=end_time,
            queryString=query,
            limit=limit,
        )
    except botocore.exceptions.ClientError as e:
        raise InsightsQueryError(
            f"start_query failed against {log_group!r}: {e}") from e
    query_id = start_response["queryId"]

    deadline = _time.monotonic() + poll_timeout_seconds
    while True:
        try:
            result = _get_results_with_retry(logs_client, query_id)
        except botocore.exceptions.ClientError as e:
            raise InsightsQueryError(
                f"get_query_results failed for {query_id}: {e}") from e
        status = result.get("status")
        if status in _COMPLETE_STATUSES:
            return _flatten_rows(result.get("results", []))
        if status in _FAILED_STATUSES:
            raise InsightsQueryError(
                f"Insights query {query_id} ended with status {status!r}")
        # Scheduled / Running / Unknown / anything else not-terminal: keep
        # polling. Unknown is documented as transient — usually clears on
        # the next call once the queryId has propagated.
        if _time.monotonic() >= deadline:
            # Best-effort cancellation so we don't leave the query running
            # server-side past the client's interest in the result. Swallow
            # all boto / network errors here — we're already raising one.
            try:
                logs_client.stop_query(queryId=query_id)
            except (botocore.exceptions.BotoCoreError,
                    botocore.exceptions.ClientError):
                logger.debug(
                    "Failed to stop_query %s after client timeout", query_id)
            raise InsightsQueryError(
                f"Insights query {query_id} did not complete within "
                f"{poll_timeout_seconds}s (last status: {status!r})")
        _safe_sleep(_POLL_INTERVAL_SECONDS)


def _get_results_with_retry(logs_client, query_id: str) -> Dict[str, Any]:
    """
    Wrap ``get_query_results`` with a small retry budget on throttle /
    transient-5xx codes so the outer poll loop doesn't fail mid-query
    when several stores read concurrently and bump up against
    CloudWatch's account-level GetQueryResults TPS limit.
    """
    attempt = 0
    while True:
        try:
            return logs_client.get_query_results(queryId=query_id)
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in _RETRYABLE_THROTTLE_CODES and attempt < _GET_RESULTS_THROTTLE_RETRIES:
                wait = _BACKOFF_BASE_SECONDS * (2 ** attempt)
                wait += random.uniform(0, _BACKOFF_BASE_SECONDS)
                _safe_sleep(wait)
                attempt += 1
                continue
            raise


def _flatten_rows(rows: List[List[Dict[str, str]]]) -> List[Dict[str, Any]]:
    """
    Translate Insights's row format into plain dicts.

    Insights returns each row as a list of ``{"field": ..., "value": ...}``
    pairs where ``value`` is always a string (even for numerics). This
    function flattens to ``{field: value}`` and leaves the caller to coerce
    types as needed — the metrics store knows which fields are numeric.

    The ``@ptr`` synthetic field that Insights includes per row is dropped
    so callers don't have to filter it out. Duplicate field names within
    a row are last-write-wins — Insights doesn't normally produce them,
    but a malformed query like ``stats avg(x) as v, max(x) as v`` would.
    """
    out: List[Dict[str, Any]] = []
    for row in rows:
        flat: Dict[str, Any] = {}
        for cell in row:
            field = cell.get("field")
            if field is None or field == "@ptr":
                continue
            flat[field] = cell.get("value")
        out.append(flat)
    return out


def to_float(value: Optional[str]) -> Optional[float]:
    """
    Best-effort coercion of an Insights string value to float. Returns
    ``None`` when the value is missing or unparseable so callers can use
    it in places like ``mapper=lambda doc: doc["value"]`` without having
    to wrap every access in a try/except.
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
