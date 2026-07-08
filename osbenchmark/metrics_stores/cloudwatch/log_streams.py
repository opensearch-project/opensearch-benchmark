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
Idempotent provisioning of CloudWatch log groups / streams plus the
LogStreamWriter that ships pre-formatted JSON log events to one stream
via PutLogEvents.

The writer handles CloudWatch's batch limits (10,000 events, 1 MiB
payload, single event 1 MiB) by chunking the caller's batch as needed,
and retries on ThrottlingException with exponential backoff. Sequence
tokens are no longer required as of CloudWatch Logs' 2023 deprecation, so
multiple workers can write to distinct streams under the same log group
without coordination.
"""
import logging
import random
import time as _time
from typing import Iterable, List, Optional, Sequence

import botocore.exceptions


# CloudWatch PutLogEvents limits (see cloudwatch-apis.md for sources).
_MAX_EVENTS_PER_BATCH = 10_000
_MAX_BATCH_BYTES = 1_048_576
_PER_EVENT_OVERHEAD_BYTES = 26
# CloudWatch rejects single log events larger than this.
_MAX_EVENT_BYTES = 1_048_576 - _PER_EVENT_OVERHEAD_BYTES

# Retry policy for transient errors. Throttling is the common case; back off
# exponentially with jitter and cap the wait so a stuck run still progresses.
_MAX_RETRIES = 5
_BASE_BACKOFF_SECONDS = 0.5
_MAX_BACKOFF_SECONDS = 30.0


# Throttle / transient 5xx error codes that warrant exponential backoff.
# Codes vary by AWS subsystem (e.g. CW Logs uses ThrottlingException, STS
# uses Throttling, regional gateways use SlowDown), so we match against a
# set rather than naming a single code.
_RETRYABLE_THROTTLE_CODES = frozenset({
    "ThrottlingException",
    "Throttling",
    "TooManyRequestsException",
    "RequestLimitExceeded",
    "SlowDown",
    "ServiceUnavailableException",
    "InternalFailure",
    "InternalServerError",
})

# Credential-rotation race codes. boto3 owns the refresh and a follow-up
# call typically succeeds, so we retry these exactly once before bubbling up.
_TRANSIENT_AUTH_CODES = frozenset({
    "ExpiredTokenException",
    "ExpiredToken",
    "InvalidSignatureException",
    "InvalidClientTokenId",
})


def _safe_sleep(seconds: float) -> None:
    """Indirection point so tests can monkeypatch sleeping."""
    _time.sleep(seconds)


def ensure_log_group(logs_client, log_group: str,
                     retention_days: Optional[int] = None) -> None:
    """
    Create ``log_group`` if it does not exist and apply ``retention_days`` if
    supplied. Both operations are idempotent — existing groups are not
    modified beyond setting the retention policy.

    Note: ``retention_days`` is applied on every call. Last-writer wins if
    multiple workers / runs disagree on the value, but the config layer
    (commit #3) already validates against the CloudWatch retention enum so
    the value is at least always a legal one.
    """
    logger = logging.getLogger(__name__)
    try:
        logs_client.create_log_group(logGroupName=log_group)
        logger.info("Created CloudWatch log group %s", log_group)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        logger.debug("CloudWatch log group %s already exists", log_group)

    if retention_days is not None:
        logs_client.put_retention_policy(
            logGroupName=log_group, retentionInDays=int(retention_days))
        logger.debug("Set retention on %s to %d days", log_group, retention_days)


def ensure_log_stream(logs_client, log_group: str, log_stream: str) -> None:
    """
    Create ``log_stream`` under ``log_group`` if it does not already exist.
    """
    try:
        logs_client.create_log_stream(
            logGroupName=log_group, logStreamName=log_stream)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass


class LogStreamWriter:
    """
    Thin PutLogEvents writer for a single (log_group, log_stream) pair.

    Caller responsibilities:
    - Pass already-JSON-encoded ``message`` strings via ``write_batch``.
    - Supply each event's timestamp as epoch milliseconds (matches EMF's
      ``_aws.Timestamp`` field).

    Writer responsibilities:
    - Sort the batch into chronological order (CloudWatch rejects
      out-of-order batches).
    - Chunk the batch to respect per-call event count / payload byte limits.
    - Retry transient throttles with exponential backoff + jitter.
    - Surface credential / endpoint / permission failures unchanged so the
      enclosing store can decide how to react.

    Sequence tokens are intentionally NOT tracked: as of the 2023 CloudWatch
    Logs deprecation they are no longer required, and boto3 >= 1.34 accepts
    PutLogEvents without one.
    """

    def __init__(self, logs_client, log_group: str, log_stream: str):
        self._client = logs_client
        self._log_group = log_group
        self._log_stream = log_stream
        self._logger = logging.getLogger(__name__)

    def write_batch(self, events: Sequence[dict]) -> int:
        """
        Ship ``events`` to CloudWatch Logs.

        :param events: Sequence of ``{"timestamp": <epoch_ms>, "message": <str>}``
            dicts. The list is sorted in place by timestamp before chunking.
        :return: Number of events actually sent. Events whose ``message``
            exceeds CloudWatch's per-event byte limit are dropped with a
            warning rather than failing the whole batch.
        """
        if not events:
            return 0

        # CloudWatch requires events in a batch to be sorted chronologically.
        ordered = sorted(events, key=lambda e: e["timestamp"])

        sent = 0
        for chunk in self._chunk(ordered):
            self._put_with_retry(chunk)
            sent += len(chunk)
        return sent

    def _chunk(self, events: Iterable[dict]) -> Iterable[List[dict]]:
        """
        Split ``events`` into batches that respect CloudWatch's per-call
        size and count caps. Drops oversized single events with a warning.
        """
        batch: List[dict] = []
        batch_bytes = 0
        for event in events:
            message = event["message"]
            encoded_size = len(message.encode("utf-8")) + _PER_EVENT_OVERHEAD_BYTES
            if encoded_size > _MAX_EVENT_BYTES:
                # No way to subdivide a single message; surface and skip.
                self._logger.warning(
                    "CloudWatch event of %d bytes exceeds the %d-byte limit "
                    "and was dropped (log group %s)",
                    encoded_size, _MAX_EVENT_BYTES, self._log_group)
                continue
            if (len(batch) >= _MAX_EVENTS_PER_BATCH
                    or batch_bytes + encoded_size > _MAX_BATCH_BYTES):
                yield batch
                batch = []
                batch_bytes = 0
            batch.append(event)
            batch_bytes += encoded_size
        if batch:
            yield batch

    def _put_with_retry(self, batch: List[dict]) -> None:
        # Per-class retry budgets so a transient stream-recreate doesn't
        # consume the throttle budget (and vice versa). RNF and auth races
        # are allowed exactly one retry each; throttles use the larger
        # exponential-backoff budget.
        throttle_attempts = 0
        rnf_attempts = 0
        auth_attempts = 0
        while True:
            try:
                self._client.put_log_events(
                    logGroupName=self._log_group,
                    logStreamName=self._log_stream,
                    logEvents=batch,
                )
                return
            except self._client.exceptions.ResourceNotFoundException as e:
                # Stream got reaped (e.g. operator deleted it). Recreate and
                # retry once. ensure_log_stream is idempotent. If the log
                # *group* was also deleted, ensure_log_stream surfaces the
                # underlying RNF — wrap with a clearer message so the
                # operator sees the actual cause.
                if rnf_attempts >= 1:
                    raise
                rnf_attempts += 1
                try:
                    ensure_log_stream(
                        self._client, self._log_group, self._log_stream)
                except self._client.exceptions.ResourceNotFoundException as inner:
                    raise RuntimeError(
                        f"CloudWatch log group {self._log_group!r} appears "
                        f"to have been deleted: {inner}"
                    ) from e
                continue
            except botocore.exceptions.ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in _RETRYABLE_THROTTLE_CODES:
                    if throttle_attempts >= _MAX_RETRIES:
                        raise
                    self._backoff(throttle_attempts)
                    throttle_attempts += 1
                    continue
                # Credential races (refresh happened mid-flight): retry ONCE
                # with no real wait — boto3 owns the refresh and the next
                # call synchronously fetches fresh creds.
                if code in _TRANSIENT_AUTH_CODES and auth_attempts < 1:
                    auth_attempts += 1
                    continue
                raise

    @staticmethod
    def _backoff(attempt: int) -> None:
        wait = min(
            _MAX_BACKOFF_SECONDS,
            _BASE_BACKOFF_SECONDS * (2 ** attempt),
        )
        # Jitter prevents synchronised retries from N workers thundering at once.
        wait += random.uniform(0, _BASE_BACKOFF_SECONDS)
        _safe_sleep(wait)
