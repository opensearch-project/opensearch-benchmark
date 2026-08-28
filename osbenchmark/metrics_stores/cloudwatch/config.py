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
Parsing and validation of the ``[reporting]`` keys consumed by the
CloudWatch metrics store.

The CloudWatch backend reads the same ``[reporting]`` section of
``~/.osb/benchmark.ini`` that the OpenSearch backend reads, but a disjoint
subset of keys. All keys live under ``datastore.*`` (existing convention).
"""
from dataclasses import dataclass
from typing import Optional

from osbenchmark import exceptions


# CloudWatch Logs PutRetentionPolicy accepts only this fixed enum (days).
# Source: https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutRetentionPolicy.html
_VALID_RETENTION_DAYS = frozenset({
    1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180,
    365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653,
})


@dataclass(frozen=True)
class CloudWatchConfig:
    """Resolved CloudWatch datastore configuration."""

    region: Optional[str]
    namespace: str
    metrics_log_group: str
    test_runs_log_group: str
    results_log_group: str
    log_retention_days: Optional[int]
    profile: Optional[str]
    role_arn: Optional[str]


def load(cfg) -> CloudWatchConfig:
    """
    Read CloudWatch datastore options from the ``[reporting]`` section.

    :param cfg: The OSB Config object.
    :raises exceptions.ConfigError: if a required option is missing.
    :raises exceptions.SystemSetupError: if a value is invalid (e.g. an
        unsupported retention period).
    """
    # region is optional here: boto3 resolves it from AWS_REGION /
    # AWS_DEFAULT_REGION / the active profile / IMDS in that order. We only
    # surface a clear error in client.py if boto3 can't find one.
    region = _opt_str(cfg, "datastore.region", default=None)
    namespace = _opt_str(cfg, "datastore.namespace", default="OSB")

    metrics_log_group = _opt_str(
        cfg, "datastore.log_group.metrics", default="benchmark-metrics")
    test_runs_log_group = _opt_str(
        cfg, "datastore.log_group.test_runs", default="benchmark-test-runs")
    results_log_group = _opt_str(
        cfg, "datastore.log_group.results", default="benchmark-results")

    retention_raw = cfg.opts("reporting", "datastore.log_retention_days",
                             default_value=None, mandatory=False)
    log_retention_days = _validate_retention(retention_raw)

    profile = _opt_str(cfg, "datastore.profile", default=None)
    role_arn = _opt_str(cfg, "datastore.role_arn", default=None)

    return CloudWatchConfig(
        region=region,
        namespace=namespace,
        metrics_log_group=metrics_log_group,
        test_runs_log_group=test_runs_log_group,
        results_log_group=results_log_group,
        log_retention_days=log_retention_days,
        profile=profile,
        role_arn=role_arn,
    )


def _opt_str(cfg, key: str, default) -> Optional[str]:
    value = cfg.opts("reporting", key, default_value=default, mandatory=False)
    if value is None:
        return None
    return str(value).strip()


def _validate_retention(raw) -> Optional[int]:
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        return None
    try:
        days = int(raw)
    except (TypeError, ValueError):
        raise exceptions.SystemSetupError(
            f"[reporting] datastore.log_retention_days must be an integer, got {raw!r}.")
    if days not in _VALID_RETENTION_DAYS:
        valid = ", ".join(str(d) for d in sorted(_VALID_RETENTION_DAYS))
        raise exceptions.SystemSetupError(
            f"[reporting] datastore.log_retention_days={days} is not accepted by "
            f"CloudWatch Logs. Valid values: {valid}.")
    return days
