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
boto3 client construction and credential resolution for the CloudWatch
metrics store.

Builds CloudWatch Logs and CloudWatch (metrics) clients via boto3's default
credential provider chain, honoring the optional ``datastore.profile`` and
``datastore.role_arn`` config keys. Performs a ``sts:GetCallerIdentity``
startup probe so OSB can log which AWS account and identity it is writing
to before any data ships.
"""
import logging
from dataclasses import dataclass
from typing import Optional

import boto3
import botocore.exceptions

from osbenchmark import exceptions
from osbenchmark.metrics_stores.cloudwatch.config import CloudWatchConfig


@dataclass(frozen=True)
class CallerIdentity:
    """Result of the sts:GetCallerIdentity probe."""
    account: str
    arn: str
    user_id: str


class CloudWatchClientFactory:
    """
    Builds boto3 clients for the CloudWatch datastore.

    The factory is configured once per process from a CloudWatchConfig and
    produces ``logs`` and ``cloudwatch`` clients on demand. Credentials are
    resolved by boto3 at session-creation time using the default provider
    chain — env vars, profile, AssumeRole (via the configured profile's
    source_profile), web identity, SSO, shared credentials/config, container
    endpoint, then EC2 IMDS. ``datastore.profile`` and ``datastore.role_arn``
    override that resolution when set.

    Region is resolved from ``datastore.region`` first; if absent, boto3's
    region chain (``AWS_REGION`` → ``AWS_DEFAULT_REGION`` → profile region)
    applies.
    """

    def __init__(self, cw_config: CloudWatchConfig):
        self._config = cw_config
        self._logger = logging.getLogger(__name__)
        self._session = self._build_session()

    def _build_session(self) -> boto3.session.Session:
        session_kwargs = {}
        if self._config.profile:
            session_kwargs["profile_name"] = self._config.profile
        if self._config.region:
            session_kwargs["region_name"] = self._config.region

        try:
            session = boto3.session.Session(**session_kwargs)
        except botocore.exceptions.ProfileNotFound as e:
            raise exceptions.ConfigError(
                f"AWS profile {self._config.profile!r} not found in ~/.aws/config "
                f"or ~/.aws/credentials.") from e

        if self._config.role_arn:
            session = self._assume_role(session, self._config.role_arn)

        return session

    def _assume_role(self, source_session: boto3.session.Session, role_arn: str) -> boto3.session.Session:
        """
        Wrap ``source_session`` in one that assumes ``role_arn`` for every
        client call. Uses ``create_assume_role_refresher`` so credentials
        auto-rotate when the STS token expires — long-running benchmarks
        don't need to manually re-authenticate.

        Region falls back to the source session's resolved region when no
        ``datastore.region`` is configured, so a profile-supplied region
        isn't dropped on the assume-role hop.
        """
        # Inject credentials by replacing the underlying botocore session's
        # credential resolver. The `_credentials` attribute is the documented
        # private-but-stable extension point — `create_assume_role_refresher`
        # itself sets credentials this way internally.
        # pylint: disable=import-outside-toplevel
        from botocore.credentials import (
            DeferredRefreshableCredentials,
            create_assume_role_refresher,
        )
        from botocore.session import Session as BotocoreSession

        sts_client = source_session.client("sts")
        refresher = create_assume_role_refresher(
            sts_client,
            {
                "RoleArn": role_arn,
                "RoleSessionName": "opensearch-benchmark-cloudwatch",
            },
        )
        creds = DeferredRefreshableCredentials(
            method="sts-assume-role", refresh_using=refresher)
        botocore_session = BotocoreSession()
        botocore_session._credentials = creds  # pylint: disable=protected-access
        region = self._config.region or source_session.region_name
        if region:
            botocore_session.set_config_variable("region", region)
        return boto3.session.Session(botocore_session=botocore_session)

    def logs_client(self):
        """Return a boto3 CloudWatch Logs client."""
        return self._session.client("logs")

    def cloudwatch_client(self):
        """Return a boto3 CloudWatch (Metrics) client."""
        return self._session.client("cloudwatch")

    def probe_caller_identity(self) -> CallerIdentity:
        """
        Call sts:GetCallerIdentity and log which account / identity / region
        the datastore will write under. Surfaces friendly errors when
        credentials cannot be resolved at all (NoCredentialsError) or when
        the credentials boto3 found are invalid (ClientError).

        Returns the resolved CallerIdentity for callers that want to expose
        the values further (e.g. attach to metric metadata).
        """
        sts = self._session.client("sts")
        try:
            response = sts.get_caller_identity()
        except botocore.exceptions.NoCredentialsError as e:
            raise exceptions.ConfigError(
                "CloudWatch datastore: unable to resolve AWS credentials. Configure "
                "via ~/.aws/credentials, AWS_PROFILE, environment variables, or an "
                "instance/task IAM role.") from e
        except botocore.exceptions.PartialCredentialsError as e:
            raise exceptions.ConfigError(
                f"CloudWatch datastore: partial AWS credentials detected ({e}). "
                f"Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and "
                f"AWS_SESSION_TOKEN if using temporary creds), or switch to a "
                f"profile / IAM role.") from e
        except botocore.exceptions.NoRegionError as e:
            raise exceptions.ConfigError(
                "CloudWatch datastore: no AWS region configured. Set [reporting] "
                "datastore.region in benchmark.ini, or AWS_REGION / "
                "AWS_DEFAULT_REGION in the environment, or a region in the "
                "active AWS profile.") from e
        except botocore.exceptions.TokenRetrievalError as e:
            raise exceptions.ConfigError(
                f"CloudWatch datastore: AWS token retrieval failed ({e}). If "
                f"using IAM Identity Center / SSO, run `aws sso login` and try "
                f"again.") from e
        except botocore.exceptions.EndpointConnectionError as e:
            raise exceptions.ConfigError(
                f"CloudWatch datastore: cannot reach AWS STS ({e}). Check "
                f"network connectivity and any VPC endpoint configuration for "
                f"sts.{self._session.region_name or '<region>'}.amazonaws.com.") from e
        except botocore.exceptions.ClientError as e:
            raise exceptions.ConfigError(
                f"CloudWatch datastore: sts:GetCallerIdentity failed: {e}") from e

        identity = CallerIdentity(
            account=response["Account"],
            arn=response["Arn"],
            user_id=response["UserId"],
        )
        region = self._session.region_name or "<unresolved>"
        self._logger.info(
            "CloudWatch datastore: writing to account %s as %s in region %s",
            identity.account, identity.arn, region,
        )
        return identity

    @property
    def region(self) -> Optional[str]:
        """The region boto3 will use for clients built by this factory."""
        return self._session.region_name
