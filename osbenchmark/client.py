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

import io
import logging
import time
import urllib3
import boto3

import aiohttp
import certifi
from opensearchpy import AsyncOpenSearch, OpenSearch, Urllib3HttpConnection
from opensearchpy.serializer import JSONSerializer
from urllib3.util.ssl_ import is_ipaddress
from botocore.session import get_session
from botocore.credentials import RefreshableCredentials
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from osbenchmark.async_connection import AsyncHttpConnection
from osbenchmark.kafka_client import KafkaMessageProducer

from osbenchmark import exceptions
from osbenchmark.context import RequestContextHolder
from osbenchmark.utils import console, convert

class PerRequestSigV4:
    """
    Sync HTTP auth matching the 3-arg signature (method, url, body).

    Uses botocore RefreshableCredentials under the hood: each call to
    `get_frozen_credentials()` checks expiry and triggers a refresh
    callback if the credentials are stale or near expiry, ensuring
    every HTTP request is signed with valid SigV4 credentials.
    """
    def __init__(self, botocore_session, service, region):
        self.bc_session = botocore_session
        self.service = service
        self.region = region

    def __call__(self, method, url, body):
        # `get_frozen_credentials()` will auto-refresh if creds are expired or close to expiry
        creds = self.bc_session.get_credentials().get_frozen_credentials()
        aws_req = AWSRequest(method=method, url=url, data=body, headers={})
        SigV4Auth(creds, self.service, self.region).add_auth(aws_req)
        return dict(aws_req.headers.items())

class PerRequestSigV4Async:
    """
    Async HTTP auth matching the 4-arg signature
    (method: str, url: str, raw_query_string: str, body: bytes) -> dict
    used by AIOHTTPConnection.perform_request.

    raw_query_string is the URL-encoded query portion of the request,
    e.g. "foo=bar&baz=qux" or an empty string if no query params.
    """
    def __init__(self, botocore_session, service, region):
        self.bc_session = botocore_session
        self.service = service
        self.region = region

    def __call__(self, method: str, url: str, raw_query_string: str, body: bytes) -> dict:
        # Grab frozen credentials; auto-refresh under the hood
        creds = self.bc_session.get_credentials().get_frozen_credentials()

        # Parse the raw query string into key/value tuples, preserving blank values
        params = []
        if raw_query_string:
            from urllib.parse import parse_qsl
            params = parse_qsl(raw_query_string, keep_blank_values=True)

        # Build the AWSRequest for signing
        aws_req = AWSRequest(
            method=method,
            url=url,
            params=params,
            data=body,
            headers={},
        )

        # Sign with SigV4
        SigV4Auth(creds, self.service, self.region).add_auth(aws_req)

        # Return signed headers as a simple dict
        return dict(aws_req.headers.items())

class LazyJSONSerializer(JSONSerializer):
    def loads(self, s):
        meta = BenchmarkAsyncOpenSearch.request_context.get()
        if "raw_response" in meta:
            return io.BytesIO(s)
        return super().loads(s)

async def on_request_start(session, trace_config_ctx, params):
    BenchmarkAsyncOpenSearch.on_request_start()

async def on_request_end(session, trace_config_ctx, params):
    BenchmarkAsyncOpenSearch.on_request_end()

trace_config = aiohttp.TraceConfig()
trace_config.on_request_start.append(on_request_start)
trace_config.on_request_end.append(on_request_end)
trace_config.on_request_exception.append(on_request_end)

class AWSV4AIOHttpConnection(AsyncHttpConnection):
    def __init__(self, *args, http_auth=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._http_auth = http_auth

class BenchmarkAsyncOpenSearch(AsyncOpenSearch, RequestContextHolder):
    pass

class OsClientFactory:
    """
    Abstracts how the OpenSearch client is created. Intended for testing.
    """
    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = dict(client_options)
        self.ssl_context = None
        self.logger = logging.getLogger(__name__)
        self.aws_log_in_dict = {}

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        if "amazon_aws_log_in" in masked_client_options:
            self.aws_log_in_dict = self.parse_aws_log_in_params()
            masked_client_options["aws_access_key_id"] = "*****"
            masked_client_options["aws_secret_access_key"] = "*****"
            # session_token is optional and used only for role based access
            if self.aws_log_in_dict.get("aws_session_token", None):
                masked_client_options["aws_session_token"] = "*****"
        self.logger.info("Creating OpenSearch client connected to %s with options [%s]", hosts, masked_client_options)

        if self.client_options.pop("use_ssl", False):
            import ssl
            self.logger.info("SSL support: on")
            self.client_options["scheme"] = "https"

            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                                          cafile=self.client_options.pop("ca_certs", certifi.where()))

            if not self.client_options.pop("verify_certs", True):
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE
                urllib3.disable_warnings()
            else:
                self.ssl_context.check_hostname = self._has_only_hostnames(hosts)
                self.ssl_context.verify_mode = ssl.CERT_REQUIRED
        else:
            self.logger.info("SSL support: off")
            self.client_options["scheme"] = "http"

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            self.logger.info("HTTP basic authentication: on")
            self.client_options["http_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
        else:
            self.logger.info("HTTP basic authentication: off")

        if self._is_set(self.client_options, "compressed"):
            self.client_options["http_compress"] = self.client_options.pop("compressed")
        if self._is_set(self.client_options, "http_compress"):
            pass
        if self._is_set(self.client_options, "enable_cleanup_closed"):
            self.client_options["enable_cleanup_closed"] = convert.to_bool(self.client_options.pop("enable_cleanup_closed"))
        
        if "amazon_aws_log_in" in self.client_options:
            # detect static env creds first
            creds_id = self.aws_log_in_dict.get("aws_access_key_id")
            creds_secret = self.aws_log_in_dict.get("aws_secret_access_key")
            creds_token = self.aws_log_in_dict.get("aws_session_token")
            if creds_id and creds_secret:
                from botocore.credentials import Credentials
                bc_sess = get_session()
                bc_sess._credentials = Credentials(creds_id, creds_secret, creds_token)
                self._aws_auth = PerRequestSigV4(bc_sess, self.aws_log_in_dict["service"], self.aws_log_in_dict["region"])
                self._aws_auth_async = PerRequestSigV4Async(bc_sess, self.aws_log_in_dict["service"], self.aws_log_in_dict["region"])
            else:
                import os
                from osbenchmark import exceptions
                role_arn = os.getenv("OSB_ROLE_ARN")
                if not role_arn:
                    raise exceptions.SystemSetupError(
                        "Environment variable OSB_ROLE_ARN must be set to the IAM Role ARN for AWS log-in"
                    )
                sts = boto3.client("sts")
                def _refresh():
                    resp = sts.assume_role(
                        RoleArn=role_arn,
                        RoleSessionName="osbRefreshSession",
                        DurationSeconds=43200,
                    )
                    creds = resp["Credentials"]
                    return {
                        "access_key": creds["AccessKeyId"],
                        "secret_key": creds["SecretAccessKey"],
                        "token": creds["SessionToken"],
                        "expiry_time": creds["Expiration"].isoformat(),
                    }
                metadata = _refresh()
                refreshable = RefreshableCredentials.create_from_metadata(
                    metadata=metadata,
                    refresh_using=_refresh,
                    method="sts-session"
                )
                bc_sess = get_session()
                bc_sess._credentials = refreshable
                self._aws_auth = PerRequestSigV4(bc_sess, self.aws_log_in_dict["service"], self.aws_log_in_dict["region"])
                self._aws_auth_async = PerRequestSigV4Async(bc_sess, self.aws_log_in_dict["service"], self.aws_log_in_dict["region"])

            # instantiate sync client
            self._client = OpenSearch(
                hosts=self.hosts,
                http_auth=self._aws_auth,
                use_ssl=True,
                verify_certs=True,
                ssl_context=self.ssl_context,
                connection_class=Urllib3HttpConnection,
                **self.client_options
            )
            # instantiate async client
            self._async_client = BenchmarkAsyncOpenSearch(
                hosts=self.hosts,
                http_auth=self._aws_auth_async,
                use_ssl=True,
                verify_certs=True,
                ssl_context=self.ssl_context,
                connection_class=AWSV4AIOHttpConnection,
                serializer=LazyJSONSerializer(),
                trace_config=trace_config,
                **self.client_options
            )
        else:
            # non-AWS code path
            self._client = OpenSearch(hosts=self.hosts, ssl_context=self.ssl_context, **self.client_options)
            from osbenchmark.async_connection import AsyncHttpConnection as _AsyncConn
            self._async_client = AsyncOpenSearch(hosts=self.hosts, connection_class=_AsyncConn, ssl_context=self.ssl_context, **self.client_options)

    def create(self):
        print("Returning sync client...")
        return self._client

    def create_async(self):
        print("Returning async client...")
        return self._async_client

    @staticmethod
    def _has_only_hostnames(hosts):
        logger = logging.getLogger(__name__)
        has_ip, has_hostname = False, False
        for host in hosts:
            if is_ipaddress(host["host"]):
                has_ip = True
            else:
                has_hostname = True

        if has_ip and has_hostname:
            console.warn("Although certificate verification is enabled, "
                "peer hostnames will not be matched since the host list is a mix "
                "of names and IP addresses", logger=logger)
            return False

        return has_hostname

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def parse_aws_log_in_params(self):
        # pylint: disable=import-outside-toplevel
        import os
        aws_log_in_dict = {}
        # aws log in : option 1) pass in parameters from os environment variables
        if self.client_options["amazon_aws_log_in"] == "environment":
            aws_log_in_dict["aws_access_key_id"] = os.environ.get("OSB_AWS_ACCESS_KEY_ID")
            aws_log_in_dict["aws_secret_access_key"] = os.environ.get("OSB_AWS_SECRET_ACCESS_KEY")
            aws_log_in_dict["region"] = os.environ.get("OSB_REGION")
            aws_log_in_dict["service"] = os.environ.get("OSB_SERVICE")
            # optional: applicable only for role-based access
            aws_log_in_dict["aws_session_token"] = os.environ.get("OSB_AWS_SESSION_TOKEN")
        # aws log in : option 2) parameters are passed in from command line
        elif self.client_options["amazon_aws_log_in"] == "client_option":
            aws_log_in_dict["aws_access_key_id"] = self.client_options.get("aws_access_key_id")
            aws_log_in_dict["aws_secret_access_key"] = self.client_options.get("aws_secret_access_key")
            aws_log_in_dict["region"] = self.client_options.get("region")
            aws_log_in_dict["service"] = self.client_options.get("service")
            # optional: applicable only for role-based access
            aws_log_in_dict["aws_session_token"] = self.client_options.get("aws_session_token")
        if (not aws_log_in_dict["aws_access_key_id"] or not aws_log_in_dict["aws_secret_access_key"]
                or not aws_log_in_dict["service"] or not aws_log_in_dict["region"]):
            self.logger.error("Invalid amazon aws log in parameters, required input aws_access_key_id, "
                              "aws_secret_access_key, service and region.")
            raise exceptions.SystemSetupError(
                "Invalid amazon aws log in parameters, required input aws_access_key_id, "
                "aws_secret_access_key, and region."
            )
        if aws_log_in_dict["service"] not in ['es', 'aoss']:
            self.logger.error("Service for aws log in should be one of 'es' or 'aoss'")
            raise exceptions.SystemSetupError(
                "Cannot specify service as '{}'. Accepted values are 'es' or 'aoss'.".format(
                    aws_log_in_dict["service"])
            )
        return aws_log_in_dict

    def create(self):
        # pylint: disable=import-outside-toplevel
        return self._client

    def create_async(self):
        # pylint: disable=import-outside-toplevel
        return self._async_client


def wait_for_rest_layer(opensearch, max_attempts=40):
    """
    Waits for ``max_attempts`` until OpenSearch's REST API is available.

    :param opensearch: OpenSearch client to use for connecting.
    :param max_attempts: The maximum number of attempts to check whether the REST API is available.
    :return: True iff OpenSearch's REST API is available.
    """
    # assume that at least the hosts that we expect to contact should be available. Note that this is not 100%
    # bullet-proof as a cluster could have e.g. dedicated masters which are not contained in our list of target hosts
    # but this is still better than just checking for any random node's REST API being reachable.
    expected_node_count = len(opensearch.transport.hosts)
    logger = logging.getLogger(__name__)
    for attempt in range(max_attempts):
        logger.debug("REST API is available after %s attempts", attempt)
        # pylint: disable=import-outside-toplevel
        import opensearchpy
        try:
            # see also WaitForHttpResource in OpenSearch tests. Contrary to the ES tests we consider the API also
            # available when the cluster status is RED (as long as all required nodes are present)
            opensearch.cluster.health(wait_for_nodes=">={}".format(expected_node_count))
            logger.info("REST API is available for >= [%s] nodes after [%s] attempts.", expected_node_count, attempt)
            return True
        except opensearchpy.ConnectionError as e:
            if "SSL: UNKNOWN_PROTOCOL" in str(e):
                raise exceptions.SystemSetupError("Could not connect to cluster via https. Is this an https endpoint?", e)
            else:
                logger.debug("Got connection error on attempt [%s]. Sleeping...", attempt)
                time.sleep(3)
        except opensearchpy.TransportError as e:
            # cluster block, our wait condition is not reached
            if e.status_code in (503, 401, 408):
                logger.debug("Got status code [%s] on attempt [%s]. Sleeping...", e.status_code, attempt)
                time.sleep(3)
            elif e.status_code == 404:
                # Serverless does not support the cluster-health API.  Test with _cat/indices for now.
                catclient = opensearchpy.client.cat.CatClient(opensearch)
                try:
                    catclient.indices()
                    return True
                except Exception as e:
                    logger.warning("Encountered exception %s when attempting to probe endpoint health", e)
                    raise e
            else:
                logger.warning("Got unexpected status code [%s] on attempt [%s].", e.status_code, attempt)
                raise e
    return False


class MessageProducerFactory:
    @staticmethod
    async def create(params):
        """
        Creates and returns a message producer based on the ingestion source.
        Currently supports Kafka. Ingestion source should be a dict like:
            {'type': 'kafka', 'param': {'topic': 'test', 'bootstrap-servers': 'localhost:34803'}}
        """
        ingestion_source = params.get("ingestion-source", {})
        producer_type = ingestion_source.get("type", "kafka").lower()
        if producer_type == "kafka":
            return await KafkaMessageProducer.create(params)
        else:
            raise ValueError(f"Unsupported ingestion source type: {producer_type}")
