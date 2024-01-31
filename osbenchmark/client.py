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

import contextvars
import logging
import time

import certifi
import urllib3
from urllib3.util.ssl_ import is_ipaddress

from osbenchmark import exceptions, doc_link
from osbenchmark.utils import console, convert


class RequestContextManager:
    """
    Ensures that request context span the defined scope and allow nesting of request contexts with proper propagation.
    This means that we can span a top-level request context, open sub-request contexts that can be used to measure
    individual timings and still measure the proper total time on the top-level request context.
    """
    def __init__(self, request_context_holder):
        self.ctx_holder = request_context_holder
        self.ctx = None
        self.token = None

    async def __aenter__(self):
        self.ctx, self.token = self.ctx_holder.init_request_context()
        return self

    @property
    def request_start(self):
        return self.ctx["request_start"]

    @property
    def request_end(self):
        return max((value for value in self.ctx["request_end_list"] if value < self.client_request_end))

    @property
    def client_request_start(self):
        return self.ctx["client_request_start"]

    @property
    def client_request_end(self):
        return self.ctx["client_request_end"]

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # propagate earliest request start and most recent request end to parent
        client_request_start = self.client_request_start
        client_request_end = self.client_request_end
        request_start = self.request_start
        request_end = self.request_end
        self.ctx_holder.restore_context(self.token)
        # don't attempt to restore these values on the top-level context as they don't exist
        if self.token.old_value != contextvars.Token.MISSING:
            self.ctx_holder.update_request_start(request_start)
            self.ctx_holder.update_request_end(request_end)
            self.ctx_holder.update_client_request_start(client_request_start)
            self.ctx_holder.update_client_request_end(client_request_end)
        self.token = None
        return False


class RequestContextHolder:
    """
    Holds request context variables. This class is only meant to be used together with RequestContextManager.
    """
    request_context = contextvars.ContextVar("benchmark_request_context")

    def new_request_context(self):
        return RequestContextManager(self)

    @classmethod
    def init_request_context(cls):
        ctx = {}
        token = cls.request_context.set(ctx)
        return ctx, token

    @classmethod
    def restore_context(cls, token):
        cls.request_context.reset(token)

    @classmethod
    def update_request_start(cls, new_request_start):
        meta = cls.request_context.get()
        # this can happen if multiple requests are sent on the wire for one logical request (e.g. scrolls)
        if "request_start" not in meta and "client_request_start" in meta:
            meta["request_start"] = new_request_start

    @classmethod
    def update_request_end(cls, new_request_end):
        meta = cls.request_context.get()
        if "request_end_list" not in meta:
            meta["request_end_list"] = []
        meta["request_end_list"].append(new_request_end)

    @classmethod
    def update_client_request_start(cls, new_client_request_start):
        meta = cls.request_context.get()
        if "client_request_start" not in meta:
            meta["client_request_start"] = new_client_request_start

    @classmethod
    def update_client_request_end(cls, new_client_request_end):
        meta = cls.request_context.get()
        meta["client_request_end"] = new_client_request_end

    @classmethod
    def on_client_request_start(cls):
        cls.update_client_request_start(time.perf_counter())

    @classmethod
    def on_client_request_end(cls):
        cls.update_client_request_end(time.perf_counter())

    @classmethod
    def on_request_start(cls):
        cls.update_request_start(time.perf_counter())

    @classmethod
    def on_request_end(cls):
        cls.update_request_end(time.perf_counter())

    @classmethod
    def return_raw_response(cls):
        ctx = cls.request_context.get()
        ctx["raw_response"] = True


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

        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        if self.client_options.pop("use_ssl", False):
            # pylint: disable=import-outside-toplevel
            import ssl
            self.logger.info("SSL support: on")
            self.client_options["scheme"] = "https"

            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                                          cafile=self.client_options.pop("ca_certs", certifi.where()))

            if not self.client_options.pop("verify_certs", True):
                self.logger.info("SSL certificate verification: off")
                # order matters to avoid ValueError: check_hostname needs a SSL context with either CERT_OPTIONAL or CERT_REQUIRED
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE

                self.logger.warning("User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a "
                                    "benchmark. Disabling urllib warnings now to avoid a logging storm. "
                                    "See https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings for details.")
                # disable:  "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly \
                # advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings"
                urllib3.disable_warnings()
            else:
                # The peer's hostname can be matched if only a hostname is provided.
                # In other words, hostname checking is disabled if an IP address is
                # found in the host lists.
                self.ssl_context.check_hostname = self._has_only_hostnames(hosts)
                self.ssl_context.verify_mode=ssl.CERT_REQUIRED
                self.logger.info("SSL certificate verification: on")

            # When using SSL_context, all SSL related kwargs in client options get ignored
            client_cert = self.client_options.pop("client_cert", False)
            client_key = self.client_options.pop("client_key", False)

            if not client_cert and not client_key:
                self.logger.info("SSL client authentication: off")
            elif bool(client_cert) != bool(client_key):
                self.logger.error(
                    "Supplied client-options contain only one of client_cert/client_key. "
                )
                defined_client_ssl_option = "client_key" if client_key else "client_cert"
                missing_client_ssl_option = "client_cert" if client_key else "client_key"
                console.println(
                    "'{}' is missing from client-options but '{}' has been specified.\n"
                    "If your OpenSearch setup requires client certificate verification both need to be supplied.\n"
                    "Read the documentation at {}\n".format(
                        missing_client_ssl_option,
                        defined_client_ssl_option,
                        console.format.link(doc_link("command_line_reference.html#client-options")))
                )
                raise exceptions.SystemSetupError(
                    "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                        defined_client_ssl_option,
                        missing_client_ssl_option))
            elif client_cert and client_key:
                self.logger.info("SSL client authentication: on")
                self.ssl_context.load_cert_chain(certfile=client_cert,
                                                 keyfile=client_key)
        else:
            self.logger.info("SSL support: off")
            self.client_options["scheme"] = "http"

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            self.logger.info("HTTP basic authentication: on")
            self.client_options["http_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
        else:
            self.logger.info("HTTP basic authentication: off")

        if self._is_set(self.client_options, "compressed"):
            console.warn("You set the deprecated client option 'compressed'. Please use 'http_compress' instead.", logger=self.logger)
            self.client_options["http_compress"] = self.client_options.pop("compressed")

        if self._is_set(self.client_options, "http_compress"):
            self.logger.info("HTTP compression: on")
        else:
            self.logger.info("HTTP compression: off")

        if self._is_set(self.client_options, "enable_cleanup_closed"):
            self.client_options["enable_cleanup_closed"] = convert.to_bool(self.client_options.pop("enable_cleanup_closed"))

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
        import opensearchpy
        from botocore.credentials import Credentials

        if "amazon_aws_log_in" not in self.client_options:
            return opensearchpy.OpenSearch(hosts=self.hosts, ssl_context=self.ssl_context, **self.client_options)

        credentials = Credentials(access_key=self.aws_log_in_dict["aws_access_key_id"],
                                  secret_key=self.aws_log_in_dict["aws_secret_access_key"],
                                  token=self.aws_log_in_dict["aws_session_token"])
        aws_auth = opensearchpy.Urllib3AWSV4SignerAuth(credentials, self.aws_log_in_dict["region"],
                                                self.aws_log_in_dict["service"])
        return opensearchpy.OpenSearch(hosts=self.hosts, use_ssl=True, verify_certs=True, http_auth=aws_auth,
                                       connection_class=opensearchpy.Urllib3HttpConnection)

    def create_async(self):
        # pylint: disable=import-outside-toplevel
        import opensearchpy
        import osbenchmark.async_connection
        import io
        import aiohttp

        from opensearchpy.serializer import JSONSerializer
        from botocore.credentials import Credentials

        class LazyJSONSerializer(JSONSerializer):
            def loads(self, s):
                meta = BenchmarkAsyncOpenSearch.request_context.get()
                if "raw_response" in meta:
                    return io.BytesIO(s)
                else:
                    return super().loads(s)

        async def on_request_start(session, trace_config_ctx, params):
            BenchmarkAsyncOpenSearch.on_request_start()

        async def on_request_end(session, trace_config_ctx, params):
            BenchmarkAsyncOpenSearch.on_request_end()

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        # ensure that we also stop the timer when a request "ends" with an exception (e.g. a timeout)
        trace_config.on_request_exception.append(on_request_end)

        # override the builtin JSON serializer
        self.client_options["serializer"] = LazyJSONSerializer()
        self.client_options["trace_config"] = trace_config

        class BenchmarkAsyncOpenSearch(opensearchpy.AsyncOpenSearch, RequestContextHolder):
            pass

        if "amazon_aws_log_in" not in self.client_options:
            return BenchmarkAsyncOpenSearch(hosts=self.hosts,
                                            connection_class=osbenchmark.async_connection.AIOHttpConnection,
                                            ssl_context=self.ssl_context,
                                            **self.client_options)

        credentials = Credentials(access_key=self.aws_log_in_dict["aws_access_key_id"],
                                  secret_key=self.aws_log_in_dict["aws_secret_access_key"],
                                  token=self.aws_log_in_dict["aws_session_token"])
        aws_auth = opensearchpy.AWSV4SignerAsyncAuth(credentials, self.aws_log_in_dict["region"],
                                                     self.aws_log_in_dict["service"])
        return BenchmarkAsyncOpenSearch(hosts=self.hosts,
                                        connection_class=osbenchmark.async_connection.AsyncHttpConnection,
                                        use_ssl=True, verify_certs=True, http_auth=aws_auth,
                                        **self.client_options)


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
