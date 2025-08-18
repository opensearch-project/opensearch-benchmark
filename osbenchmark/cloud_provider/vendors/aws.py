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
import os
import logging

import opensearchpy
import boto3
from botocore.credentials import Credentials

from osbenchmark import exceptions, async_connection
from ..cloud_provider import CloudProvider

class AWSProvider(CloudProvider):
    AVAILABLE_SERVICES = ['es', 'aoss']
    VALID_CONFIG_SETTINGS = ['config', 'environment', 'session']

    def __init__(self):
        self.aws_log_in_config = {}
        self.aws_metrics_log_in_config = {}
        self.logger = logging.getLogger(__name__)

    def validate_client_options(self, client_options) -> bool:
        return "amazon_aws_log_in" in client_options

    def validate_config_for_metrics(self, config) -> bool:
        metrics_amazon_aws_log_in = config.opts("reporting", "datastore.amazon_aws_log_in",
                                                      default_value=None, mandatory=False)

        if metrics_amazon_aws_log_in in AWSProvider.VALID_CONFIG_SETTINGS:
            return True

        return False

    def mask_client_options(self, masked_client_options, client_options) -> dict:
        masked_client_options["aws_access_key_id"] = "*****"
        masked_client_options["aws_secret_access_key"] = "*****"
        # session_token is optional and used only for role based access
        if self.aws_log_in_config.get("aws_session_token", None):
            masked_client_options["aws_session_token"] = "*****"

        return masked_client_options

    def parse_log_in_params(self, client_options=None, config=None, for_metrics_datastore=False) -> dict:
        if for_metrics_datastore:
            # This is meant for the situation where benchmark.ini specifies datastore with AWS credentials
            if config is None:
                raise exceptions.ConfigError("Missing config when parsing log in params for metrics.")

            metrics_amazon_aws_log_in = config.opts("reporting", "datastore.amazon_aws_log_in",
                                                default_value=None, mandatory=False)

            metrics_aws_access_key_id = None
            metrics_aws_secret_access_key = None
            metrics_aws_session_token = None
            metrics_aws_region = None
            metrics_aws_service = None

            if metrics_amazon_aws_log_in == 'config':
                metrics_aws_access_key_id = config.opts("reporting", "datastore.aws_access_key_id",
                                                            default_value=None, mandatory=False)
                metrics_aws_secret_access_key = config.opts("reporting", "datastore.aws_secret_access_key",
                                                                default_value=None, mandatory=False)
                metrics_aws_session_token = config.opts("reporting", "datastore.aws_session_token",
                                                            default_value=None, mandatory=False)
                metrics_aws_region = config.opts("reporting", "datastore.region",
                                                    default_value=None, mandatory=False)
                metrics_aws_service = config.opts("reporting", "datastore.service",
                                                        default_value=None, mandatory=False)
            elif metrics_amazon_aws_log_in == 'environment':
                metrics_aws_access_key_id = os.getenv("OSB_DATASTORE_AWS_ACCESS_KEY_ID", default=None)
                metrics_aws_secret_access_key = os.getenv("OSB_DATASTORE_AWS_SECRET_ACCESS_KEY", default=None)
                metrics_aws_session_token = os.getenv("OSB_DATASTORE_AWS_SESSION_TOKEN", default=None)
                metrics_aws_region = os.getenv("OSB_DATASTORE_REGION", default=None)
                metrics_aws_service = os.getenv("OSB_DATASTORE_SERVICE", default=None)

            if metrics_amazon_aws_log_in is not None:
                if (
                        not metrics_aws_access_key_id or
                        not metrics_aws_secret_access_key or
                        not metrics_aws_region or
                        not metrics_aws_service
                ):
                    if metrics_amazon_aws_log_in == 'environment':
                        missing_aws_credentials_message = "Missing AWS credentials through " \
                                                        "OSB_DATASTORE_AWS_ACCESS_KEY_ID, " \
                                                        "OSB_DATASTORE_AWS_SECRET_ACCESS_KEY, " \
                                                        "OSB_DATASTORE_REGION, OSB_DATASTORE_SERVICE " \
                                                        "environment variables."
                    elif metrics_amazon_aws_log_in == 'config':
                        missing_aws_credentials_message = "Missing AWS credentials through datastore.aws_access_key_id, " \
                                                        "datastore.aws_secret_access_key, datastore.region, " \
                                                        "datastore.service in the config file."
                    else:
                        missing_aws_credentials_message = "datastore.amazon_aws_log_in can only be one of " \
                                                        "'environment' or 'config'"
                    raise exceptions.ConfigError(missing_aws_credentials_message) from None

                if metrics_aws_service not in AWSProvider.AVAILABLE_SERVICES:
                    raise exceptions.ConfigError(f"datastore.service can only be one of {AWSProvider.AVAILABLE_SERVICES}") from None

            self.aws_metrics_log_in_config['metrics_aws_log_in_choice'] = metrics_amazon_aws_log_in
            self.aws_metrics_log_in_config['metrics_aws_access_key_id'] = metrics_aws_access_key_id
            self.aws_metrics_log_in_config['metrics_aws_secret_access_key'] = metrics_aws_secret_access_key
            self.aws_metrics_log_in_config['metrics_aws_session_token'] = metrics_aws_session_token
            self.aws_metrics_log_in_config['metrics_aws_service'] = metrics_aws_service
            self.aws_metrics_log_in_config['metrics_aws_region'] = metrics_aws_region

        else:
            def validate_for_environment_and_client_options():
                # Validate aws_log_in_config
                required_fields = ["aws_access_key_id", "aws_secret_access_key", "service", "region"]
                for field in required_fields:
                    if not self.aws_log_in_config[field]:
                        msg = "Invalid AWS log in parameters, required inputs are aws_access_key_id, \
                            aws_secret_access_key, service and region."
                        self.logger.error(msg)
                        raise exceptions.SystemSetupError(msg)

            # This is for all other client use-cases
            if client_options is None:
                raise exceptions.ConfigurationError("Missing client options when parsing log in params")

            # AWS log in : option 1) pass in parameters from os environment variables
            if client_options["amazon_aws_log_in"] == "environment":
                self.aws_log_in_config["aws_access_key_id"] = os.environ.get("OSB_AWS_ACCESS_KEY_ID")
                self.aws_log_in_config["aws_secret_access_key"] = os.environ.get("OSB_AWS_SECRET_ACCESS_KEY")
                self.aws_log_in_config["region"] = os.environ.get("OSB_REGION")
                self.aws_log_in_config["service"] = os.environ.get("OSB_SERVICE")
                # optional: applicable only for role-based access
                self.aws_log_in_config["aws_session_token"] = os.environ.get("OSB_AWS_SESSION_TOKEN")
                validate_for_environment_and_client_options()

            # AWS log in : option 2) parameters are passed in from command line
            elif client_options["amazon_aws_log_in"] == "client_option":
                self.aws_log_in_config["aws_access_key_id"] = client_options.get("aws_access_key_id")
                self.aws_log_in_config["aws_secret_access_key"] = client_options.get("aws_secret_access_key")
                self.aws_log_in_config["region"] = client_options.get("region")
                self.aws_log_in_config["service"] = client_options.get("service")
                # optional: applicable only for role-based access
                self.aws_log_in_config["aws_session_token"] = client_options.get("aws_session_token")
                validate_for_environment_and_client_options()

            # AWS log in: option 3) parameters are passed in from command line but for session
            elif client_options["amazon_aws_log_in"] == "session":
                self.aws_log_in_config["region"] = client_options.get("region")
                self.aws_log_in_config["service"] = client_options.get("service")

                # Validate session differently from environment and client_option
                if client_options["amazon_aws_log_in"] == "session" and not self.aws_log_in_config["region"]:
                    self.logger.error("region is mandatory parameter for session client.")
                    raise exceptions.SystemSetupError(
                        "region is mandatory parameter for session client."
                    )

            if self.aws_log_in_config["service"] not in AWSProvider.AVAILABLE_SERVICES:
                self.logger.error("Service for AWS log in should be one %s", AWSProvider.AVAILABLE_SERVICES)
                raise exceptions.SystemSetupError(
                    "Cannot specify service as '{}'. Accepted values are {}.".format(
                        self.aws_log_in_config["service"],
                        AWSProvider.AVAILABLE_SERVICES)
                )

    def update_client_options_for_metrics(self, client_options):
        if self.aws_metrics_log_in_config['metrics_aws_log_in_choice'] is not None:
            client_options["amazon_aws_log_in"] = 'client_option'
            client_options["aws_access_key_id"] = self.aws_metrics_log_in_config['metrics_aws_access_key_id']
            client_options["aws_secret_access_key"] = self.aws_metrics_log_in_config['metrics_aws_secret_access_key']
            client_options["service"] = self.aws_metrics_log_in_config['metrics_aws_service']
            client_options["region"] = self.aws_metrics_log_in_config['metrics_aws_region']

            if self.aws_metrics_log_in_config['metrics_aws_session_token']:
                client_options["aws_session_token"] = self.aws_metrics_log_in_config['metrics_aws_session_token']

        return client_options

    def create_client(self, hosts, client_options, client_class=None, use_async=False):
        self.logger.info("client options %s", client_options)
        if client_options['amazon_aws_log_in'] == "session":
            credentials = boto3.Session().get_credentials()
        else:
            credentials = Credentials(access_key=self.aws_log_in_config["aws_access_key_id"],
                                    secret_key=self.aws_log_in_config["aws_secret_access_key"],
                                    token=self.aws_log_in_config["aws_session_token"])

        if use_async:
            aws_auth = opensearchpy.AWSV4SignerAsyncAuth(credentials, self.aws_log_in_config["region"],
                                                     self.aws_log_in_config["service"])
            return client_class(hosts=hosts, use_ssl=True, verify_certs=True, http_auth=aws_auth,
                                        connection_class=async_connection.AsyncHttpConnection,
                                        **client_options)
        else:
            aws_auth = opensearchpy.Urllib3AWSV4SignerAuth(credentials, self.aws_log_in_config["region"],
                                                    self.aws_log_in_config["service"])
            return opensearchpy.OpenSearch(hosts=hosts, use_ssl=True, verify_certs=True, http_auth=aws_auth,
                                        connection_class=opensearchpy.Urllib3HttpConnection)
