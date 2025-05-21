import os
from abc import ABC, abstractmethod

import opensearchpy
from botocore.credentials import Credentials

from osbenchmark import exceptions, async_connection

class CloudProvider(ABC):

    @abstractmethod
    def validate_client_options(self, client_options: dict) -> bool:
        pass

    @abstractmethod
    def validate_config_for_metrics(self, config) -> bool:
        pass

    @abstractmethod
    def mask_client_options(self, masked_client_options: dict, client_options: dict) -> dict:
        pass

    @abstractmethod
    def parse_log_in_params(self, client_options: dict) -> dict:
        pass

    @abstractmethod
    def parse_log_in_params_for_metrics(self, config) -> dict:
        pass

    @abstractmethod
    def update_client_options_for_metrics(self, client_options) -> dict:
        pass

    @abstractmethod
    def create_client(self, hosts):
        pass

    @abstractmethod
    def create_async_client(self, hosts, client_class):
        pass

class AWSProvider(CloudProvider):
    AVAILABLE_SERVICES = ['es', 'aoss']
    VALID_CONFIG_SETTINGS = ['config', 'environment']

    def __init__(self):
        self.aws_log_in_dict = {}
        self.aws_metrics_log_in_dict = {}

    def validate_client_options(self, client_options) -> bool:
        return "amazon_aws_log_in" in client_options

    def validate_config_for_metrics(self, config) -> bool:
        metrics_amazon_aws_log_in = config.opts("results_publishing", "datastore.amazon_aws_log_in",
                                                      default_value=None, mandatory=False)

        if (metrics_amazon_aws_log_in in AWSProvider.VALID_CONFIG_SETTINGS):
            return True

        return False

    def mask_client_options(self, masked_client_options, client_options) -> dict:
        self.aws_log_in_dict = self.parse_log_in_params(client_options)

        masked_client_options["aws_access_key_id"] = "*****"
        masked_client_options["aws_secret_access_key"] = "*****"
        # session_token is optional and used only for role based access
        if self.aws_log_in_dict.get("aws_session_token", None):
            masked_client_options["aws_session_token"] = "*****"

        return masked_client_options

    def parse_log_in_params(self, client_options) -> dict:
        log_in_dict = {}
        # aws log in : option 1) pass in parameters from os environment variables
        if client_options["amazon_aws_log_in"] == "environment":
            log_in_dict["aws_access_key_id"] = os.environ.get("OSB_AWS_ACCESS_KEY_ID")
            log_in_dict["aws_secret_access_key"] = os.environ.get("OSB_AWS_SECRET_ACCESS_KEY")
            log_in_dict["region"] = os.environ.get("OSB_REGION")
            log_in_dict["service"] = os.environ.get("OSB_SERVICE")
            # optional: applicable only for role-based access
            log_in_dict["aws_session_token"] = os.environ.get("OSB_AWS_SESSION_TOKEN")

        # aws log in : option 2) parameters are passed in from command line
        elif client_options["amazon_aws_log_in"] == "client_option":
            log_in_dict["aws_access_key_id"] = client_options.get("aws_access_key_id")
            log_in_dict["aws_secret_access_key"] = client_options.get("aws_secret_access_key")
            log_in_dict["region"] = client_options.get("region")
            log_in_dict["service"] = client_options.get("service")
            # optional: applicable only for role-based access
            log_in_dict["aws_session_token"] = client_options.get("aws_session_token")

        if (not log_in_dict["aws_access_key_id"] or not log_in_dict["aws_secret_access_key"]
                or not log_in_dict["service"] or not log_in_dict["region"]):
            self.logger.error("Invalid amazon aws log in parameters, required input aws_access_key_id, "
                              "aws_secret_access_key, service and region.")
            raise exceptions.SystemSetupError(
                "Invalid amazon aws log in parameters, required input aws_access_key_id, "
                "aws_secret_access_key, and region."
            )

        if log_in_dict["service"] not in ['es', 'aoss']:
            self.logger.error("Service for aws log in should be one %s", AWSProvider.AVAILABLE_SERVICES)
            raise exceptions.SystemSetupError(
                "Cannot specify service as '{}'. Accepted values are {}.".format(
                    log_in_dict["service"],
                    AWSProvider.AVAILABLE_SERVICES)
            )
        return log_in_dict

    def parse_log_in_params_for_metrics(self, config):
        metrics_amazon_aws_log_in = config.opts("results_publishing", "datastore.amazon_aws_log_in",
                                                      default_value=None, mandatory=False)
        # This is meant to interpret the config and check for aws log in
        metrics_aws_access_key_id = None
        metrics_aws_secret_access_key = None
        metrics_aws_session_token = None
        metrics_aws_region = None
        metrics_aws_service = None

        if metrics_amazon_aws_log_in == 'config':
            metrics_aws_access_key_id = config.opts("results_publishing", "datastore.aws_access_key_id",
                                                          default_value=None, mandatory=False)
            metrics_aws_secret_access_key = config.opts("results_publishing", "datastore.aws_secret_access_key",
                                                              default_value=None, mandatory=False)
            metrics_aws_session_token = config.opts("results_publishing", "datastore.aws_session_token",
                                                          default_value=None, mandatory=False)
            metrics_aws_region = config.opts("results_publishing", "datastore.region",
                                                   default_value=None, mandatory=False)
            metrics_aws_service = config.opts("results_publishing", "datastore.service",
                                                    default_value=None, mandatory=False)
        elif metrics_amazon_aws_log_in == 'environment':
            metrics_aws_access_key_id = os.getenv("OSB_DATASTORE_AWS_ACCESS_KEY_ID", default=None)
            metrics_aws_secret_access_key = os.getenv("OSB_DATASTORE_AWS_SECRET_ACCESS_KEY", default=None)
            metrics_aws_session_token = os.getenv("OSB_DATASTORE_AWS_SESSION_TOKEN", default=None)
            metrics_aws_region = os.getenv("OSB_DATASTORE_REGION", default=None)
            metrics_aws_service = os.getenv("OSB_DATASTORE_SERVICE", default=None)

        if metrics_amazon_aws_log_in is not None:
            if (
                    not metrics_aws_access_key_id or not metrics_aws_secret_access_key
                    or not metrics_aws_region or not metrics_aws_service
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

            if (metrics_aws_service not in ['es', 'aoss']):
                raise exceptions.ConfigError("datastore.service can only be one of 'es' or 'aoss'") from None

        self.aws_metrics_log_in_dict['metrics_aws_log_in_choice'] = metrics_amazon_aws_log_in
        self.aws_metrics_log_in_dict['metrics_aws_access_key_id'] = metrics_aws_access_key_id
        self.aws_metrics_log_in_dict['metrics_aws_secret_access_key'] = metrics_aws_secret_access_key
        self.aws_metrics_log_in_dict['metrics_aws_session_token'] = metrics_aws_session_token
        self.aws_metrics_log_in_dict['metrics_aws_service'] = metrics_aws_service
        self.aws_metrics_log_in_dict['metrics_aws_region'] = metrics_aws_region

    def update_client_options_for_metrics(self, client_options):
        # add options for aws user login:
        # pass in aws access key id, aws secret access key, aws session token, service and region on command
        if self.aws_metrics_log_in_dict['metrics_aws_log_in_choice'] is not None:
            client_options["amazon_aws_log_in"] = 'client_option'
            client_options["aws_access_key_id"] = self.aws_metrics_log_in_dict['metrics_aws_access_key_id']
            client_options["aws_secret_access_key"] = self.aws_metrics_log_in_dict['metrics_aws_secret_access_key']
            client_options["service"] = self.aws_metrics_log_in_dict['metrics_aws_service']
            client_options["region"] = self.aws_metrics_log_in_dict['metrics_aws_region']

            if self.aws_metrics_log_in_dict['metrics_aws_session_token']:
                client_options["aws_session_token"] = self.aws_metrics_log_in_dict['metrics_aws_session_token']

        return client_options

    def create_client(self, hosts):
        credentials = Credentials(access_key=self.aws_log_in_dict["aws_access_key_id"],
                                  secret_key=self.aws_log_in_dict["aws_secret_access_key"],
                                  token=self.aws_log_in_dict["aws_session_token"])
        aws_auth = opensearchpy.Urllib3AWSV4SignerAuth(credentials, self.aws_log_in_dict["region"],
                                                self.aws_log_in_dict["service"])
        return opensearchpy.OpenSearch(hosts=hosts, use_ssl=True, verify_certs=True, http_auth=aws_auth,
                                       connection_class=opensearchpy.Urllib3HttpConnection)


    def create_async_client(self, hosts, client_options, client_class):
        credentials = Credentials(access_key=self.aws_log_in_dict["aws_access_key_id"],
                            secret_key=self.aws_log_in_dict["aws_secret_access_key"],
                            token=self.aws_log_in_dict["aws_session_token"])
        aws_auth = opensearchpy.AWSV4SignerAsyncAuth(credentials, self.aws_log_in_dict["region"],
                                                     self.aws_log_in_dict["service"])
        return client_class(hosts=hosts,
                                        connection_class=async_connection.AsyncHttpConnection,
                                        use_ssl=True, verify_certs=True, http_auth=aws_auth,
                                        **client_options)

class CloudProviderFactory:

    providers = [
        AWSProvider()
    ]

    @classmethod
    def get_provider_from_client_options(cls, client_options) -> CloudProvider:
        for provider in cls.providers:
            if provider.validate_client_options(client_options):
                return provider

        return None

    @classmethod
    def get_provider_from_config(cls, config) -> CloudProvider:
        for provider in cls.providers:
            if provider.validate_config_for_metrics(config):
                return provider

        return None