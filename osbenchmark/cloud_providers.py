import os
from abc import ABC, abstractmethod

import opensearchpy
from botocore.credentials import Credentials

from osbenchmark.context import RequestContextHolder
from osbenchmark import exceptions, async_connection

class CloudProvider(ABC):

    @abstractmethod
    def validate_client_options(self, client_options: dict) -> bool:
        pass

    @abstractmethod
    def mask_client_options(self, masked_client_options: dict, client_options: dict) -> dict:
        pass

    @abstractmethod
    def parse_log_in_params(self, client_options: dict) -> dict:
        pass

    @abstractmethod
    def create_client(self, hosts):
        pass

    @abstractmethod
    def create_async_client(self, hosts, client_class):
        pass

class AWSProvider(CloudProvider):
    AVAILABLE_SERVICES = ['es', 'aoss']

    def __init__(self):
        self.aws_log_in_dict = {}

    def validate_client_options(self, client_options) -> bool:
        return "amazon_aws_log_in" in client_options

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
    def get_provider(cls, client_options: dict) -> CloudProvider:
        for provider in cls.providers:
            if provider.validate_client_options(client_options):
                return provider

        return None
