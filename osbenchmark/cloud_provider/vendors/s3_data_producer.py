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

import sys
import logging
from boto3 import client

from osbenchmark import exceptions
from osbenchmark.data_streaming.data_producer import DataProducer
from osbenchmark.cloud_provider import CloudProviderFactory
from osbenchmark.cloud_provider.vendors.aws import AWSProvider

class S3DataProducer(DataProducer):
    """
    Generate data by downloading an object from S3.
    Will support downloading from multiple objects in the future.
    """
    def __init__(self, bucket:str, key:str, client_options: dict) -> None:
        """
        Constructor.
        :param bucket: The S3 bucket to download from.
        :param key: The object to download.  Could be a list or pattern in the future.
        :param client_options: A dict containing the client options.
        """
        try:
            self.logger = logging.getLogger(__name__)

            # Credentials are set via the client options in a similar manner as used for
            # the existing SigV4 support.
            if client_options:
                # We assume "amazon_aws_log_in" is set as one of the client options.
                provider = CloudProviderFactory.get_provider_from_client_options(client_options)
                if not isinstance(provider, AWSProvider):
                    raise exceptions.ConfigError("S3DataProducer operates only on AWS")

                # Extract credentials from the environment, as set in the OSB_* variables.
                provider.parse_log_in_params(client_options=client_options)
                masked_client_options = dict(client_options)
                provider.mask_client_options(masked_client_options, client_options)
                self.logger.info("Masking client options with cloud provider: [%s]", provider)

                s3 = client('s3',
                            aws_access_key_id = provider.aws_log_in_config['aws_access_key_id'],
                            aws_secret_access_key = provider.aws_log_in_config['aws_secret_access_key'],
                            aws_session_token = provider.aws_log_in_config['aws_session_token'],
                            )
            else:
                # For testing.  Set credentials with environment variables.
                s3 = client('s3')

            # Stream the HTTP response.
            response = s3.get_object(Bucket=bucket, Key=key)
            self.streaming_body = response['Body']
        except Exception as e:
            print(f"Error: {e}")

    def get_data(self, size: int) -> bytes:
        """
        Obtain data by reading directly from the response stream.  Streaming reads do not operate
        via multi-part downloads; this capability will need to be added explictly for better performance.

        :param size: the amount of data to be read from the stream, in bytes.  Typically, this
                     should be the size of the target buffer intended to receive the data.
        """
        try:
            chunk = self.streaming_body.read(size)
            return chunk
        except Exception as e:
            print(f"Error: {e}")


# For testing.  Set AWS credentials using environment variables.
def main(bucket: str, key: str, size: str = 4096) -> None:
    producer = S3DataProducer(bucket, key, None)
    while c := producer.get_data(size):
        print(c)

if __name__ == '__main__':
    # pylint: disable = no-value-for-parameter
    main(*sys.argv[1:])
