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

from concurrent.futures import ThreadPoolExecutor, wait
from boto3 import client

from osbenchmark import exceptions
from osbenchmark.data_streaming.data_producer import DataProducer
from osbenchmark.cloud_provider import CloudProviderFactory
from osbenchmark.cloud_provider.vendors.aws import AWSProvider

CHUNK_SIZE = 50 * 1024**2
BATCH = 16

class S3DataProducer(DataProducer):
    """
    Generate data by downloading an object from S3.
    Will support downloading from multiple objects in the future.
    """
    def __init__(self, bucket:str, key:str, client_options: dict, multipart_size: int = 0) -> None:
        """
        Constructor.
        :param bucket: The S3 bucket to download from.
        :param key: The object to download.  Could be a list or pattern in the future.
        :param client_options: A dict containing the client options.
        :param multipart_size: The size of the object if it is to be downloaded in multipart fashion.
        """
        try:
            self.logger = logging.getLogger(__name__)
            self.bucket = bucket
            self.key = key

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

                self.s3_client = client('s3',
                                   aws_access_key_id = provider.aws_log_in_config['aws_access_key_id'],
                                   aws_secret_access_key = provider.aws_log_in_config['aws_secret_access_key'],
                                   aws_session_token = provider.aws_log_in_config['aws_session_token'],
                                   )
            else:
                # For testing.  Set credentials with environment variables.
                self.s3_client = client('s3')
            if multipart_size:
                self.generator = self.s3_get_multipart_generator(self.s3_client, self.bucket, self.key, 0, multipart_size)
            else:
                response = self.s3_client.get_object(Bucket=bucket, Key=key)
                self.streaming_body = response['Body']
        except Exception as e:
            print(f"Error: {e}")

    def gen_range_args(self, beg, end, chunk_size):
        length = end - beg
        n = (length + chunk_size - 1) // chunk_size
        ranges = []
        for i in range(n):
            r_beg = beg + i * chunk_size
            if i == n - 1:
                r_end = end - 1
            else:
                r_end = r_beg + chunk_size - 1
            ranges.append(f'bytes={r_beg}-{r_end}')
        return ranges

    def s3_get_object_range(self, args):
        self.s3_client, bucket, key, range_header = args
        resp = self.s3_client.get_object(Bucket=bucket, Key=key, Range=range_header)
        body = resp['Body'].read()
        return body

    def s3_get_multipart_generator(self, s3_client, bucket, key, beg, end):
        ranges = self.gen_range_args(beg, end, CHUNK_SIZE)
        with ThreadPoolExecutor(max_workers=BATCH) as executor:
            for i in range(0, len(ranges), BATCH):
                chunk = ranges[i:i+BATCH]
                futures = [executor.submit(self.s3_get_object_range, (s3_client, bucket, key, range)) for range in chunk]
                wait(futures)
                for future in futures:
                    yield future.result()

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

    def get_data_multipart(self, size: int) -> bytes:
        """
        Obtain data via generator.
        """
        try:
            # rsl = "".join([next(self.generator).decode('utf-8') for i in range(BATCH)])
            lis = list()
            for _ in range(BATCH):
                lis.append(next(self.generator).decode('utf-8'))
        except StopIteration:
            pass
        return "".join(lis)

    def output_chunk(self, rsl, chunk_id):
        with open("file" + "{:04d}".format(chunk_id), "w") as fh:
            print(rsl, file=fh, end='')

    def gen_chunks(self):
        "Generate chunked output ready for ingestion by OSB clients."
        chunk_id = 0
        trailing = ""
        for chunk in self.generator:
            rsl = chunk.decode('utf-8')
            i = len(rsl)
            while i and rsl[i-1] != '\n':
                i -= 1
            if i == 0:
                raise Exception("could not locate document end")
            self.output_chunk(trailing + rsl[:i], chunk_id)
            trailing = rsl[i:]
            chunk_id += 1


# For testing.  Set AWS credentials using environment variables.
def main(bucket: str, key: str, size: str = 1024*1024) -> None:
    producer = S3DataProducer(bucket, key, None, 10737417962)
    producer.gen_chunks()

if __name__ == '__main__':
    # pylint: disable = no-value-for-parameter
    main(*sys.argv[1:])
