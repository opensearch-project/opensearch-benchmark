# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import os
import sys
import logging

from concurrent.futures import ThreadPoolExecutor, wait
from boto3 import client

from osbenchmark import exceptions
from osbenchmark.data_streaming.data_producer import DataProducer
from osbenchmark.cloud_provider import CloudProviderFactory
from osbenchmark.cloud_provider.vendors.aws import AWSProvider
from osbenchmark.workload.ingestion_manager import IngestionManager

class S3DataProducer(DataProducer):
    """
    Generate data by downloading an object from S3.
    Will support downloading from multiple objects in the future.
    """
    def __init__(self, bucket:str, keys, client_options: dict, data_dir=None) -> None:
        """
        Constructor.
        :param bucket: The S3 bucket to download from.
        :param key: The object(s) to download, a string or a list of strings.
        :param client_options: A dict containing the client options.
        """
        try:
            self.logger = logging.getLogger(__name__)
            self.bucket = bucket
            self.keys = keys

            # Defaults.  These may be overridden by the Ingestion Manager later.
            self.data_dir = data_dir or "/tmp"
            self.chunk_prefix = "chunk-"
            self.chunk_size = 50 * 1024**2              # download 50 MB chunks
            self.num_workers = 16                       # should be based on number of cores

            # Credentials are set via the client options in a similar manner as used for
            # the existing SigV4 support.
            if client_options:
                # TBD: use transient credentials that auto-refresh.
                # We assume "amazon_aws_log_in" is set as one of the client options.
                # provider = CloudProviderFactory.get_provider_from_client_options(client_options)
                client_options["amazon_aws_log_in"] = "environment"
                provider = CloudProviderFactory.providers[0]
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
        except Exception as e:
            print(f"Error: {e}")

    def _get_next_downloader(self):
        "Generator that returns the downloader for the next object to be downloaded."
        # Name globbing to be added later.
        keys = [self.keys] if not isinstance(self.keys, list) else self.keys
        for k in keys:
            # Obtain the object size.
            response = self.s3_client.head_object(Bucket=self.bucket, Key=k)
            size = response['ContentLength']
            yield self._s3_multipart_downloader(self.bucket, k, 0, size)

    def _gen_range_args(self, beg, end, chunk_size):
        "Partition a range and return the bounds in range header format."
        # Note that the S3 range header arg bounds are inclusive.
        # See: https://www.rfc-editor.org/rfc/rfc9110.html#name-range
        length = end - beg
        n = (length + chunk_size - 1) // chunk_size
        ranges = list()
        for i in range(n):
            r_beg = beg + i * chunk_size
            if i == n - 1:
                r_end = end - 1
            else:
                r_end = r_beg + chunk_size - 1
            ranges.append(f'bytes={r_beg}-{r_end}')
        return ranges

    def _s3_get_object_subrange(self, args):
        "Download a subrange of an S3 object."
        bucket, key, range = args
        resp = self.s3_client.get_object(Bucket=bucket, Key=key, Range=range)
        return resp['Body'].read()

    def _s3_multipart_downloader(self, bucket, key, beg, end):
        """
        Generator that splits a streaming download into parts, runs a subset of these
        downloads concurrently, and returns the next downloaded chunk.
        """
        ranges = self._gen_range_args(beg, end, self.chunk_size)

        # Carry out a multi-part download, with the specified number of workers.
        # Ensure futures are garbage collected before more are issued, to not run out of memory.
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            for i in range(0, len(ranges), self.num_workers):
                subranges = ranges[i:i+self.num_workers]
                futures = [executor.submit(self._s3_get_object_subrange, (bucket, key, range))
                           for range in subranges]
                wait(futures)
                for future in futures:
                    yield future.result()

    def _output_chunk(self, rsl, chunk_id):
        "Write a chunk into its file.  It will be processed later by one ingestion client."
        with open(os.path.join(self.data_dir, self.chunk_prefix + "{:05d}".format(chunk_id)),
                  "w", encoding='utf-8') as fh:
            fh.write(rsl)

    def generate_chunked_data(self):
        "Generate chunked output ready for ingestion by OSB clients."
        chunk_id = 0
        partial_line = ""
        downloaders = self._get_next_downloader()
        for downloader in downloaders:
            for chunk in downloader:
                rsl = chunk.decode('utf-8')
                i = len(rsl)
                while i and rsl[i-1] != '\n':
                    i -= 1
                if i == 0:
                    raise exceptions.DataStreamingError(f"could not locate document end in chunk {chunk_id}")
                self._output_chunk(partial_line + rsl[:i], chunk_id)
                if IngestionManager.rd_index.value == chunk_id:
                    with IngestionManager.load_empty:
                        IngestionManager.load_empty.notify_all()
                chunk_id += 1
                IngestionManager.wr_count.value = chunk_id
                partial_line = rsl[i:]
                if chunk_id - IngestionManager.rd_index.value > IngestionManager.plimsoll:
                    with IngestionManager.load_full:
                        IngestionManager.load_full.wait()
        self._output_chunk("", chunk_id)
        chunk_id += 1
        IngestionManager.wr_count.value = chunk_id
        with IngestionManager.load_empty:
            IngestionManager.load_empty.notify_all()


# For testing.  Set AWS credentials using environment variables.
def main(bucket: str, keys: str) -> None:
    producer = S3DataProducer(bucket, keys, None)
    producer.generate_chunked_data()

if __name__ == '__main__':
    # pylint: disable = no-value-for-parameter
    main(*sys.argv[1:])
