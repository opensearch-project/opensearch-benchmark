# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

# pylint: disable=protected-access

from unittest import TestCase
import unittest.mock as mock

from osbenchmark.cloud_provider.vendors.s3_data_producer import S3DataProducer

# pylint: disable=too-many-public-methods
class TestS3DataProducer(TestCase):

    # pylint: disable = arguments-differ
    @mock.patch('osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer.__init__', return_value=None)
    def setUp(self, mock_init):
        self.producer = S3DataProducer('bucket', 'key', None)

    def test_gen_range_args_aligned(self):
        self.assertEqual(self.producer._gen_range_args(0, 8, 4), ['bytes=0-3', 'bytes=4-7'])

    def test_gen_range_args_unaligned(self):
        self.assertEqual(self.producer._gen_range_args(0, 10, 4), ['bytes=0-3', 'bytes=4-7', 'bytes=8-9'])

    def test_gen_range_args_empty(self):
        self.assertEqual(self.producer._gen_range_args(0, 0, 4), [])

    @mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._output_chunk")
    @mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._get_next_downloader")
    def test_generate_chunked_data_aligned(self, downloader, outputter):
        downloader.return_value = [ [ b"this is line 1\n" ] ]
        self.producer.generate_chunked_data()
        outputter.assert_has_calls([ mock.call("this is line 1\n", 0) ])

    @mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._output_chunk")
    @mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._get_next_downloader")
    def test_generate_chunked_data_unaligned(self, downloader, outputter):
        downloader.return_value = [ [ b"this is line 1\nthis is line 2" ] ]
        self.producer.generate_chunked_data()
        outputter.assert_has_calls([ mock.call("this is line 1\n", 0) ])

    @mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._output_chunk")
    @mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._get_next_downloader")
    def test_generate_chunked_data_aligned_multiline(self, downloader, outputter):
        downloader.return_value = [ [ b"this is line 0\nthis is line 1\n" ] ]
        self.producer.generate_chunked_data()
        outputter.assert_has_calls([ mock.call("this is line 0\nthis is line 1\n", 0) ])

    @mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._output_chunk")
    @mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._get_next_downloader")
    def test_generate_chunked_data_unaligned_multiline(self, downloader, outputter):
        downloader.return_value = [ [ b"this is line 0\nthis is line 1\nthis is line 2" ] ]
        self.producer.generate_chunked_data()
        outputter.assert_has_calls([ mock.call("this is line 0\nthis is line 1\n", 0) ])

    def int_generator(self, n):
        for i in range(n):
            yield i

    def get_object_subrange(self, args):
        return next(self.get_obj_subrange_generator)

    def test_multipart_downloader_4_2(self):
        self.producer.chunk_size = 4
        self.producer.num_workers = 2
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1, 2])

    def test_multipart_downloader_4_1(self):
        self.producer.chunk_size = 4
        self.producer.num_workers = 1
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1, 2])

    def test_multipart_downloader_4_4(self):
        self.producer.chunk_size = 4
        self.producer.num_workers = 4
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1, 2])

    def test_multipart_downloader_4_8(self):
        self.producer.chunk_size = 4
        self.producer.num_workers = 8
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1, 2])

    def test_multipart_downloader_8_2(self):
        self.producer.chunk_size = 8
        self.producer.num_workers = 2
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1])

    def test_multipart_downloader_8_4(self):
        self.producer.chunk_size = 8
        self.producer.num_workers = 4
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1])

    def test_multipart_downloader_8_1(self):
        self.producer.chunk_size = 8
        self.producer.num_workers = 4
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1])

    def test_multipart_downloader_8_5(self):
        self.producer.chunk_size = 8
        self.producer.num_workers = 5
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1])

    def test_multipart_downloader_8_8(self):
        self.producer.chunk_size = 8
        self.producer.num_workers = 8
        self.get_obj_subrange_generator = self.int_generator(40)

        with mock.patch("osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer._s3_get_object_subrange", wraps=self.get_object_subrange):
            generator = self.producer._s3_multipart_downloader('bucket', 'key', 0, 12)
            self.assertEqual(list(generator), [0, 1])

    def test_get_keys_single(self):
        self.producer.keys = "file.json"
        self.assertEqual(list(self.producer._get_next_key()), ["file.json"])

    def test_get_keys_glob_0(self):
        self.producer.bucket = None
        self.producer.keys = "file*"
        self.producer.s3_client = mock.Mock()
        self.producer.s3_client.list_objects.return_value = { "Contents": [] }
        self.assertEqual(list(self.producer._get_next_key()), [])

    def test_get_keys_glob_1(self):
        self.producer.bucket = None
        self.producer.keys = "file*"
        self.producer.s3_client = mock.Mock()
        self.producer.s3_client.list_objects.return_value = { "Contents": [ { "Key": "file1" } ] }
        self.assertEqual(list(self.producer._get_next_key()), ["file1"])

    def test_get_keys_glob_2(self):
        self.producer.bucket = None
        self.producer.keys = "file*"
        self.producer.s3_client = mock.Mock()
        self.producer.s3_client.list_objects.return_value = { "Contents": [ { "Key": "file1" }, { "Key": "file2" } ] }
        self.assertEqual(list(self.producer._get_next_key()), ["file1", "file2"])
