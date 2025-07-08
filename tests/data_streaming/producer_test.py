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

from unittest import TestCase
from unittest.mock import patch

from osbenchmark.cloud_provider.vendors.s3_data_producer import S3DataProducer

class TestS3DataProducer(TestCase):

    # The S3 data producer downloads data from S3 as an HTTP response body.
    # For the purposes of unit testing, model reading the response by reading from a string instead.
    # pylint: disable = arguments-differ
    @patch('osbenchmark.cloud_provider.vendors.s3_data_producer.S3DataProducer.__init__', return_value=None)
    def setUp(self, mock_init):
        self.obj = S3DataProducer('bucket', 'key', None)
        self.obj.streaming_body = io.StringIO("Lorem ipsum dolor sit amet")

    def test_data_producer_get_5(self):
        self.assertEqual(self.obj.get_data(5), "Lorem")

    def test_data_producer_get_5_again(self):
        self.assertEqual(self.obj.get_data(5), "Lorem")

    def test_data_producer_get_11(self):
        self.assertEqual(self.obj.get_data(11), "Lorem ipsum")

    def test_data_producer_get_all(self):
        self.assertEqual(self.obj.get_data(100), "Lorem ipsum dolor sit amet")

    def test_data_producer_get_nothing(self):
        self.assertEqual(self.obj.get_data(0), "")

    def test_data_producer_get_chunks(self):
        self.assertEqual(self.obj.get_data(5), "Lorem")
        self.assertEqual(self.obj.get_data(6), " ipsum")
        self.assertEqual(self.obj.get_data(6), " dolor")
        self.assertEqual(self.obj.get_data(4), " sit")
        self.assertEqual(self.obj.get_data(8), " amet")
        self.assertEqual(self.obj.get_data(8), "")
        self.assertEqual(self.obj.get_data(8), "")

    def test_data_producer_empty_stream(self):
        self.obj.streaming_body = io.StringIO("")
        self.assertEqual(self.obj.get_data(8), "")
        self.assertEqual(self.obj.get_data(8), "")
