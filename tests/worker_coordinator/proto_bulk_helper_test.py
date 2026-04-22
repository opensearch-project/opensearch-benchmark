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

from unittest import TestCase

import numpy as np
from opensearch.protobufs.schemas import common_pb2
from osbenchmark.worker_coordinator.proto_helpers.ProtoBulkHelper import ProtoBulkHelper, _build_float_binary_le

class ProtoBulkHelperTests(TestCase):
    def test_build_proto_request_single_document(self):
        params = {
            "index": "test-index",
            "body": b'{"index": {"_index": "test-index"}}\n{"field1": "value1", "field2": "value2"}\n'
        }

        result = ProtoBulkHelper.build_proto_request(params)

        self.assertIsInstance(result, common_pb2.BulkRequest)
        self.assertEqual(result.index, "test-index")
        self.assertEqual(len(result.bulk_request_body), 1)
        self.assertEqual(result.bulk_request_body[0].object, b'{"field1": "value1", "field2": "value2"}')
        self.assertTrue(result.bulk_request_body[0].operation_container.HasField("index"))

    def test_build_proto_request_multiple_documents(self):
        params = {
            "index": "test-index",
            "body": (b'{"index": {"_index": "test-index"}}\n'
                    b'{"field1": "value1"}\n'
                    b'{"index": {"_index": "test-index"}}\n'
                    b'{"field1": "value2"}\n')
        }

        result = ProtoBulkHelper.build_proto_request(params)

        self.assertIsInstance(result, common_pb2.BulkRequest)
        self.assertEqual(result.index, "test-index")
        self.assertEqual(len(result.bulk_request_body), 2)
        self.assertEqual(result.bulk_request_body[0].object, b'{"field1": "value1"}')
        self.assertEqual(result.bulk_request_body[1].object, b'{"field1": "value2"}')

    def test_build_stats_success_response(self):
        mock_bulk_response = common_pb2.BulkResponse()
        mock_bulk_response.took = 100

        for _ in range(3):
            item = common_pb2.Item()
            item.index.status = 201
            mock_bulk_response.items.append(item)

        params = {
            "index": "test-index",
            "bulk-size": 3,
            "unit": "ops"
        }

        result = ProtoBulkHelper.build_stats(mock_bulk_response, params)

        expected = {
            "index": "test-index",
            "weight": 3,
            "unit": "ops",
            "took": 100,
            "success": True,
            "success-count": 3,
            "error-count": 0,
        }

        self.assertEqual(result, expected)

    def test_build_stats_bulk_error_response_status(self):
        mock_bulk_response = common_pb2.BulkResponse()
        mock_bulk_response.errors = True

        params = {
            "index": "test-index",
            "bulk-size": 15,
            "unit": "ops"
        }

        result = ProtoBulkHelper.build_stats(mock_bulk_response, params)

        expected = {
            "index": "test-index",
            "weight": 15,
            "unit": "ops",
            "took": None,
            "success": False,
            "success-count": 0,
            "error-count": 15,
            "error-type": "bulk"
        }

        self.assertEqual(result, expected)

    def test_build_stats_detailed_results_raises_exception(self):
        mock_bulk_response = common_pb2.BulkResponse()

        params = {"detailed-results": True}

        with self.assertRaises(Exception) as ctx:
            ProtoBulkHelper.build_stats(mock_bulk_response, params)

        self.assertIn("Detailed results not supported for gRPC bulk requests", str(ctx.exception))

    # --- extra_field_values / vector bulk tests ---

    def test_build_float_binary_le_basic(self):
        vec = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        result = _build_float_binary_le(vec)
        self.assertEqual(result.dimension, 3)
        self.assertEqual(len(result.bytes_le), 12)  # 3 * 4 bytes
        # Verify round-trip: decode the LE bytes back to floats
        decoded = np.frombuffer(result.bytes_le, dtype='<f4')
        np.testing.assert_array_equal(decoded, vec)

    def test_build_float_binary_le_from_float64(self):
        """float64 input should be converted to float32."""
        vec = np.array([1.5, 2.5], dtype=np.float64)
        result = _build_float_binary_le(vec)
        self.assertEqual(result.dimension, 2)
        decoded = np.frombuffer(result.bytes_le, dtype='<f4')
        np.testing.assert_array_almost_equal(decoded, [1.5, 2.5])

    def test_build_proto_vector_request_single_vector(self):
        vec = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        params = {
            "index": "test-index",
            "field": "my_vector",
            "vectors": [vec],
        }

        result = ProtoBulkHelper.build_proto_vector_request(params)

        self.assertIsInstance(result, common_pb2.BulkRequest)
        self.assertEqual(result.index, "test-index")
        self.assertEqual(len(result.bulk_request_body), 1)

        body = result.bulk_request_body[0]
        self.assertEqual(body.object, b'{}')
        self.assertTrue(body.operation_container.HasField("index"))
        self.assertIn("my_vector", body.extra_field_values)

        bfv = body.extra_field_values["my_vector"]
        self.assertTrue(bfv.HasField("float_array_value"))
        fav = bfv.float_array_value
        self.assertTrue(fav.HasField("binary_le"))
        self.assertEqual(fav.binary_le.dimension, 3)
        decoded = np.frombuffer(fav.binary_le.bytes_le, dtype='<f4')
        np.testing.assert_array_equal(decoded, vec)

    def test_build_proto_vector_request_multiple_vectors(self):
        vecs = [
            np.array([1.0, 0.0], dtype=np.float32),
            np.array([0.0, 1.0], dtype=np.float32),
            np.array([0.5, 0.5], dtype=np.float32),
        ]
        params = {
            "index": "vec-index",
            "field": "embedding",
            "vectors": vecs,
        }

        result = ProtoBulkHelper.build_proto_vector_request(params)

        self.assertEqual(len(result.bulk_request_body), 3)
        for i, body in enumerate(result.bulk_request_body):
            self.assertEqual(body.object, b'{}')
            bfv = body.extra_field_values["embedding"]
            decoded = np.frombuffer(bfv.float_array_value.binary_le.bytes_le, dtype='<f4')
            np.testing.assert_array_equal(decoded, vecs[i])

    def test_build_proto_vector_request_high_dimensional(self):
        vec = np.random.rand(768).astype(np.float32)
        params = {
            "index": "test-index",
            "field": "vec",
            "vectors": [vec],
        }

        result = ProtoBulkHelper.build_proto_vector_request(params)

        body = result.bulk_request_body[0]
        fav = body.extra_field_values["vec"].float_array_value
        self.assertEqual(fav.binary_le.dimension, 768)
        self.assertEqual(len(fav.binary_le.bytes_le), 768 * 4)
        decoded = np.frombuffer(fav.binary_le.bytes_le, dtype='<f4')
        np.testing.assert_array_equal(decoded, vec)
