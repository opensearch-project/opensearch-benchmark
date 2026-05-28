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

import cbor2
from opensearch.protobufs.schemas import common_pb2
from osbenchmark.worker_coordinator.proto_helpers.ProtoBulkHelper import ProtoBulkHelper, _serialize_doc

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

    def test_build_proto_request_defaults_to_json(self):
        params = {
            "index": "test-index",
            "body": b'{"index": {}}\n{"key": "val"}\n'
        }

        result = ProtoBulkHelper.build_proto_request(params)

        self.assertEqual(result.bulk_request_body[0].object, b'{"key": "val"}')

    def test_build_proto_request_cbor_single_document(self):
        params = {
            "index": "test-index",
            "body": b'{"index": {"_index": "test-index"}}\n{"field1": "value1", "field2": 42}\n',
            "document-format": "cbor"
        }

        result = ProtoBulkHelper.build_proto_request(params)

        self.assertEqual(len(result.bulk_request_body), 1)
        decoded = cbor2.loads(result.bulk_request_body[0].object)
        self.assertEqual(decoded, {"field1": "value1", "field2": 42})

    def test_build_proto_request_cbor_multiple_documents(self):
        params = {
            "index": "test-index",
            "body": (b'{"index": {}}\n'
                    b'{"name": "alice", "age": 30}\n'
                    b'{"index": {}}\n'
                    b'{"name": "bob", "age": 25}\n'),
            "document-format": "cbor"
        }

        result = ProtoBulkHelper.build_proto_request(params)

        self.assertEqual(len(result.bulk_request_body), 2)
        self.assertEqual(cbor2.loads(result.bulk_request_body[0].object), {"name": "alice", "age": 30})
        self.assertEqual(cbor2.loads(result.bulk_request_body[1].object), {"name": "bob", "age": 25})

    def test_build_proto_request_cbor_preserves_types(self):
        params = {
            "index": "test-index",
            "body": b'{"index": {}}\n{"str": "hello", "int": 1, "float": 1.5, "bool": true, "null": null}\n',
            "document-format": "cbor"
        }

        result = ProtoBulkHelper.build_proto_request(params)
        decoded = cbor2.loads(result.bulk_request_body[0].object)

        self.assertEqual(decoded["str"], "hello")
        self.assertEqual(decoded["int"], 1)
        self.assertEqual(decoded["float"], 1.5)
        self.assertIs(decoded["bool"], True)
        self.assertIsNone(decoded["null"])

    def test_build_proto_request_cbor_is_smaller_than_json(self):
        doc_json = '{"field1": "value1", "field2": "value2", "number": 12345}'
        body = b'{"index": {}}\n' + doc_json.encode('utf-8') + b'\n'

        result_json = ProtoBulkHelper.build_proto_request({"index": "i", "body": body})
        result_cbor = ProtoBulkHelper.build_proto_request({"index": "i", "body": body, "document-format": "cbor"})

        self.assertLess(
            len(result_cbor.bulk_request_body[0].object),
            len(result_json.bulk_request_body[0].object)
        )

    def test_build_proto_request_unsupported_format_raises(self):
        params = {
            "index": "test-index",
            "body": b'{"index": {}}\n{"key": "val"}\n',
            "document-format": "smile"
        }

        with self.assertRaises(ValueError) as ctx:
            ProtoBulkHelper.build_proto_request(params)

        self.assertIn("smile", str(ctx.exception))
        self.assertIn("Unsupported document-format", str(ctx.exception))

    def test_serialize_doc_json(self):
        result = _serialize_doc('{"key": "value"}', "json")
        self.assertEqual(result, b'{"key": "value"}')

    def test_serialize_doc_cbor(self):
        result = _serialize_doc('{"key": "value"}', "cbor")
        decoded = cbor2.loads(result)
        self.assertEqual(decoded, {"key": "value"})

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
