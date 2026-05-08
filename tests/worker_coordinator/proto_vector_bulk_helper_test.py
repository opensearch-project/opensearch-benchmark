# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import json
import random
from unittest import TestCase

import cbor2
from opensearch.protobufs.schemas import common_pb2
from osbenchmark.worker_coordinator.proto_helpers.ProtoVectorBulkHelper import (
    ProtoVectorBulkHelper, _serialize_doc_dict
)


class ProtoVectorBulkHelperTests(TestCase):
    def _make_vector_body(self, num_docs=2, dim=4):
        """Build a body in the same format as BulkVectorsFromDataSetParamSource."""
        body = []
        for i in range(num_docs):
            body.append({"index": {"_index": "target_index", "_id": i}})
            body.append({"target_field": [float(x) for x in range(dim)]})
        return body

    def test_build_proto_request_json_default(self):
        body = self._make_vector_body(num_docs=2, dim=3)
        params = {"index": "target_index", "body": body}

        result = ProtoVectorBulkHelper.build_proto_request(params)

        self.assertIsInstance(result, common_pb2.BulkRequest)
        self.assertEqual(result.index, "target_index")
        self.assertEqual(len(result.bulk_request_body), 2)
        doc0 = json.loads(result.bulk_request_body[0].object)
        self.assertEqual(doc0, {"target_field": [0.0, 1.0, 2.0]})

    def test_build_proto_request_cbor(self):
        body = self._make_vector_body(num_docs=2, dim=4)
        params = {"index": "target_index", "body": body, "document-format": "cbor"}

        result = ProtoVectorBulkHelper.build_proto_request(params)

        self.assertEqual(len(result.bulk_request_body), 2)
        doc0 = cbor2.loads(result.bulk_request_body[0].object)
        self.assertEqual(doc0, {"target_field": [0.0, 1.0, 2.0, 3.0]})
        doc1 = cbor2.loads(result.bulk_request_body[1].object)
        self.assertEqual(doc1, {"target_field": [0.0, 1.0, 2.0, 3.0]})

    def test_build_proto_request_extracts_index_from_action_metadata(self):
        body = [
            {"index": {"_index": "my-vector-index", "_id": 0}},
            {"target_field": [1.0, 2.0]}
        ]
        # No index in params — should extract from action metadata
        params = {"body": body}

        result = ProtoVectorBulkHelper.build_proto_request(params)

        self.assertEqual(result.index, "my-vector-index")

    def test_cbor_smaller_than_json_for_vectors(self):
        """CBOR should be smaller for float vectors with realistic values."""
        dim = 768
        random.seed(42)
        doc = {"target_field": [random.uniform(-1.0, 1.0) for _ in range(dim)]}

        json_bytes = json.dumps(doc).encode('utf-8')
        cbor_bytes = cbor2.dumps(doc)

        self.assertLess(len(cbor_bytes), len(json_bytes))

    def test_cbor_preserves_vector_values(self):
        vec = [0.123456, -0.789012, 1.0, 0.0, -1.5]
        doc = {"target_field": vec, "id": 42}
        params = {
            "index": "target_index",
            "body": [{"index": {}}, doc],
            "document-format": "cbor"
        }

        result = ProtoVectorBulkHelper.build_proto_request(params)
        decoded = cbor2.loads(result.bulk_request_body[0].object)

        self.assertEqual(decoded["id"], 42)
        for expected, actual in zip(vec, decoded["target_field"]):
            self.assertAlmostEqual(expected, actual, places=5)

    def test_unsupported_format_raises(self):
        params = {
            "index": "target_index",
            "body": [{"index": {}}, {"f": [1.0]}],
            "document-format": "smile"
        }
        with self.assertRaises(ValueError) as ctx:
            ProtoVectorBulkHelper.build_proto_request(params)
        self.assertIn("smile", str(ctx.exception))

    def test_build_stats_success(self):
        resp = common_pb2.BulkResponse()
        resp.took = 50
        for _ in range(3):
            item = common_pb2.Item()
            item.index.status = 201
            resp.items.append(item)

        params = {"index": "target_index", "size": 3}
        result = ProtoVectorBulkHelper.build_stats(resp, params)

        self.assertTrue(result["success"])
        self.assertEqual(result["success-count"], 3)
        self.assertEqual(result["error-count"], 0)
        self.assertEqual(result["took"], 50)
        self.assertEqual(result["weight"], 3)

    def test_build_stats_error(self):
        resp = common_pb2.BulkResponse()
        resp.errors = True
        params = {"index": "target_index", "size": 10}
        result = ProtoVectorBulkHelper.build_stats(resp, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-count"], 10)
        self.assertEqual(result["error-type"], "bulk")


class SerializeDocDictTests(TestCase):
    def test_json(self):
        doc = {"field": [1.0, 2.0]}
        result = _serialize_doc_dict(doc, "json")
        self.assertEqual(json.loads(result), doc)

    def test_cbor(self):
        doc = {"field": [1.0, 2.0]}
        result = _serialize_doc_dict(doc, "cbor")
        self.assertEqual(cbor2.loads(result), doc)
