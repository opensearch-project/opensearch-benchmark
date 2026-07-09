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
# pylint: disable=protected-access

import unittest.mock as mock
from unittest import TestCase

from osbenchmark.engine.milvus.runners import (
    MilvusBulkVectorDataSet,
    MilvusBulkIndex,
    MilvusVectorSearch,
    MilvusQuery,
    MilvusCreateIndex,
    MilvusDeleteIndex,
    MilvusRefresh,
    MilvusForceMerge,
    MilvusClusterHealth,
    MilvusIndicesStats,
    MilvusWarmupRunner,
    MilvusNoOp,
    register_milvus_runners,
)
from tests import run_async


def _make_milvus_client(**overrides):
    """Create a standard mock milvus_client with common attributes."""
    client = mock.AsyncMock()
    client._collection_name = overrides.get("collection_name", "target_index")
    client.client_options = overrides.get("client_options", {})
    return client


def _milvus_search_response(hits=None):
    """Build a raw Milvus search response (List[List[Hit-like dicts]])."""
    if hits is None:
        hits = [{"doc_id": 1, "distance": 0.95}]
    return [hits]


def _converted_response(total_value=1, hits_list=None):
    """Build a converted OpenSearch-style response (output of convert_milvus_search_response)."""
    if hits_list is None:
        hits_list = [{"_index": "target_index", "_id": "1", "_score": 0.95}]
    return {
        "took": 0,
        "timed_out": False,
        "hits": {
            "total": {"value": total_value, "relation": "eq"},
            "hits": hits_list,
        },
    }


# ---------------------------------------------------------------------------
# MilvusBulkVectorDataSet
# ---------------------------------------------------------------------------
class MilvusBulkVectorDataSetTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.parse_vector_body")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_success_with_parse_vector_body(self, mock_ctx, mock_parse):
        milvus_client = _make_milvus_client()
        mock_parse.return_value = (
            [{"doc_id": 0, "embedding": [1.0, 2.0]}, {"doc_id": 1, "embedding": [3.0, 4.0]}],
            "vectors",
        )
        milvus_client.bulk.return_value = {"items": [{"_id": 0}, {"_id": 1}], "errors": False}

        body = [
            {"index": {"_index": "vectors", "_id": 0}},
            {"embedding": [1.0, 2.0]},
            {"index": {"_index": "vectors", "_id": 1}},
            {"embedding": [3.0, 4.0]},
        ]
        params = {"body": body, "size": 2}

        runner = MilvusBulkVectorDataSet()
        result = await runner(milvus_client, params)

        self.assertEqual(result["weight"], 2)
        self.assertEqual(result["unit"], "docs")
        self.assertTrue(result["success"])
        self.assertEqual(result["error-count"], 0)
        milvus_client.bulk.assert_called_once()
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.parse_vector_body")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_empty_body_returns_early(self, mock_ctx, mock_parse):
        milvus_client = _make_milvus_client()
        mock_parse.return_value = ([], None)

        params = {"body": [], "size": 0}

        runner = MilvusBulkVectorDataSet()
        result = await runner(milvus_client, params)

        self.assertTrue(result["success"])
        self.assertEqual(result["weight"], 0)
        milvus_client.bulk.assert_not_called()

    @mock.patch("osbenchmark.engine.milvus.runners.parse_vector_body")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_errors_in_bulk_response(self, mock_ctx, mock_parse):
        milvus_client = _make_milvus_client()
        mock_parse.return_value = (
            [{"doc_id": 0, "embedding": [1.0]}, {"doc_id": 1, "embedding": [2.0]}],
            "vectors",
        )
        # Only 1 item returned but 2 sent => error_count=1, and errors=True
        milvus_client.bulk.return_value = {"items": [{"_id": 0}], "errors": True}

        params = {"body": [], "size": 2}

        runner = MilvusBulkVectorDataSet()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-count"], 1)

    @mock.patch("osbenchmark.engine.milvus.runners.parse_vector_body")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_exception_returns_failure(self, mock_ctx, mock_parse):
        milvus_client = _make_milvus_client()
        mock_parse.return_value = (
            [{"doc_id": 0, "embedding": [1.0]}],
            "vectors",
        )
        milvus_client.bulk.side_effect = ConnectionError("connection lost")

        params = {"body": [], "size": 1}

        runner = MilvusBulkVectorDataSet()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-type"], "milvus")
        self.assertIn("connection lost", result["error-description"])
        mock_ctx.on_request_end.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.parse_vector_body")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_result_dict_format(self, mock_ctx, mock_parse):
        milvus_client = _make_milvus_client()
        mock_parse.return_value = (
            [{"doc_id": 0, "embedding": [1.0]}],
            "vectors",
        )
        milvus_client.bulk.return_value = {"items": [{"_id": 0}], "errors": False}

        params = {"body": [], "size": 5}

        runner = MilvusBulkVectorDataSet()
        result = await runner(milvus_client, params)

        self.assertIsInstance(result, dict)
        self.assertIn("weight", result)
        self.assertIn("unit", result)
        self.assertIn("success", result)
        self.assertIn("error-count", result)

    def test_repr(self):
        runner = MilvusBulkVectorDataSet()
        self.assertEqual(repr(runner), "milvus-bulk-vector-data-set")


# ---------------------------------------------------------------------------
# MilvusBulkIndex
# ---------------------------------------------------------------------------
class MilvusBulkIndexTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_bulk_index_success(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.bulk.return_value = {"errors": False}

        params = {"body": [{"doc_id": 1}], "bulk-size": 10, "unit": "docs", "index": "myindex"}

        runner = MilvusBulkIndex()
        result = await runner(milvus_client, params)

        self.assertTrue(result["success"])
        self.assertEqual(result["weight"], 10)
        self.assertEqual(result["unit"], "docs")
        milvus_client.bulk.assert_called_once_with(
            body=[{"doc_id": 1}], index="myindex"
        )
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_bulk_index_errors_flag(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.bulk.return_value = {"errors": True}

        params = {"body": [], "bulk-size": 5, "index": "myindex"}

        runner = MilvusBulkIndex()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_bulk_index_exception(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.bulk.side_effect = RuntimeError("grpc error")

        params = {"body": [], "bulk-size": 1, "index": "myindex"}

        runner = MilvusBulkIndex()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-type"], "milvus")
        self.assertIn("grpc error", result["error-description"])
        mock_ctx.on_request_end.assert_called_once()

    def test_repr(self):
        runner = MilvusBulkIndex()
        self.assertEqual(repr(runner), "milvus-bulk-index")


# ---------------------------------------------------------------------------
# MilvusVectorSearch
# ---------------------------------------------------------------------------
class MilvusVectorSearchTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_nested_knn_extraction(self, mock_ctx, mock_build, mock_convert):
        """KNN nested under body.query.knn with field_name -> {vector, k}."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(total_value=5, hits_list=[
            {"_id": "1", "_score": 0.9}, {"_id": "2", "_score": 0.8},
            {"_id": "3", "_score": 0.7}, {"_id": "4", "_score": 0.6},
            {"_id": "5", "_score": 0.5},
        ])

        params = {
            "body": {"query": {"knn": {"embedding": {"vector": [1.0, 2.0, 3.0], "k": 5}}}},
            "index": "myindex",
            "k": 100,
        }

        runner = MilvusVectorSearch()
        result = await runner(milvus_client, params)

        self.assertTrue(result["success"])
        self.assertEqual(result["hits"], 5)
        # Verify search body was built with correct field name and vector
        call_kwargs = milvus_client.search.call_args[1]
        self.assertEqual(call_kwargs["body"]["anns_field"], "embedding")
        self.assertEqual(call_kwargs["body"]["data"], [[1.0, 2.0, 3.0]])
        # k=5 from knn config should override the params k=100
        self.assertEqual(call_kwargs["body"]["limit"], 5)

    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_flat_knn_extraction(self, mock_ctx, mock_build, mock_convert):
        """KNN at body.knn with flat vector/field keys."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(total_value=2)

        params = {
            "body": {"knn": {"vector": [1.0, 2.0], "field": "my_vec"}},
            "index": "myindex",
            "k": 10,
        }

        runner = MilvusVectorSearch()
        result = await runner(milvus_client, params)

        self.assertTrue(result["success"])
        call_kwargs = milvus_client.search.call_args[1]
        self.assertEqual(call_kwargs["body"]["anns_field"], "my_vec")
        self.assertEqual(call_kwargs["body"]["data"], [[1.0, 2.0]])

    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_no_vector_returns_failure(self, mock_ctx, mock_build):
        """Missing vector in body returns failure immediately without searching."""
        milvus_client = _make_milvus_client()

        params = {"body": {"query": {"match_all": {}}}, "index": "myindex"}

        runner = MilvusVectorSearch()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["hits"], 0)
        milvus_client.search.assert_not_called()

    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_numpy_tolist_conversion(self, mock_ctx, mock_build, mock_convert):
        """Numpy arrays are converted via .tolist() before sending to Milvus."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(total_value=1)

        # Simulate numpy array with tolist method
        numpy_like = mock.MagicMock()
        numpy_like.tolist.return_value = [1.0, 2.0, 3.0]

        params = {
            "body": {"knn": {"vector": numpy_like, "field": "embedding"}},
            "index": "myindex",
            "k": 10,
        }

        runner = MilvusVectorSearch()
        await runner(milvus_client, params)

        numpy_like.tolist.assert_called_once()
        call_kwargs = milvus_client.search.call_args[1]
        self.assertEqual(call_kwargs["body"]["data"], [[1.0, 2.0, 3.0]])

    @mock.patch("osbenchmark.engine.milvus.runners.calculate_topk_recall")
    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_recall_calculation(self, mock_ctx, mock_build, mock_convert, mock_recall):
        """Recall is calculated when neighbors and k are provided."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(
            total_value=3,
            hits_list=[
                {"_id": "0", "_score": 1.0},
                {"_id": "1", "_score": 0.9},
                {"_id": "5", "_score": 0.8},
            ],
        )
        # recall@k=2/3, recall@1=1.0
        mock_recall.side_effect = [2.0 / 3.0, 1.0]

        params = {
            "body": {"knn": {"embedding": {"vector": [1.0, 2.0], "k": 3}}},
            "index": "myindex",
            "k": 3,
            "neighbors": ["0", "1", "2"],
        }

        runner = MilvusVectorSearch()
        result = await runner(milvus_client, params)

        self.assertAlmostEqual(result["recall@k"], 2.0 / 3.0, places=5)
        self.assertAlmostEqual(result["recall@1"], 1.0, places=5)
        self.assertEqual(mock_recall.call_count, 2)

    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_recall_not_calculated_without_neighbors(self, mock_ctx, mock_build, mock_convert):
        """Recall keys absent when neighbors param is missing."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(total_value=1)

        params = {
            "body": {"knn": {"embedding": {"vector": [1.0], "k": 10}}},
            "index": "myindex",
            "k": 10,
        }

        runner = MilvusVectorSearch()
        result = await runner(milvus_client, params)

        self.assertNotIn("recall@k", result)
        self.assertNotIn("recall@1", result)

    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_exception_returns_failure(self, mock_ctx, mock_build):
        """Search exceptions are caught and return success=False."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        milvus_client.search.side_effect = RuntimeError("grpc timeout")

        params = {
            "body": {"knn": {"embedding": {"vector": [1.0], "k": 5}}},
            "index": "myindex",
        }

        runner = MilvusVectorSearch()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-type"], "milvus")
        self.assertIn("grpc timeout", result["error-description"])
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_detailed_results(self, mock_ctx, mock_build, mock_convert):
        """detailed-results flag adds hits_total and took to result."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(total_value=42)

        params = {
            "body": {"knn": {"embedding": {"vector": [1.0], "k": 10}}},
            "index": "myindex",
            "detailed-results": True,
        }

        runner = MilvusVectorSearch()
        result = await runner(milvus_client, params)

        self.assertEqual(result["hits_total"], 42)
        self.assertIn("took", result)

    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_no_detailed_results_by_default(self, mock_ctx, mock_build, mock_convert):
        """Without detailed-results, extra keys are absent."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(total_value=10)

        params = {
            "body": {"knn": {"embedding": {"vector": [1.0], "k": 10}}},
            "index": "myindex",
        }

        runner = MilvusVectorSearch()
        result = await runner(milvus_client, params)

        self.assertNotIn("hits_total", result)

    def test_extract_doc_id_returns_string(self):
        self.assertEqual(MilvusVectorSearch._extract_doc_id(123), "123")
        self.assertEqual(MilvusVectorSearch._extract_doc_id("abc"), "abc")
        self.assertEqual(MilvusVectorSearch._extract_doc_id(0), "0")

    def test_repr(self):
        runner = MilvusVectorSearch()
        self.assertEqual(repr(runner), "milvus-vector-search")


# ---------------------------------------------------------------------------
# MilvusQuery
# ---------------------------------------------------------------------------
class MilvusQueryTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_knn_extraction(self, mock_ctx, mock_build, mock_convert):
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(total_value=3)

        params = {
            "body": {"query": {"knn": {"embedding": {"vector": [1.0, 2.0], "k": 5}}}},
            "index": "myindex",
        }

        runner = MilvusQuery()
        result = await runner(milvus_client, params)

        self.assertEqual(result["hits"], 3)
        call_kwargs = milvus_client.search.call_args[1]
        self.assertEqual(call_kwargs["body"]["anns_field"], "embedding")
        self.assertEqual(call_kwargs["body"]["data"], [[1.0, 2.0]])
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_no_vector_returns_success_with_zero_hits(self, mock_ctx):
        """MilvusQuery returns success=True with 0 hits when no vector is found."""
        milvus_client = _make_milvus_client()

        params = {"body": {"query": {"match_all": {}}}, "index": "myindex"}

        runner = MilvusQuery()
        result = await runner(milvus_client, params)

        self.assertTrue(result["success"])
        self.assertEqual(result["hits"], 0)
        milvus_client.search.assert_not_called()

    @mock.patch("osbenchmark.engine.milvus.runners.convert_milvus_search_response")
    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_k_from_body_size(self, mock_ctx, mock_build, mock_convert):
        """k falls back to body.size when not in params."""
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        mock_convert.return_value = _converted_response(total_value=1)

        params = {
            "body": {"knn": {"vector": [1.0], "field": "embedding"}, "size": 50},
            "index": "myindex",
        }

        runner = MilvusQuery()
        await runner(milvus_client, params)

        call_kwargs = milvus_client.search.call_args[1]
        self.assertEqual(call_kwargs["body"]["limit"], 50)

    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_exception_returns_failure(self, mock_ctx, mock_build):
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}
        milvus_client.search.side_effect = RuntimeError("timeout")

        params = {
            "body": {"knn": {"vector": [1.0], "field": "embedding"}},
            "index": "myindex",
        }

        runner = MilvusQuery()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-type"], "milvus")
        mock_ctx.on_request_end.assert_called_once()

    def test_repr(self):
        runner = MilvusQuery()
        self.assertEqual(repr(runner), "milvus-query")


# ---------------------------------------------------------------------------
# MilvusCreateIndex
# ---------------------------------------------------------------------------
class MilvusCreateIndexTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.build_collection_schema")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_create_from_indices_list(self, mock_ctx, mock_schema):
        milvus_client = _make_milvus_client()
        mock_schema.return_value = (mock.Mock(), mock.Mock(), "target_index")

        indices = [("index1", {"mappings": {}}), ("index2", {"mappings": {}})]
        params = {"indices": indices}

        runner = MilvusCreateIndex()
        result = await runner(milvus_client, params)

        self.assertEqual(milvus_client.indices.create.call_count, 2)
        self.assertEqual(result["weight"], 2)
        self.assertEqual(result["unit"], "ops")
        self.assertTrue(result["success"])
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.build_collection_schema")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_create_from_single_index(self, mock_ctx, mock_schema):
        milvus_client = _make_milvus_client()
        mock_schema.return_value = (mock.Mock(), mock.Mock(), "target_index")

        params = {"index": "myindex", "body": {"mappings": {}}}

        runner = MilvusCreateIndex()
        result = await runner(milvus_client, params)

        milvus_client.indices.create.assert_called_once()
        call_kwargs = milvus_client.indices.create.call_args[1]
        self.assertEqual(call_kwargs["index"], "myindex")
        self.assertEqual(result["weight"], 1)
        self.assertTrue(result["success"])

    @mock.patch("osbenchmark.engine.milvus.runners.build_collection_schema")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_schema_built_before_timing(self, mock_ctx, mock_schema):
        """build_collection_schema is called before on_request_start."""
        call_order = []
        mock_schema.side_effect = lambda *a, **kw: (
            call_order.append("schema"),
            (mock.Mock(), mock.Mock(), "target_index"),
        )[1]
        mock_ctx.on_request_start.side_effect = lambda: call_order.append("timing_start")

        milvus_client = _make_milvus_client()
        params = {"indices": [("idx1", {})]}

        runner = MilvusCreateIndex()
        await runner(milvus_client, params)

        self.assertEqual(call_order.index("schema"), 0)
        self.assertGreater(call_order.index("timing_start"), call_order.index("schema"))

    @mock.patch("osbenchmark.engine.milvus.runners.build_collection_schema")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_create_exception_returns_failure(self, mock_ctx, mock_schema):
        milvus_client = _make_milvus_client()
        mock_schema.return_value = (mock.Mock(), mock.Mock(), "target_index")
        milvus_client.indices.create.side_effect = RuntimeError("already exists")

        params = {"indices": [("idx1", {})]}

        runner = MilvusCreateIndex()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-type"], "milvus")
        mock_ctx.on_request_end.assert_called_once()

    def test_repr(self):
        runner = MilvusCreateIndex()
        self.assertEqual(repr(runner), "milvus-create-index")


# ---------------------------------------------------------------------------
# MilvusDeleteIndex
# ---------------------------------------------------------------------------
class MilvusDeleteIndexTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_delete_from_list(self, mock_ctx):
        milvus_client = _make_milvus_client()

        params = {"indices": ["index1", "index2"]}

        runner = MilvusDeleteIndex()
        result = await runner(milvus_client, params)

        self.assertEqual(milvus_client.indices.delete.call_count, 2)
        self.assertEqual(result["weight"], 2)
        self.assertTrue(result["success"])
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_delete_single(self, mock_ctx):
        milvus_client = _make_milvus_client()

        params = {"index": "myindex"}

        runner = MilvusDeleteIndex()
        result = await runner(milvus_client, params)

        milvus_client.indices.delete.assert_called_once_with(index="myindex")
        self.assertEqual(result["weight"], 1)
        self.assertTrue(result["success"])

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_only_if_exists_true_and_exists(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.indices.exists.return_value = True

        params = {"indices": ["index1"], "only-if-exists": True}

        runner = MilvusDeleteIndex()
        result = await runner(milvus_client, params)

        milvus_client.indices.exists.assert_called_once_with(index="index1")
        milvus_client.indices.delete.assert_called_once_with(index="index1")
        self.assertEqual(result["weight"], 1)

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_only_if_exists_true_and_not_exists(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.indices.exists.return_value = False

        params = {"indices": ["index1"], "only-if-exists": True}

        runner = MilvusDeleteIndex()
        result = await runner(milvus_client, params)

        milvus_client.indices.exists.assert_called_once_with(index="index1")
        milvus_client.indices.delete.assert_not_called()
        self.assertEqual(result["weight"], 0)
        self.assertTrue(result["success"])

    def test_repr(self):
        runner = MilvusDeleteIndex()
        self.assertEqual(repr(runner), "milvus-delete-index")


# ---------------------------------------------------------------------------
# MilvusWarmupRunner
# ---------------------------------------------------------------------------
class MilvusWarmupRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_calls_load_collection(self, mock_ctx, mock_build):
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}

        params = {"index": "target_index", "target_index_dimension": 3}

        runner = MilvusWarmupRunner()
        result = await runner(milvus_client, params)

        milvus_client.load_collection.assert_called_once_with(collection_name="target_index")
        self.assertTrue(result["success"])

    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_issues_warmup_queries(self, mock_ctx, mock_build):
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}

        params = {"index": "target_index", "target_index_dimension": 3}

        runner = MilvusWarmupRunner()
        await runner(milvus_client, params)

        self.assertEqual(
            milvus_client.search.call_count,
            MilvusWarmupRunner.DEFAULT_WARMUP_QUERIES,
        )

    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_exception_returns_failure(self, mock_ctx, mock_build):
        milvus_client = _make_milvus_client()
        milvus_client.load_collection.side_effect = RuntimeError("collection not found")

        params = {"index": "target_index", "target_index_dimension": 3}

        runner = MilvusWarmupRunner()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-type"], "milvus")
        self.assertIn("collection not found", result["error-description"])
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.build_search_params")
    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_timing_context_set_up(self, mock_ctx, mock_build):
        milvus_client = _make_milvus_client()
        mock_build.return_value = {"search_params": {"params": {"ef": 256}}}

        params = {"index": "target_index", "target_index_dimension": 3}

        runner = MilvusWarmupRunner()
        await runner(milvus_client, params)

        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    def test_repr(self):
        runner = MilvusWarmupRunner()
        self.assertEqual(repr(runner), "warmup-knn-indices")


# ---------------------------------------------------------------------------
# MilvusRefresh
# ---------------------------------------------------------------------------
class MilvusRefreshTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_calls_refresh(self, mock_ctx):
        milvus_client = _make_milvus_client()

        params = {"index": "myindex"}

        runner = MilvusRefresh()
        result = await runner(milvus_client, params)

        milvus_client.indices.refresh.assert_called_once_with(index="myindex")
        self.assertTrue(result["success"])
        self.assertEqual(result["weight"], 1)
        self.assertEqual(result["unit"], "ops")
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_exception_returns_failure(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.indices.refresh.side_effect = RuntimeError("flush failed")

        params = {"index": "myindex"}

        runner = MilvusRefresh()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertIn("flush failed", result["error-description"])
        mock_ctx.on_request_end.assert_called_once()

    def test_repr(self):
        runner = MilvusRefresh()
        self.assertEqual(repr(runner), "milvus-refresh")


# ---------------------------------------------------------------------------
# MilvusForceMerge
# ---------------------------------------------------------------------------
class MilvusForceMergeTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_calls_forcemerge(self, mock_ctx):
        milvus_client = _make_milvus_client()

        params = {"index": "myindex"}

        runner = MilvusForceMerge()
        result = await runner(milvus_client, params)

        milvus_client.indices.forcemerge.assert_called_once_with(index="myindex")
        self.assertTrue(result["success"])
        self.assertEqual(result["weight"], 1)
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_exception_returns_failure(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.indices.forcemerge.side_effect = RuntimeError("compact failed")

        params = {"index": "myindex"}

        runner = MilvusForceMerge()
        result = await runner(milvus_client, params)

        self.assertFalse(result["success"])
        self.assertIn("compact failed", result["error-description"])
        mock_ctx.on_request_end.assert_called_once()

    def test_repr(self):
        runner = MilvusForceMerge()
        self.assertEqual(repr(runner), "milvus-force-merge")


# ---------------------------------------------------------------------------
# MilvusClusterHealth
# ---------------------------------------------------------------------------
class MilvusClusterHealthTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_green_success(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.cluster.health.return_value = {"status": "green"}

        runner = MilvusClusterHealth()
        result = await runner(milvus_client, {})

        self.assertTrue(result["success"])
        self.assertEqual(result["cluster-status"], "green")
        self.assertEqual(result["relocating-shards"], 0)
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_red_failure(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.cluster.health.return_value = {"status": "red"}

        runner = MilvusClusterHealth()
        result = await runner(milvus_client, {})

        self.assertFalse(result["success"])
        self.assertEqual(result["cluster-status"], "red")

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_exception_returns_red(self, mock_ctx):
        milvus_client = _make_milvus_client()
        milvus_client.cluster.health.side_effect = RuntimeError("connection refused")

        runner = MilvusClusterHealth()
        result = await runner(milvus_client, {})

        self.assertFalse(result["success"])
        self.assertEqual(result["cluster-status"], "red")
        mock_ctx.on_request_end.assert_called_once()

    def test_repr(self):
        runner = MilvusClusterHealth()
        self.assertEqual(repr(runner), "milvus-cluster-health")


# ---------------------------------------------------------------------------
# MilvusNoOp
# ---------------------------------------------------------------------------
class MilvusNoOpTests(TestCase):

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_returns_success(self, mock_ctx):
        runner = MilvusNoOp("put-pipeline")
        result = await runner(_make_milvus_client(), {})

        self.assertEqual(result["weight"], 1)
        self.assertEqual(result["unit"], "ops")
        self.assertTrue(result["success"])

    @mock.patch("osbenchmark.engine.milvus.runners.request_context_holder")
    @run_async
    async def test_timing_context_set_up(self, mock_ctx):
        runner = MilvusNoOp("put-pipeline")
        await runner(_make_milvus_client(), {})

        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    def test_repr_returns_name(self):
        runner = MilvusNoOp("put-pipeline")
        self.assertEqual(repr(runner), "put-pipeline")

    def test_repr_returns_different_name(self):
        runner = MilvusNoOp("delete-pipeline")
        self.assertEqual(repr(runner), "delete-pipeline")


# ---------------------------------------------------------------------------
# register_milvus_runners
# ---------------------------------------------------------------------------
class RegisterMilvusRunnersTests(TestCase):

    @mock.patch("osbenchmark.worker_coordinator.runner.register_runner")
    def test_registers_all_named_operations(self, mock_register):
        from osbenchmark import workload as wl  # pylint: disable=import-outside-toplevel

        register_milvus_runners()

        registered_ops = [call[0][0] for call in mock_register.call_args_list]

        expected_ops = [
            wl.OperationType.Bulk,
            wl.OperationType.Search,
            wl.OperationType.PaginatedSearch,
            wl.OperationType.VectorSearch,
            wl.OperationType.BulkVectorDataSet,
            wl.OperationType.CreateIndex,
            wl.OperationType.DeleteIndex,
            wl.OperationType.IndexStats,
            wl.OperationType.ClusterHealth,
            wl.OperationType.Refresh,
            wl.OperationType.ForceMerge,
            "warmup-knn-indices",
        ]

        for op in expected_ops:
            self.assertIn(op, registered_ops, f"{op} should be registered")

    @mock.patch("osbenchmark.worker_coordinator.runner.register_runner")
    def test_all_registered_as_async(self, mock_register):
        register_milvus_runners()

        for call in mock_register.call_args_list:
            kwargs = call[1]
            self.assertTrue(
                kwargs.get("async_runner", False),
                f"Registration for {call[0][0]} should have async_runner=True",
            )

    @mock.patch("osbenchmark.worker_coordinator.runner.register_runner")
    def test_correct_runner_types(self, mock_register):
        from osbenchmark import workload as wl  # pylint: disable=import-outside-toplevel

        register_milvus_runners()

        type_map = {}
        for call in mock_register.call_args_list:
            op_type = call[0][0]
            runner_instance = call[0][1]
            type_map[op_type] = type(runner_instance)

        self.assertEqual(type_map[wl.OperationType.Bulk], MilvusBulkIndex)
        self.assertEqual(type_map[wl.OperationType.Search], MilvusQuery)
        self.assertEqual(type_map[wl.OperationType.PaginatedSearch], MilvusQuery)
        self.assertEqual(type_map[wl.OperationType.VectorSearch], MilvusVectorSearch)
        self.assertEqual(type_map[wl.OperationType.BulkVectorDataSet], MilvusBulkVectorDataSet)
        self.assertEqual(type_map[wl.OperationType.CreateIndex], MilvusCreateIndex)
        self.assertEqual(type_map[wl.OperationType.DeleteIndex], MilvusDeleteIndex)
        self.assertEqual(type_map[wl.OperationType.IndexStats], MilvusIndicesStats)
        self.assertEqual(type_map[wl.OperationType.ClusterHealth], MilvusClusterHealth)
        self.assertEqual(type_map[wl.OperationType.Refresh], MilvusRefresh)
        self.assertEqual(type_map[wl.OperationType.ForceMerge], MilvusForceMerge)
        self.assertEqual(type_map["warmup-knn-indices"], MilvusWarmupRunner)

        # No-ops
        self.assertEqual(type_map[wl.OperationType.PutPipeline], MilvusNoOp)
        self.assertEqual(type_map[wl.OperationType.DeletePipeline], MilvusNoOp)
        self.assertEqual(type_map[wl.OperationType.CreateSearchPipeline], MilvusNoOp)
        self.assertEqual(type_map[wl.OperationType.PutSettings], MilvusNoOp)
