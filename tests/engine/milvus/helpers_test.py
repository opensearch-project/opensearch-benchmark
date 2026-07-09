# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import unittest
from unittest import TestCase
from unittest.mock import MagicMock, patch

import numpy as np

from osbenchmark.engine.milvus.helpers import (
    get_metric_type,
    build_search_params,
    convert_milvus_search_response,
    parse_vector_body,
    calculate_topk_recall,
)


# ---------------------------------------------------------------------------
# get_metric_type
# ---------------------------------------------------------------------------
class GetMetricTypeTests(TestCase):
    def test_cosinesimil(self):
        self.assertEqual(get_metric_type("cosinesimil"), "COSINE")

    def test_l2(self):
        self.assertEqual(get_metric_type("l2"), "L2")

    def test_innerproduct(self):
        self.assertEqual(get_metric_type("innerproduct"), "IP")

    def test_ip_lowercase(self):
        self.assertEqual(get_metric_type("ip"), "IP")

    def test_cosine(self):
        self.assertEqual(get_metric_type("cosine"), "COSINE")

    def test_angular(self):
        self.assertEqual(get_metric_type("angular"), "COSINE")

    def test_uppercase_passthrough(self):
        self.assertEqual(get_metric_type("COSINE"), "COSINE")
        self.assertEqual(get_metric_type("L2"), "L2")
        self.assertEqual(get_metric_type("IP"), "IP")

    def test_unknown_defaults_to_cosine(self):
        self.assertEqual(get_metric_type("hamming"), "COSINE")
        self.assertEqual(get_metric_type(""), "COSINE")
        self.assertEqual(get_metric_type("unknown_metric"), "COSINE")


# ---------------------------------------------------------------------------
# build_collection_schema
# ---------------------------------------------------------------------------
class BuildCollectionSchemaTests(TestCase):
    def _make_mock_client(self):
        client = MagicMock()
        client.create_schema.return_value = MagicMock()
        client.prepare_index_params.return_value = MagicMock()
        return client

    @patch("osbenchmark.engine.milvus.helpers.DataType", create=True)
    @patch.dict("sys.modules", {"pymilvus": MagicMock()})
    def test_defaults(self, _mock_dt):
        import sys  # pylint: disable=import-outside-toplevel
        sys.modules["pymilvus"] = MagicMock()

        try:
            from osbenchmark.engine.milvus.helpers import build_collection_schema as bcs  # pylint: disable=import-outside-toplevel

            client = self._make_mock_client()
            _, index_params, collection_name = bcs(client, {})

            self.assertEqual(collection_name, "target_index")
            client.create_schema.assert_called_once()
            client.prepare_index_params.assert_called_once()
            # Vector field defaults to "embedding", index_type to HNSW
            index_params.add_index.assert_called_once()
            call_kwargs = index_params.add_index.call_args
            self.assertEqual(call_kwargs[1]["field_name"], "embedding")
            self.assertEqual(call_kwargs[1]["index_type"], "HNSW")
            self.assertEqual(call_kwargs[1]["metric_type"], "COSINE")
            self.assertEqual(call_kwargs[1]["params"]["M"], 16)
            self.assertEqual(call_kwargs[1]["params"]["efConstruction"], 200)
        finally:
            del sys.modules["pymilvus"]

    @patch.dict("sys.modules", {"pymilvus": MagicMock()})
    def test_custom_params(self):
        import sys  # pylint: disable=import-outside-toplevel
        sys.modules["pymilvus"] = MagicMock()
        try:
            from osbenchmark.engine.milvus.helpers import build_collection_schema as bcs  # pylint: disable=import-outside-toplevel

            client = self._make_mock_client()
            params = {
                "target_index_dimension": 128,
                "target_index_space_type": "l2",
                "target_field_name": "vec",
                "target_index_name": "my_collection",
                "hnsw_m": 32,
                "hnsw_ef_construction": 400,
            }
            _, index_params, collection_name = bcs(client, params)

            self.assertEqual(collection_name, "my_collection")
            call_kwargs = index_params.add_index.call_args[1]
            self.assertEqual(call_kwargs["field_name"], "vec")
            self.assertEqual(call_kwargs["metric_type"], "L2")
            self.assertEqual(call_kwargs["params"]["M"], 32)
            self.assertEqual(call_kwargs["params"]["efConstruction"], 400)
        finally:
            del sys.modules["pymilvus"]

    @patch.dict("sys.modules", {"pymilvus": MagicMock()})
    def test_client_options_override_metric(self):
        import sys  # pylint: disable=import-outside-toplevel
        sys.modules["pymilvus"] = MagicMock()
        try:
            from osbenchmark.engine.milvus.helpers import build_collection_schema as bcs  # pylint: disable=import-outside-toplevel

            client = self._make_mock_client()
            params = {"target_index_space_type": "l2"}
            client_options = {"metric_type": "IP"}
            _, index_params, _ = bcs(client, params, client_options)

            call_kwargs = index_params.add_index.call_args[1]
            self.assertEqual(call_kwargs["metric_type"], "IP")
        finally:
            del sys.modules["pymilvus"]

    @patch.dict("sys.modules", {"pymilvus": MagicMock()})
    def test_client_options_override_index_type(self):
        import sys  # pylint: disable=import-outside-toplevel
        sys.modules["pymilvus"] = MagicMock()
        try:
            from osbenchmark.engine.milvus.helpers import build_collection_schema as bcs  # pylint: disable=import-outside-toplevel

            client = self._make_mock_client()
            client_options = {"index_type": "IVF_FLAT"}
            _, index_params, _ = bcs(client, {}, client_options)

            call_kwargs = index_params.add_index.call_args[1]
            self.assertEqual(call_kwargs["index_type"], "IVF_FLAT")
        finally:
            del sys.modules["pymilvus"]

    @patch.dict("sys.modules", {"pymilvus": MagicMock()})
    def test_schema_has_doc_id_and_vector_fields(self):
        import sys  # pylint: disable=import-outside-toplevel
        sys.modules["pymilvus"] = MagicMock()
        try:
            from osbenchmark.engine.milvus.helpers import build_collection_schema as bcs  # pylint: disable=import-outside-toplevel

            client = self._make_mock_client()
            schema_mock = client.create_schema.return_value
            bcs(client, {"target_field_name": "my_vec"})

            add_field_calls = schema_mock.add_field.call_args_list
            self.assertEqual(len(add_field_calls), 2)
            # First call: doc_id primary key
            self.assertEqual(add_field_calls[0][1]["field_name"], "doc_id")
            self.assertTrue(add_field_calls[0][1]["is_primary"])
            self.assertFalse(add_field_calls[0][1]["auto_id"])
            # Second call: vector field
            self.assertEqual(add_field_calls[1][1]["field_name"], "my_vec")
        finally:
            del sys.modules["pymilvus"]

    @patch.dict("sys.modules", {"pymilvus": MagicMock()})
    def test_hnsw_params_from_client_options(self):
        import sys  # pylint: disable=import-outside-toplevel
        sys.modules["pymilvus"] = MagicMock()
        try:
            from osbenchmark.engine.milvus.helpers import build_collection_schema as bcs  # pylint: disable=import-outside-toplevel

            client = self._make_mock_client()
            client_options = {"hnsw_m": 64, "hnsw_ef_construction": 500}
            _, index_params, _ = bcs(client, {}, client_options)

            call_kwargs = index_params.add_index.call_args[1]
            self.assertEqual(call_kwargs["params"]["M"], 64)
            self.assertEqual(call_kwargs["params"]["efConstruction"], 500)
        finally:
            del sys.modules["pymilvus"]


# ---------------------------------------------------------------------------
# build_search_params
# ---------------------------------------------------------------------------
class BuildSearchParamsTests(TestCase):
    def test_hnsw_defaults(self):
        result = build_search_params({})
        self.assertEqual(result["k"], 100)
        self.assertEqual(result["ef_search"], 256)
        self.assertEqual(result["search_params"]["params"]["ef"], 256)

    def test_hnsw_k_larger_than_256(self):
        result = build_search_params({"k": 500})
        self.assertEqual(result["k"], 500)
        self.assertEqual(result["ef_search"], 500)
        self.assertEqual(result["search_params"]["params"]["ef"], 500)

    def test_hnsw_ef_search_override_via_client_options(self):
        result = build_search_params({}, client_options={"hnsw_ef_search": 512})
        self.assertEqual(result["ef_search"], 512)
        self.assertEqual(result["search_params"]["params"]["ef"], 512)

    def test_hnsw_ef_search_override_via_params(self):
        result = build_search_params({"hnsw_ef_search": 300})
        self.assertEqual(result["ef_search"], 300)

    def test_ivf_flat_branch(self):
        result = build_search_params({}, client_options={"index_type": "IVF_FLAT"})
        self.assertIn("nprobe", result["search_params"]["params"])
        self.assertEqual(result["search_params"]["params"]["nprobe"], 128)

    def test_ivf_custom_nprobe(self):
        result = build_search_params(
            {"nprobe": 64}, client_options={"index_type": "IVF_SQ8"}
        )
        self.assertEqual(result["search_params"]["params"]["nprobe"], 64)

    def test_diskann_branch(self):
        result = build_search_params({}, client_options={"index_type": "DISKANN"})
        self.assertIn("search_list", result["search_params"]["params"])
        self.assertEqual(result["search_params"]["params"]["search_list"], 256)

    def test_unknown_index_type_empty_params(self):
        result = build_search_params({}, client_options={"index_type": "EXOTIC"})
        self.assertEqual(result["search_params"]["params"], {})

    def test_query_k_fallback(self):
        result = build_search_params({"query_k": 50})
        self.assertEqual(result["k"], 50)

    def test_k_takes_precedence_over_query_k(self):
        result = build_search_params({"k": 200, "query_k": 50})
        self.assertEqual(result["k"], 200)


# ---------------------------------------------------------------------------
# convert_milvus_search_response
# ---------------------------------------------------------------------------
class ConvertMilvusSearchResponseTests(TestCase):
    def test_none_returns_empty(self):
        result = convert_milvus_search_response(None)
        self.assertEqual(result["hits"]["total"]["value"], 0)
        self.assertEqual(result["hits"]["hits"], [])

    def test_empty_list_returns_empty(self):
        result = convert_milvus_search_response([])
        self.assertEqual(result["hits"]["total"]["value"], 0)
        self.assertEqual(result["hits"]["hits"], [])

    def test_single_hit(self):
        results = [[{"doc_id": 42, "distance": 0.95}]]
        result = convert_milvus_search_response(results, "my_coll")
        hits = result["hits"]["hits"]
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["_id"], "42")
        self.assertEqual(hits[0]["_score"], 0.95)
        self.assertEqual(hits[0]["_index"], "my_coll")

    def test_multiple_hits(self):
        results = [[
            {"doc_id": 1, "distance": 0.9},
            {"doc_id": 2, "distance": 0.8},
            {"doc_id": 3, "distance": 0.7},
        ]]
        result = convert_milvus_search_response(results)
        self.assertEqual(result["hits"]["total"]["value"], 3)
        self.assertEqual(len(result["hits"]["hits"]), 3)

    def test_doc_id_coerced_to_str(self):
        results = [[{"doc_id": 12345, "distance": 0.5}]]
        result = convert_milvus_search_response(results)
        self.assertIsInstance(result["hits"]["hits"][0]["_id"], str)

    def test_id_fallback_when_no_doc_id(self):
        results = [[{"id": 99, "distance": 0.5}]]
        result = convert_milvus_search_response(results)
        self.assertEqual(result["hits"]["hits"][0]["_id"], "99")

    def test_missing_id_fields_returns_empty_string(self):
        results = [[{"distance": 0.5}]]
        result = convert_milvus_search_response(results)
        self.assertEqual(result["hits"]["hits"][0]["_id"], "")

    def test_default_collection_name(self):
        results = [[{"doc_id": 1, "distance": 0.5}]]
        result = convert_milvus_search_response(results)
        self.assertEqual(result["hits"]["hits"][0]["_index"], "default")

    def test_response_structure(self):
        results = [[{"doc_id": 1, "distance": 0.5}]]
        result = convert_milvus_search_response(results)
        self.assertIn("took", result)
        self.assertIn("timed_out", result)
        self.assertFalse(result["timed_out"])
        self.assertEqual(result["hits"]["total"]["relation"], "eq")


# ---------------------------------------------------------------------------
# parse_vector_body
# ---------------------------------------------------------------------------
class ParseVectorBodyTests(TestCase):
    def test_single_pair(self):
        body = [
            {"index": {"_index": "idx", "_id": 0}},
            {"embedding": [1.0, 2.0, 3.0]},
        ]
        docs, index = parse_vector_body(body)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["doc_id"], 0)
        self.assertEqual(docs[0]["embedding"], [1.0, 2.0, 3.0])
        self.assertEqual(index, "idx")

    def test_multiple_pairs(self):
        body = [
            {"index": {"_index": "idx", "_id": 0}},
            {"embedding": [1.0]},
            {"index": {"_index": "idx", "_id": 1}},
            {"embedding": [2.0]},
        ]
        docs, _ = parse_vector_body(body)
        self.assertEqual(len(docs), 2)
        self.assertEqual(docs[0]["doc_id"], 0)
        self.assertEqual(docs[1]["doc_id"], 1)

    def test_numpy_array_converted_to_list(self):
        vec = np.array([1.0, 2.0, 3.0])
        body = [
            {"index": {"_index": "idx", "_id": 5}},
            {"embedding": vec},
        ]
        docs, _ = parse_vector_body(body)
        self.assertIsInstance(docs[0]["embedding"], list)
        self.assertEqual(docs[0]["embedding"], [1.0, 2.0, 3.0])

    def test_non_integer_id_raises(self):
        body = [
            {"index": {"_index": "idx", "_id": "abc"}},
            {"embedding": [1.0]},
        ]
        with self.assertRaises(ValueError) as ctx:
            parse_vector_body(body)
        self.assertIn("integer _id", str(ctx.exception))

    def test_none_action_skipped(self):
        body = [
            None,
            {"embedding": [1.0]},
            {"index": {"_index": "idx", "_id": 0}},
            {"embedding": [2.0]},
        ]
        docs, _ = parse_vector_body(body)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["doc_id"], 0)

    def test_none_doc_skipped(self):
        body = [
            {"index": {"_index": "idx", "_id": 0}},
            None,
            {"index": {"_index": "idx", "_id": 1}},
            {"embedding": [2.0]},
        ]
        docs, _ = parse_vector_body(body)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["doc_id"], 1)

    def test_empty_body(self):
        docs, index = parse_vector_body([])
        self.assertEqual(docs, [])
        self.assertIsNone(index)

    def test_non_list_body(self):
        docs, index = parse_vector_body("not a list")
        self.assertEqual(docs, [])
        self.assertIsNone(index)

    def test_string_integer_id_accepted(self):
        body = [
            {"index": {"_index": "idx", "_id": "42"}},
            {"embedding": [1.0]},
        ]
        docs, _ = parse_vector_body(body)
        self.assertEqual(docs[0]["doc_id"], 42)

    def test_missing_id_uses_positional_default(self):
        body = [
            {"index": {"_index": "idx"}},
            {"embedding": [1.0]},
        ]
        docs, _ = parse_vector_body(body)
        # _id defaults to i // 2, which is 0 for the first pair
        self.assertEqual(docs[0]["doc_id"], 0)

    def test_odd_length_body_ignores_trailing(self):
        body = [
            {"index": {"_index": "idx", "_id": 0}},
            {"embedding": [1.0]},
            {"index": {"_index": "idx", "_id": 1}},
        ]
        docs, _ = parse_vector_body(body)
        self.assertEqual(len(docs), 1)


# ---------------------------------------------------------------------------
# calculate_topk_recall
# ---------------------------------------------------------------------------
class CalculateTopkRecallTests(TestCase):
    def test_perfect_recall(self):
        predictions = [1, 2, 3, 4, 5]
        neighbors = [1, 2, 3, 4, 5]
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 5), 1.0)

    def test_zero_recall(self):
        predictions = [10, 20, 30]
        neighbors = [1, 2, 3]
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 3), 0.0)

    def test_partial_recall(self):
        predictions = [1, 2, 99]
        neighbors = [1, 2, 3]
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 3), 2.0 / 3.0)

    def test_none_neighbors_returns_zero(self):
        self.assertAlmostEqual(calculate_topk_recall([1, 2, 3], None, 3), 0.0)

    def test_str_coercion(self):
        # Predictions as strings, neighbors as ints
        predictions = ["1", "2", "3"]
        neighbors = [1, 2, 3]
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 3), 1.0)

    def test_int_vs_numpy_int(self):
        predictions = [np.int64(1), np.int64(2)]
        neighbors = [1, 2]
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 2), 1.0)

    def test_minus_one_sentinels_excluded(self):
        # -1 sentinels in neighbors should be excluded from truth set
        predictions = [1, 2]
        neighbors = [1, 2, -1, -1]
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 4), 1.0)

    def test_all_minus_one_neighbors_returns_one(self):
        predictions = [1, 2, 3]
        neighbors = [-1, -1, -1]
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 3), 1.0)

    def test_topk_smaller_than_lists(self):
        predictions = [1, 2, 3, 4, 5]
        neighbors = [1, 2, 3, 4, 5]
        # Only top-2 should be considered
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 2), 1.0)

    def test_topk_larger_than_neighbors(self):
        predictions = [1, 2, 3, 4, 5]
        neighbors = [1, 2]
        # min_results = min(5, 2) = 2; truth = {1, 2}; correct = 2
        self.assertAlmostEqual(calculate_topk_recall(predictions, neighbors, 5), 1.0)


if __name__ == "__main__":
    unittest.main()
