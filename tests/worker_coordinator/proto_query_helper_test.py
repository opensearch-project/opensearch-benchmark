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

from opensearch.protobufs.schemas import search_pb2

from osbenchmark.worker_coordinator.proto_helpers.ProtoKNNQueryHelper import ProtoKNNQueryHelper
from osbenchmark.worker_coordinator.proto_helpers.ProtoQueryHelper import ProtoQueryHelper

class ProtoQueryHelperTests(TestCase):
    def test_build_proto_request_match_all_query(self):
        params = {
            "index": "test-index",
            "body": {
                "query": {
                    "match_all": {}
                },
                "size": 10,
                "_source": True
            },
            "request-timeout": 5000,
            "cache": "true"
        }

        request = ProtoQueryHelper.build_proto_request(params)

        self.assertIsInstance(request, search_pb2.SearchRequest)
        self.assertEqual(request.index, ["test-index"])
        self.assertEqual(request.request_body.size, 10)
        self.assertEqual(request.request_body.timeout, "5000ms")
        self.assertTrue(request.request_cache)
        self.assertTrue(request.x_source.bool)
        self.assertTrue(request.request_body.query.HasField("match_all"))

    def test_build_proto_request_term_query(self):
        params = {
            "index": "test-index",
            "body": {
                "query": {
                    "term": {
                        "log.file.path": {
                            "value" : "/var/log/messages/birdknight"
                        }
                    }
                }
            }
        }

        result = ProtoQueryHelper.build_proto_request(params)

        self.assertIsInstance(result, search_pb2.SearchRequest)
        self.assertTrue(result.request_body.query.HasField("term"))
        self.assertEqual(result.request_body.query.term.field, "log.file.path")
        self.assertEqual(result.request_body.query.term.value.string, "/var/log/messages/birdknight")

class ProtoKNNQueryHelperTests(TestCase):
    def test_build_proto_request_basic_knn(self):
        params = {
            "index": "test-index",
            "body": {
                "query": {
                    "knn": {
                        "target_field": {
                            "vector": [0.1, 0.2, 0.3, 0.4],
                            "k": 10
                        }
                    }
                },
                "size": 5
            },
            "cache": True
        }

        result = ProtoKNNQueryHelper.build_proto_request(params)

        self.assertIsInstance(result, search_pb2.SearchRequest)
        self.assertEqual(result.index, ["test-index"])
        self.assertEqual(result.size, 5)
        self.assertTrue(result.request_cache)
        self.assertTrue(result.request_body.query.HasField("knn"))
        self.assertEqual(result.request_body.query.knn.field, "target_field")
        self.assertEqual(list(result.request_body.query.knn.vector), [0.1, 0.2, 0.3, 0.4])
        self.assertEqual(result.request_body.query.knn.k, 10)


#     def test_build_proto_request_terms_query(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "terms": {
#                         "tags": ["python", "opensearch", "search"]
#                     }
#                 }
#             }
#         }
#
#         result = ProtoQueryHelper.build_proto_request(params)
#
#         self.assertIsInstance(result, search_pb2.SearchRequest)
#         self.assertTrue(result.request_body.query.HasField("terms"))
#         expected_terms = ["python", "opensearch", "search"]
#         # actual_terms = list(result.request_body.query.terms.["tags"].string_array.string_array)
#         # self.assertEqual(actual_terms, expected_terms)
#
#     def test_build_proto_request_unsupported_query_type(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "bool": {
#                         "must": [{"match": {"title": "search"}}]
#                     }
#                 }
#             }
#         }
#
#         with self.assertRaises(Exception) as ctx:
#             ProtoQueryHelper.build_proto_request(params)
#
#         self.assertIn("Unsupported query type", str(ctx.exception))
#
#     def test_build_proto_request_defaults(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "match_all": {}
#                 }
#             }
#         }
#
#         result = ProtoQueryHelper.build_proto_request(params)
#
#         self.assertIsNone(result.size)
#         self.assertIsNone(result.timeout)
#         self.assertFalse(result.request_cache)
#         self.assertFalse(result.x_source.bool_value)
#
#     def test_build_stats_simple_response(self):
#         mock_response = mock.Mock()
#         mock_response.WhichOneof.return_value = 'response_body'
#         mock_response.response_body = search_pb2.ResponseBody()
#
#         params = {}
#
#         result = ProtoQueryHelper.build_stats(mock_response, params)
#
#         expected = {
#             "weight": 1,
#             "unit": "ops",
#             "success": True
#         }
#
#         self.assertEqual(result, expected)
#
#     def test_build_stats_detailed_response(self):
#         mock_response = mock.Mock()
#         mock_response.WhichOneof.return_value = 'response_body'
#
#         response_body = search_pb2.ResponseBody()
#         response_body.took = 15
#         response_body.timed_out = False
#
#         # Mock hits structure
#         hits = search_pb2.Hits()
#         total_hits = search_pb2.TotalHits()
#         total_hits.value = 1000
#         total_hits.relation = 1  # TOTAL_HITS_RELATION_EQ
#         hits.total.CopyFrom(total_hits)
#         response_body.hits.CopyFrom(hits)
#
#         mock_response.response_body = response_body
#
#         params = {"detailed-results": True}
#
#         result = ProtoQueryHelper.build_stats(mock_response, params)
#
#         expected = {
#             "weight": 1,
#             "unit": "ops",
#             "success": True,
#             "hits": 1000,
#             "hits_relation": "TOTAL_HITS_RELATION_EQ",
#             "timed_out": False,
#             "took": 15
#         }
#
#         self.assertEqual(result, expected)
#
#     def test_build_stats_error_response(self):
#         mock_response = mock.Mock()
#         mock_response.WhichOneof.return_value = 'error_4xx_response'
#
#         params = {}
#
#         with self.assertRaises(Exception) as ctx:
#             ProtoQueryHelper.build_stats(mock_response, params)
#
#         self.assertIn("Server responded with error", str(ctx.exception))
#
#
# class ProtoKNNQueryHelperTests(TestCase):
#     def test_build_proto_request_basic_knn(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "knn": {
#                         "target_field": {
#                             "vector": [0.1, 0.2, 0.3, 0.4],
#                             "k": 10
#                         }
#                     }
#                 },
#                 "size": 5
#             },
#             "cache": True
#         }
#
#         result = ProtoKNNQueryHelper.build_proto_request(params)
#
#         self.assertIsInstance(result, search_pb2.SearchRequest)
#         self.assertEqual(result.index, ["test-index"])
#         self.assertEqual(result.size, 5)
#         self.assertTrue(result.request_cache)
#         self.assertTrue(result.request_body.query.HasField("knn"))
#         self.assertEqual(result.request_body.query.knn.field, "target_field")
#         self.assertEqual(list(result.request_body.query.knn.vector), [0.1, 0.2, 0.3, 0.4])
#         self.assertEqual(result.request_body.query.knn.k, 10)
#
#     def test_build_proto_request_with_request_params(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "knn": {
#                         "target_field": {
#                             "vector": [1.0, 2.0],
#                             "k": 5
#                         }
#                     }
#                 }
#             },
#             "request-params": {
#                 "_source": ["field1", "field2"],
#                 "allow_partial_search_results": True
#             }
#         }
#
#         result = ProtoKNNQueryHelper.build_proto_request(params)
#
#         self.assertEqual(result.source, ["field1", "field2"])
#
#     def test_build_proto_request_with_body_fields(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "knn": {
#                         "target_field": {
#                             "vector": [0.5],
#                             "k": 3
#                         }
#                     }
#                 },
#                 "docvalue_fields": ["timestamp"],
#                 "stored_fields": ["title", "content"]
#             }
#         }
#
#         result = ProtoKNNQueryHelper.build_proto_request(params)
#
#         self.assertIsInstance(result, search_pb2.SearchRequest)
#
#     def test_build_stats_simple_response(self):
#         mock_response = mock.Mock()
#         mock_response.WhichOneof.return_value = 'response_body'
#         mock_response.response_body = search_pb2.ResponseBody()
#
#         params = {}
#
#         result = ProtoKNNQueryHelper.build_stats(mock_response, params)
#
#         expected = {
#             "weight": 1,
#             "unit": "ops",
#             "success": True
#         }
#
#         self.assertEqual(result, expected)
#
#     def test_build_stats_detailed_response(self):
#         mock_response = mock.Mock()
#         mock_response.WhichOneof.return_value = 'response_body'
#
#         response_body = search_pb2.ResponseBody()
#         response_body.took = 25
#         response_body.timed_out = False
#
#         # Mock hits structure
#         hits = search_pb2.Hits()
#         total_hits = search_pb2.TotalHits()
#         total_hits.value = 50
#         total_hits.relation = 2  # TOTAL_HITS_RELATION_GTE
#         hits.total.CopyFrom(total_hits)
#         response_body.hits.CopyFrom(hits)
#
#         mock_response.response_body = response_body
#
#         params = {"detailed-results": True}
#
#         result = ProtoKNNQueryHelper.build_stats(mock_response, params)
#
#         expected = {
#             "weight": 1,
#             "unit": "ops",
#             "success": True,
#             "hits": 50,
#             "hits_relation": "TOTAL_HITS_RELATION_GTE",
#             "timed_out": False,
#             "took": 25
#         }
#
#         self.assertEqual(result, expected)
#
#     def test_build_stats_error_response(self):
#         mock_response = mock.Mock()
#         mock_response.WhichOneof.return_value = 'error_5xx_response'
#
#         params = {}
#
#         with self.assertRaises(Exception) as ctx:
#             ProtoKNNQueryHelper.build_stats(mock_response, params)
#
#         self.assertIn("Server responded with error", str(ctx.exception))
#
#
# class ProtoQueryHelperTermParsingTests(TestCase):
#     def test_parse_term_query_with_dict_value(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "term": {
#                         "status": {"value": "active"}
#                     }
#                 }
#             }
#         }
#
#         result = ProtoQueryHelper.build_proto_request(params)
#
#         self.assertTrue(result.request_body.query.HasField("term"))
#         self.assertEqual(result.request_body.query.term.field, "status")
#         self.assertEqual(result.request_body.query.term.value.string, "active")
#
#     def test_parse_term_query_multiple_fields_raises_error(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "term": {
#                         "status": "active",
#                         "category": "news"
#                     }
#                 }
#             }
#         }
#
#         with self.assertRaises(Exception) as ctx:
#             ProtoQueryHelper.build_proto_request(params)
#
#         self.assertIn("multiple distinct fields", str(ctx.exception))
#
#     def test_parse_terms_query_multiple_fields_raises_error(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "terms": {
#                         "tags": ["python", "search"],
#                         "categories": ["tech", "dev"]
#                     }
#                 }
#             }
#         }
#
#         with self.assertRaises(Exception) as ctx:
#             ProtoQueryHelper.build_proto_request(params)
#
#         self.assertIn("multiple distinct fields", str(ctx.exception))
#
#     def test_parse_term_query_non_string_value_raises_error(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "term": {
#                         "count": 42
#                     }
#                 }
#             }
#         }
#
#         with self.assertRaises(Exception) as ctx:
#             ProtoQueryHelper.build_proto_request(params)
#
#         self.assertIn("Type", str(ctx.exception))
#         self.assertIn("not supported", str(ctx.exception))
#
#     def test_parse_terms_query_with_dict_format(self):
#         params = {
#             "index": "test-index",
#             "body": {
#                 "query": {
#                     "terms": {
#                         "tags": {"value": ["python", "search"]}
#                     }
#                 }
#             }
#         }
#
#         result = ProtoQueryHelper.build_proto_request(params)
#
#         self.assertTrue(result.request_body.query.HasField("terms"))
#         expected_terms = ["python", "search"]
#         actual_terms = list(result.request_body.query.terms.terms_lookup_field_string_array_map["tags"].string_array.string_array)
#         self.assertEqual(actual_terms, expected_terms)