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
from opensearch.protobufs.schemas import search_pb2

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

    def test_build_proto_request_term_query_multi_field_fails(self):
        params = {
            "index": "test-index",
            "body": {
                "query": {
                    "term": {
                        "log.file.path": {
                            "value" : ["/var/log/messages/birdknight", "/var/log/messages/otherterm"]
                        }
                    }
                }
            }
        }

        with self.assertRaises(Exception):
            ProtoQueryHelper.build_proto_request(params)

class ProtoKNNQueryHelperTests(TestCase):
    def test_build_vector_search_proto_request_basic(self):
        params = {
            'index': 'target_index',
            'body': {
                'query': {
                    'knn': {
                        'target_field': {
                            'vector': np.array([1.49081022e-01], dtype=np.float32),
                            'k': 100
                        }
                    }
                },
                'size': 100
            },
            'k': 100,
            'request-params': {
                '_source': False
            }
        }

        request = ProtoQueryHelper.build_vector_search_proto_request(params)

        self.assertIsInstance(request, search_pb2.SearchRequest)
        self.assertEqual(request.index, ['target_index'])
        self.assertEqual(request.request_body.size, 100)
        self.assertFalse(request.x_source.bool)
        self.assertTrue(request.request_body.query.HasField('knn'))
        self.assertEqual(request.request_body.query.knn.field, 'target_field')
        self.assertEqual(request.request_body.query.knn.k, 100)
        self.assertAlmostEqual(request.request_body.query.knn.vector[0], 1.49081022e-01, places=5)

    def test_build_vector_search_proto_request_with_timeout_and_cache(self):
        params = {
            'index': 'test_index',
            'body': {
                'query': {
                    'knn': {
                        'embeddings': {
                            'vector': np.array([0.1, 0.2, 0.3], dtype=np.float32),
                            'k': 50
                        }
                    }
                },
                'size': 50
            },
            'k': 50,
            'request-params': {
                '_source': 'true'
            },
            'request-timeout': 5000,
            'cache': True
        }

        request = ProtoQueryHelper.build_vector_search_proto_request(params)

        self.assertIsInstance(request, search_pb2.SearchRequest)
        self.assertEqual(request.index, ['test_index'])
        self.assertEqual(request.request_body.size, 50)
        self.assertEqual(request.request_body.timeout, '5000ms')
        self.assertTrue(request.x_source.bool)
        self.assertTrue(request.request_cache)
        self.assertTrue(request.request_body.query.HasField('knn'))
        self.assertEqual(request.request_body.query.knn.field, 'embeddings')
        self.assertEqual(request.request_body.query.knn.k, 50)
        for i, expected in enumerate([0.1, 0.2, 0.3]):
            self.assertAlmostEqual(request.request_body.query.knn.vector[i], expected, places=5)

    def test_build_vector_search_proto_request_no_size(self):
        params = {
            'index': 'test_index',
            'body': {
                'query': {
                    'knn': {
                        'vector_field': {
                            'vector': np.array([1.0], dtype=np.float32),
                            'k': 5
                        }
                    }
                }
            },
            'k': 5,
            'request-params': {
                '_source': False
            }
        }

        request = ProtoQueryHelper.build_vector_search_proto_request(params)

        self.assertEqual(request.request_body.size, 0)

    def test_build_vector_search_proto_request_detailed_results_raises_error(self):
        params = {
            'index': 'test_index',
            'body': {
                'query': {
                    'knn': {
                        'vector_field': {
                            'vector': np.array([1.0], dtype=np.float32),
                            'k': 5
                        }
                    }
                }
            },
            'k': 5,
            'request-params': {
                '_source': False
            },
            'detailed-results': True
        }

        with self.assertRaises(NotImplementedError) as context:
            ProtoQueryHelper.build_vector_search_proto_request(params)

        self.assertIn('Detailed results not supported', str(context.exception))

    def test_build_vector_search_proto_request_calculate_recall_raises_error(self):
        params = {
            'index': 'test_index',
            'body': {
                'query': {
                    'knn': {
                        'vector_field': {
                            'vector': np.array([1.0], dtype=np.float32),
                            'k': 5
                        }
                    }
                }
            },
            'k': 5,
            'request-params': {
                '_source': False
            },
            'calculate-recall': True
        }

        with self.assertRaises(NotImplementedError) as context:
            ProtoQueryHelper.build_vector_search_proto_request(params)

        self.assertIn('Recall calculations not supported', str(context.exception))

    def test_build_vector_search_proto_request_compression_raises_error(self):
        params = {
            'index': 'test_index',
            'body': {
                'query': {
                    'knn': {
                        'vector_field': {
                            'vector': np.array([1.0], dtype=np.float32),
                            'k': 5
                        }
                    }
                }
            },
            'k': 5,
            'request-params': {
                '_source': False
            },
            'response-compression-enabled': True
        }

        with self.assertRaises(NotImplementedError) as context:
            ProtoQueryHelper.build_vector_search_proto_request(params)

        self.assertIn('Compression not supported', str(context.exception))
