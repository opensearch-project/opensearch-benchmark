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

        result = ProtoQueryHelper.build_proto_request(params)

        self.assertIsInstance(result, search_pb2.SearchRequest)
        self.assertEqual(result.index, ["test-index"])
        self.assertEqual(result.request_body.size, 5)
        self.assertTrue(result.request_cache)
        self.assertTrue(result.request_body.query.HasField("knn"))
        self.assertEqual(result.request_body.query.knn.field, "target_field")
        for i, expected in enumerate([0.1, 0.2, 0.3, 0.4]):
            self.assertAlmostEqual(result.request_body.query.knn.vector[i], expected, places=5)
        self.assertEqual(result.request_body.query.knn.k, 10)