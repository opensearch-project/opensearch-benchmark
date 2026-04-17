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

from osbenchmark import exceptions
from osbenchmark.engine.vespa.runners import (
    VespaBulkIndex,
    VespaVectorSearch,
    VespaBulkVectorDataSet,
    VespaQuery,
    VespaScrollQuery,
    VespaCreateIndex,
    VespaDeleteIndex,
    VespaIndicesStats,
    VespaClusterHealth,
    VespaRefresh,
    VespaForceMerge,
    VespaNoOp,
    VespaWarmupIndicesRunner,
    register_vespa_runners,
)
from tests import run_async


def _make_vespa_client(**overrides):
    """Create a standard mock vespa_client with common attributes."""
    client = mock.AsyncMock()
    client._app_name = overrides.get("app_name", "testapp")
    client._namespace = overrides.get("namespace", "benchmark")
    client.client_options = overrides.get("client_options", {"max_concurrent": "8"})
    return client


def _vespa_search_response(hits=None, total_count=1, search_time=5, timed_out=False):
    """Build a raw Vespa search response dict."""
    if hits is None:
        hits = [{"id": "id::testapp::1", "relevance": 1.0, "fields": {"title": "doc1"}}]
    resp = {
        "root": {
            "fields": {"totalCount": total_count},
        },
        "timing": {"searchtime": search_time},
    }
    if hits is not None:
        resp["root"]["children"] = hits
    return resp


def _opensearch_style_response(total_value=1, took=5, timed_out=False, hits_list=None):
    """Build a converted OpenSearch-style response (output of convert_vespa_response)."""
    if hits_list is None:
        hits_list = [{"_id": "1", "_score": 1.0, "_source": {"title": "doc1"}}]
    return {
        "took": took,
        "timed_out": timed_out,
        "hits": {
            "total": {"value": total_value, "relation": "eq"},
            "max_score": hits_list[0]["_score"] if hits_list else 0,
            "hits": hits_list,
        },
    }


class VespaBulkIndexRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_success_pyvespa_path(self, mock_ctx, mock_parse, mock_transform):
        # PYVESPA_AVAILABLE=True triggers the feed_batch path (HTTP/2 multiplexing)
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        mock_parse.return_value = [
            {"_id": "1", "_source": {"title": "doc1"}, "_action": "index"},
        ]

        params = {"body": b'{"index":{}}\n{"title":"doc1"}\n', "bulk-size": 1, "unit": "docs", "index": "myindex"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        self.assertEqual(result["weight"], 1)
        self.assertEqual(result["unit"], "docs")
        self.assertTrue(result["success"])
        self.assertEqual(result["error-count"], 0)
        vespa_client.feed_batch.assert_called_once()
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", False)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_success_aiohttp_fallback(self, mock_ctx, mock_parse, mock_transform):
        # PYVESPA_AVAILABLE=False triggers per-document aiohttp POST fallback
        vespa_client = _make_vespa_client()

        mock_parse.return_value = [
            {"_id": "1", "_source": {"title": "doc1"}, "_action": "index"},
            {"_id": "2", "_source": {"title": "doc2"}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "myindex"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        self.assertTrue(result["success"])
        self.assertEqual(result["error-count"], 0)
        self.assertEqual(vespa_client.index.call_count, 2)
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_returns_weight_and_unit(self, mock_ctx, mock_parse, mock_transform):
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        mock_parse.return_value = [
            {"_id": "1", "_source": {"title": "doc1"}, "_action": "index"},
            {"_id": "2", "_source": {"title": "doc2"}, "_action": "index"},
            {"_id": "3", "_source": {"title": "doc3"}, "_action": "index"},
        ]

        params = {"body": b'data', "bulk-size": 100, "unit": "docs", "index": "idx"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        self.assertEqual(result["weight"], 100)
        self.assertEqual(result["unit"], "docs")

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_weight_uses_bulk_size_param(self, mock_ctx, mock_parse, mock_transform):
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        mock_parse.return_value = [
            {"_id": "1", "_source": {"f": "v"}, "_action": "index"},
        ]

        params = {"body": b'data', "bulk-size": 500, "unit": "docs", "index": "idx"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        # bulk-size param overrides len(documents)
        self.assertEqual(result["weight"], 500)

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_weight_defaults_to_doc_count(self, mock_ctx, mock_parse, mock_transform):
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        mock_parse.return_value = [
            {"_id": "1", "_source": {"f": "v"}, "_action": "index"},
            {"_id": "2", "_source": {"f": "v"}, "_action": "index"},
        ]

        # bulk-size=0 means fallback to len(documents)
        params = {"body": b'data', "bulk-size": 0, "index": "idx"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        self.assertEqual(result["weight"], 2)

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_reports_failure_on_errors(self, mock_ctx, mock_parse, mock_transform):
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 3, "responses": []}

        mock_parse.return_value = [
            {"_id": "1", "_source": {"f": "v"}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "idx"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        self.assertFalse(result["success"])
        self.assertEqual(result["error-count"], 3)

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_reports_error_count(self, mock_ctx, mock_parse, mock_transform):
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 5, "responses": []}

        mock_parse.return_value = [
            {"_id": str(i), "_source": {"f": "v"}, "_action": "index"} for i in range(10)
        ]

        params = {"body": b'data', "index": "idx"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        self.assertEqual(result["error-count"], 5)

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa", side_effect=lambda d: d)
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_transforms_documents_with_nested(self, mock_ctx, mock_parse, mock_transform):
        # Vespa can't handle @-prefixed fields natively; transform flattens them
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        # Doc with @timestamp triggers transform
        mock_parse.return_value = [
            {"_id": "1", "_source": {"@timestamp": "2023-01-01T00:00:00Z", "status": 200}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "idx"}

        runner = VespaBulkIndex()
        await runner(vespa_client, params)

        mock_transform.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_transforms_documents_with_nested_dict(self, mock_ctx, mock_parse, mock_transform):
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        # Doc with nested dict value triggers transform
        mock_parse.return_value = [
            {"_id": "1", "_source": {"log": {"level": "info"}}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "idx"}

        runner = VespaBulkIndex()
        await runner(vespa_client, params)

        mock_transform.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa",
                return_value={"flat_field": "transformed_value"})
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_feeds_transformed_output(self, mock_ctx, mock_parse, mock_transform):
        # Verify the transform output actually reaches feed_batch, not the original source.
        # Without this, a bug that drops the transform result would go undetected.
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        mock_parse.return_value = [
            {"_id": "1", "_source": {"@timestamp": "2023-01-01T00:00:00Z"}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "idx"}

        runner = VespaBulkIndex()
        await runner(vespa_client, params)

        fed_docs = vespa_client.feed_batch.call_args[1]["documents"]
        self.assertEqual(fed_docs[0]["fields"], {"flat_field": "transformed_value"})

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_skips_transform_for_flat(self, mock_ctx, mock_parse, mock_transform):
        # Optimization: flat docs with no @-fields or nested dicts skip the transform entirely
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        # Flat doc without @timestamp or nested dicts — no transform
        mock_parse.return_value = [
            {"_id": "1", "_source": {"status": 200, "message": "ok"}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "idx"}

        runner = VespaBulkIndex()
        await runner(vespa_client, params)

        mock_transform.assert_not_called()

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", False)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_handles_update_action(self, mock_ctx, mock_parse, mock_transform):
        # aiohttp fallback dispatches to vespa_client.update vs .index based on _action
        vespa_client = _make_vespa_client()

        mock_parse.return_value = [
            {"_id": "1", "_source": {"status": 200}, "_action": "update"},
        ]

        params = {"body": b'data', "index": "myindex"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        vespa_client.update.assert_called_once()
        vespa_client.index.assert_not_called()
        self.assertEqual(result["error-count"], 0)

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_max_concurrent_from_params(self, mock_ctx, mock_parse, mock_transform):
        # params-level max_concurrent takes priority over client_options
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        mock_parse.return_value = [
            {"_id": "1", "_source": {"f": "v"}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "idx", "max_concurrent": 16}

        runner = VespaBulkIndex()
        await runner(vespa_client, params)

        call_kwargs = vespa_client.feed_batch.call_args[1]
        self.assertEqual(call_kwargs["max_workers"], 16)

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_max_concurrent_from_client_options(self, mock_ctx, mock_parse, mock_transform):
        # Falls back to client_options["max_concurrent"] when not in params (string value, cast to int)
        vespa_client = _make_vespa_client(client_options={"max_concurrent": "64"})
        vespa_client.feed_batch.return_value = {"errors": 0, "responses": []}

        mock_parse.return_value = [
            {"_id": "1", "_source": {"f": "v"}, "_action": "index"},
        ]

        # No max_concurrent in params — falls back to client_options
        params = {"body": b'data', "index": "idx"}

        runner = VespaBulkIndex()
        await runner(vespa_client, params)

        call_kwargs = vespa_client.feed_batch.call_args[1]
        self.assertEqual(call_kwargs["max_workers"], 64)

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", False)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_aiohttp_counts_exceptions(self, mock_ctx, mock_parse, mock_transform):
        # aiohttp path catches per-doc exceptions and tallies them as errors (not raised)
        vespa_client = _make_vespa_client()
        vespa_client.index.side_effect = [
            None,  # first doc succeeds
            ConnectionError("connection lost"),  # second doc fails
        ]

        mock_parse.return_value = [
            {"_id": "1", "_source": {"f": "v"}, "_action": "index"},
            {"_id": "2", "_source": {"f": "v"}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "myindex"}

        runner = VespaBulkIndex()
        result = await runner(vespa_client, params)

        self.assertEqual(result["error-count"], 1)

    @mock.patch("osbenchmark.engine.vespa.runners.transform_document_for_vespa")
    @mock.patch("osbenchmark.engine.vespa.runners.parse_bulk_body")
    @mock.patch("osbenchmark.engine.vespa.runners.PYVESPA_AVAILABLE", True)
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_index_pyvespa_exception_propagates(self, mock_ctx, mock_parse, mock_transform):
        # Unlike aiohttp path, pyvespa path lets exceptions propagate (batch-level failure).
        # Timing context must still be cleaned up in the finally block.
        vespa_client = _make_vespa_client()
        vespa_client.feed_batch.side_effect = ConnectionError("connection lost")

        mock_parse.return_value = [
            {"_id": "1", "_source": {"f": "v"}, "_action": "index"},
        ]

        params = {"body": b'data', "index": "idx"}

        runner = VespaBulkIndex()
        with self.assertRaises(ConnectionError):
            await runner(vespa_client, params)

        # timing context still cleaned up
        mock_ctx.on_request_end.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    def test_repr(self):
        runner = VespaBulkIndex()
        self.assertEqual(repr(runner), "vespa-bulk-index")


class VespaQueryRunnerTests(TestCase):

    def test_repr(self):
        runner = VespaQuery()
        self.assertEqual(repr(runner), "vespa-query")


class VespaVectorSearchRunnerTests(TestCase):

    def test_extract_doc_id(self):
        self.assertEqual(VespaVectorSearch._extract_doc_id("id:ns:type::123"), "123")
        self.assertEqual(VespaVectorSearch._extract_doc_id("id:target_index:target_index::0"), "0")
        self.assertEqual(VespaVectorSearch._extract_doc_id("42"), "42")

    def test_repr(self):
        runner = VespaVectorSearch()
        self.assertEqual(repr(runner), "vespa-vector-search")


class VespaBulkVectorDataSetRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_vector_calls_bulk(self, mock_ctx):
        vespa_client = _make_vespa_client()

        # Vectorsearch workload produces alternating action/doc pairs
        body = [
            {"index": {"_index": "vectors", "_id": 0}},
            {"embedding": [1.0, 2.0]},
            {"index": {"_index": "vectors", "_id": 1}},
            {"embedding": [3.0, 4.0]},
        ]
        params = {"body": body, "size": 2, "index": "vectors"}

        runner = VespaBulkVectorDataSet()
        await runner(vespa_client, params)

        vespa_client.bulk.assert_called_once()
        call_kwargs = vespa_client.bulk.call_args[1]
        self.assertEqual(len(call_kwargs["body"]), 2)
        self.assertEqual(call_kwargs["body"][0]["_id"], "0")
        self.assertEqual(call_kwargs["body"][0]["fields"], {"embedding": [1.0, 2.0]})
        self.assertEqual(call_kwargs["body"][1]["_id"], "1")
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_vector_returns_size_and_docs(self, mock_ctx):
        vespa_client = _make_vespa_client()

        body = [
            {"index": {"_index": "vectors", "_id": 0}},
            {"embedding": [1.0]},
        ]
        params = {"body": body, "size": 50, "index": "vectors"}

        runner = VespaBulkVectorDataSet()
        result = await runner(vespa_client, params)

        self.assertEqual(result, (50, "docs"))

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_bulk_vector_passes_index(self, mock_ctx):
        vespa_client = _make_vespa_client()

        body = [
            {"index": {"_index": "vectors", "_id": 0}},
            {"embedding": [1.0]},
        ]
        params = {"body": body, "size": 1, "index": "vectors"}

        runner = VespaBulkVectorDataSet()
        await runner(vespa_client, params)

        call_kwargs = vespa_client.bulk.call_args[1]
        self.assertEqual(call_kwargs["index"], "vectors")

    def test_repr(self):
        runner = VespaBulkVectorDataSet()
        self.assertEqual(repr(runner), "vespa-bulk-vector-data-set")


class VespaScrollQueryRunnerTests(TestCase):

    def _make_page_response(self, total_value=100, took=5, timed_out=False, hits_count=10):
        """Create a page response with the specified number of hits."""
        hits_list = [
            {"_id": str(i), "_score": 1.0, "_source": {"f": "v"}} for i in range(hits_count)
        ]
        return {
            "took": took,
            "timed_out": timed_out,
            "hits": {
                "total": {"value": total_value, "relation": "eq"},
                "max_score": 1.0 if hits_list else 0,
                "hits": hits_list,
            },
        }

    def test_repr(self):
        runner = VespaScrollQuery()
        self.assertEqual(repr(runner), "vespa-scroll-query")


class VespaCreateIndexRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_create_from_indices_list(self, mock_ctx):
        vespa_client = _make_vespa_client()

        indices = [("index1", {"mappings": {}}), ("index2", {"mappings": {}})]
        params = {"indices": indices}

        runner = VespaCreateIndex()
        result = await runner(vespa_client, params)

        self.assertEqual(vespa_client.indices.create.call_count, 2)
        self.assertEqual(result["weight"], 2)
        self.assertEqual(result["unit"], "ops")
        self.assertTrue(result["success"])
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_create_from_single_index_param(self, mock_ctx):
        # Alternate param shape: single "index"+"body" instead of "indices" list
        vespa_client = _make_vespa_client()

        params = {"index": "myindex", "body": {"mappings": {}}}

        runner = VespaCreateIndex()
        result = await runner(vespa_client, params)

        vespa_client.indices.create.assert_called_once_with(index="myindex", body={"mappings": {}})
        self.assertEqual(result["weight"], 1)
        self.assertTrue(result["success"])

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_create_returns_weight_and_unit(self, mock_ctx):
        vespa_client = _make_vespa_client()

        indices = [("idx1", {}), ("idx2", {}), ("idx3", {})]
        params = {"indices": indices}

        runner = VespaCreateIndex()
        result = await runner(vespa_client, params)

        self.assertEqual(result["weight"], 3)
        self.assertEqual(result["unit"], "ops")
        self.assertTrue(result["success"])

    def test_repr(self):
        runner = VespaCreateIndex()
        self.assertEqual(repr(runner), "vespa-create-index")


class VespaDeleteIndexRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_delete_from_indices_list(self, mock_ctx):
        vespa_client = _make_vespa_client()

        params = {"indices": ["index1", "index2"]}

        runner = VespaDeleteIndex()
        result = await runner(vespa_client, params)

        self.assertEqual(vespa_client.indices.delete.call_count, 2)
        self.assertEqual(result["weight"], 2)
        self.assertTrue(result["success"])
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_delete_from_single_index_param(self, mock_ctx):
        vespa_client = _make_vespa_client()

        params = {"index": "myindex"}

        runner = VespaDeleteIndex()
        result = await runner(vespa_client, params)

        vespa_client.indices.delete.assert_called_once_with(index="myindex")
        self.assertEqual(result["weight"], 1)
        self.assertTrue(result["success"])

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_delete_only_if_exists_true_and_exists(self, mock_ctx):
        vespa_client = _make_vespa_client()
        vespa_client.indices.exists.return_value = True

        params = {"indices": ["index1"], "only-if-exists": True}

        runner = VespaDeleteIndex()
        result = await runner(vespa_client, params)

        vespa_client.indices.exists.assert_called_once_with(index="index1")
        vespa_client.indices.delete.assert_called_once_with(index="index1")
        self.assertEqual(result["weight"], 1)

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_delete_only_if_exists_true_and_not_exists(self, mock_ctx):
        # Skips delete entirely; weight=0 signals no actual work was done
        vespa_client = _make_vespa_client()
        vespa_client.indices.exists.return_value = False

        params = {"indices": ["index1"], "only-if-exists": True}

        runner = VespaDeleteIndex()
        result = await runner(vespa_client, params)

        vespa_client.indices.exists.assert_called_once_with(index="index1")
        vespa_client.indices.delete.assert_not_called()
        self.assertEqual(result["weight"], 0)

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_delete_only_if_exists_false(self, mock_ctx):
        vespa_client = _make_vespa_client()

        params = {"indices": ["index1"]}

        runner = VespaDeleteIndex()
        result = await runner(vespa_client, params)

        # Default only-if-exists=False means delete without checking
        vespa_client.indices.exists.assert_not_called()
        vespa_client.indices.delete.assert_called_once_with(index="index1")
        self.assertEqual(result["weight"], 1)

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_delete_returns_weight(self, mock_ctx):
        # Weight reflects only actually-deleted indices, not total requested
        vespa_client = _make_vespa_client()
        vespa_client.indices.exists.side_effect = [True, False, True]

        params = {"indices": ["idx1", "idx2", "idx3"], "only-if-exists": True}

        runner = VespaDeleteIndex()
        result = await runner(vespa_client, params)

        # idx1 exists (delete), idx2 doesn't (skip), idx3 exists (delete)
        self.assertEqual(vespa_client.indices.delete.call_count, 2)
        self.assertEqual(result["weight"], 2)

    def test_repr(self):
        runner = VespaDeleteIndex()
        self.assertEqual(repr(runner), "vespa-delete-index")


class VespaIndicesStatsRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_stats_calls_indices_stats(self, mock_ctx):
        vespa_client = _make_vespa_client()
        stats_response = {"_all": {"primaries": {"docs": {"count": 1000}}}}
        vespa_client.indices.stats.return_value = stats_response

        params = {"index": "myindex"}

        runner = VespaIndicesStats()
        await runner(vespa_client, params)

        vespa_client.indices.stats.assert_called_once_with(index="myindex")
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_stats_returns_response(self, mock_ctx):
        vespa_client = _make_vespa_client()
        stats_response = {"_all": {"primaries": {"docs": {"count": 500}}}}
        vespa_client.indices.stats.return_value = stats_response

        params = {"index": "myindex"}

        runner = VespaIndicesStats()
        result = await runner(vespa_client, params)

        self.assertEqual(result["weight"], 1)
        self.assertEqual(result["unit"], "ops")
        self.assertEqual(result["stats"], stats_response)

    def test_repr(self):
        runner = VespaIndicesStats()
        self.assertEqual(repr(runner), "vespa-indices-stats")


class VespaClusterHealthRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_returns_green_success(self, mock_ctx):
        vespa_client = _make_vespa_client()
        vespa_client.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 0,
        }

        runner = VespaClusterHealth()
        result = await runner(vespa_client, {})

        self.assertTrue(result["success"])
        self.assertEqual(result["cluster-status"], "green")
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_returns_yellow_success(self, mock_ctx):
        # Design decision: yellow is treated as success (matches OpenSearch behavior)
        vespa_client = _make_vespa_client()
        vespa_client.cluster.health.return_value = {
            "status": "yellow",
            "relocating_shards": 0,
        }

        runner = VespaClusterHealth()
        result = await runner(vespa_client, {})

        self.assertTrue(result["success"])
        self.assertEqual(result["cluster-status"], "yellow")

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_returns_red_failure(self, mock_ctx):
        vespa_client = _make_vespa_client()
        vespa_client.cluster.health.return_value = {
            "status": "red",
            "relocating_shards": 0,
        }

        runner = VespaClusterHealth()
        result = await runner(vespa_client, {})

        self.assertFalse(result["success"])
        self.assertEqual(result["cluster-status"], "red")

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_includes_cluster_status(self, mock_ctx):
        vespa_client = _make_vespa_client()
        vespa_client.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 0,
        }

        runner = VespaClusterHealth()
        result = await runner(vespa_client, {})

        self.assertIn("cluster-status", result)

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_includes_relocating_shards(self, mock_ctx):
        vespa_client = _make_vespa_client()
        vespa_client.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 5,
        }

        runner = VespaClusterHealth()
        result = await runner(vespa_client, {})

        self.assertEqual(result["relocating-shards"], 5)

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_unknown_status_not_success(self, mock_ctx):
        # Only "green" and "yellow" are treated as success; anything else (including "unknown") fails
        vespa_client = _make_vespa_client()
        vespa_client.cluster.health.return_value = {
            "status": "unknown",
            "relocating_shards": 0,
        }

        runner = VespaClusterHealth()
        result = await runner(vespa_client, {})

        self.assertFalse(result["success"])
        self.assertEqual(result["cluster-status"], "unknown")

    def test_repr(self):
        runner = VespaClusterHealth()
        self.assertEqual(repr(runner), "vespa-cluster-health")


class VespaRefreshRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_refresh_calls_indices_refresh(self, mock_ctx):
        vespa_client = _make_vespa_client()
        vespa_client.indices.refresh.return_value = {
            "acknowledged": True,
            "_shards": {"total": 1, "successful": 1, "failed": 0},
        }

        params = {"index": "myindex"}

        runner = VespaRefresh()
        await runner(vespa_client, params)

        vespa_client.indices.refresh.assert_called_once_with(index="myindex")
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_refresh_returns_shards(self, mock_ctx):
        vespa_client = _make_vespa_client()
        shards = {"total": 2, "successful": 2, "failed": 0}
        vespa_client.indices.refresh.return_value = {
            "acknowledged": True,
            "_shards": shards,
        }

        params = {"index": "myindex"}

        runner = VespaRefresh()
        result = await runner(vespa_client, params)

        self.assertEqual(result["weight"], 1)
        self.assertEqual(result["unit"], "ops")
        self.assertEqual(result["shards"], shards)

    def test_repr(self):
        runner = VespaRefresh()
        self.assertEqual(repr(runner), "vespa-refresh")


class VespaForceMergeRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_force_merge_calls_forcemerge(self, mock_ctx):
        vespa_client = _make_vespa_client()
        vespa_client.indices.forcemerge = mock.AsyncMock(return_value={
            "_shards": {"total": 1, "successful": 1, "failed": 0},
        })

        params = {"index": "myindex"}

        runner = VespaForceMerge()
        await runner(vespa_client, params)

        vespa_client.indices.forcemerge.assert_called_once_with(index="myindex")
        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_force_merge_returns_shards(self, mock_ctx):
        vespa_client = _make_vespa_client()
        shards = {"total": 3, "successful": 3, "failed": 0}
        vespa_client.indices.forcemerge = mock.AsyncMock(return_value={"_shards": shards})

        params = {"index": "myindex"}

        runner = VespaForceMerge()
        result = await runner(vespa_client, params)

        self.assertEqual(result["weight"], 1)
        self.assertEqual(result["unit"], "ops")
        self.assertEqual(result["shards"], shards)

    def test_repr(self):
        runner = VespaForceMerge()
        self.assertEqual(repr(runner), "vespa-force-merge")


class VespaNoOpRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_logs_skip_message(self, mock_ctx):
        # VespaNoOp stubs out OS-only operations so workloads run without --exclude-tasks
        runner = VespaNoOp("put-pipeline")
        with mock.patch.object(runner, "logger") as mock_logger:
            await runner(_make_vespa_client(), {})
            mock_logger.info.assert_called_once()
            args = mock_logger.info.call_args[0]
            self.assertIn("put-pipeline", args[1])

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_returns_success(self, mock_ctx):
        runner = VespaNoOp("delete-pipeline")
        result = await runner(_make_vespa_client(), {})

        self.assertEqual(result["weight"], 1)
        self.assertEqual(result["unit"], "ops")
        self.assertTrue(result["success"])

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_sets_up_timing_context(self, mock_ctx):
        runner = VespaNoOp("create-search-pipeline")
        await runner(_make_vespa_client(), {})

        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    def test_repr_returns_name(self):
        runner = VespaNoOp("put-pipeline")
        self.assertEqual(repr(runner), "put-pipeline")

    def test_repr_returns_different_name(self):
        runner = VespaNoOp("custom-noop-operation")
        self.assertEqual(repr(runner), "custom-noop-operation")


class VespaWarmupIndicesRunnerTests(TestCase):

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_issues_warmup_queries(self, mock_ctx):
        vespa_client = _make_vespa_client()

        runner = VespaWarmupIndicesRunner()
        result = await runner(vespa_client, {"index": "target_index"})

        self.assertEqual(vespa_client.search.call_count, VespaWarmupIndicesRunner.DEFAULT_WARMUP_QUERIES)
        self.assertTrue(result["success"])

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_sets_up_timing_context(self, mock_ctx):
        vespa_client = _make_vespa_client()

        runner = VespaWarmupIndicesRunner()
        await runner(vespa_client, {"index": "target_index"})

        mock_ctx.on_client_request_start.assert_called_once()
        mock_ctx.on_request_start.assert_called_once()
        mock_ctx.on_request_end.assert_called_once()
        mock_ctx.on_client_request_end.assert_called_once()

    def test_repr(self):
        runner = VespaWarmupIndicesRunner()
        self.assertEqual(repr(runner), "warmup-knn-indices")


class RegisterVespaRunnersTests(TestCase):

    @mock.patch("osbenchmark.worker_coordinator.runner.register_runner")
    def test_registers_all_named_operations(self, mock_register):
        # Verifies that all OS OperationType enums that Vespa supports get a runner registered
        from osbenchmark import workload  # pylint: disable=import-outside-toplevel

        register_vespa_runners()

        registered_ops = [call[0][0] for call in mock_register.call_args_list]

        expected_ops = [
            workload.OperationType.Bulk,
            workload.OperationType.Search,
            workload.OperationType.PaginatedSearch,
            workload.OperationType.ScrollSearch,
            workload.OperationType.VectorSearch,
            workload.OperationType.BulkVectorDataSet,
            workload.OperationType.CreateIndex,
            workload.OperationType.DeleteIndex,
            workload.OperationType.IndexStats,
            workload.OperationType.ClusterHealth,
            workload.OperationType.Refresh,
            workload.OperationType.ForceMerge,
            "warmup-knn-indices",
        ]

        for op in expected_ops:
            self.assertIn(op, registered_ops, f"{op} should be registered")

    @mock.patch("osbenchmark.worker_coordinator.runner.register_runner")
    def test_registers_noop_operations(self, mock_register):
        # OS-only operations (pipelines, settings) get VespaNoOp stubs so workloads don't error
        from osbenchmark import workload  # pylint: disable=import-outside-toplevel

        register_vespa_runners()

        registered_ops = [call[0][0] for call in mock_register.call_args_list]

        noop_ops = [
            workload.OperationType.PutPipeline,
            workload.OperationType.DeletePipeline,
            workload.OperationType.CreateSearchPipeline,
            workload.OperationType.PutSettings,
        ]

        for op in noop_ops:
            self.assertIn(op, registered_ops, f"No-op {op} should be registered")

    @mock.patch("osbenchmark.worker_coordinator.runner.register_runner")
    def test_total_registration_count(self, mock_register):
        # Sentinel: catches accidentally added/removed registrations. Update if runner list changes.
        register_vespa_runners()

        # 13 named runners + 4 no-ops = 17 total
        self.assertEqual(mock_register.call_count, 17)

    @mock.patch("osbenchmark.worker_coordinator.runner.register_runner")
    def test_all_registered_as_async(self, mock_register):
        # All Vespa runners must be async_runner=True; sync registration would deadlock the event loop
        register_vespa_runners()

        for call in mock_register.call_args_list:
            kwargs = call[1]
            self.assertTrue(
                kwargs.get("async_runner", False),
                f"Registration for {call[0][0]} should have async_runner=True",
            )

    @mock.patch("osbenchmark.worker_coordinator.runner.register_runner")
    def test_named_runners_have_correct_types(self, mock_register):
        # Guards against accidental runner/operation-type mismatches (e.g., PaginatedSearch -> VespaQuery)
        register_vespa_runners()

        from osbenchmark import workload  # pylint: disable=import-outside-toplevel

        type_map = {}
        for call in mock_register.call_args_list:
            op_type = call[0][0]
            runner_instance = call[0][1]
            type_map[op_type] = type(runner_instance)

        self.assertEqual(type_map[workload.OperationType.Bulk], VespaBulkIndex)
        self.assertEqual(type_map[workload.OperationType.Search], VespaQuery)
        self.assertEqual(type_map[workload.OperationType.PaginatedSearch], VespaQuery)
        self.assertEqual(type_map[workload.OperationType.ScrollSearch], VespaScrollQuery)
        self.assertEqual(type_map[workload.OperationType.VectorSearch], VespaVectorSearch)
        self.assertEqual(type_map[workload.OperationType.BulkVectorDataSet], VespaBulkVectorDataSet)
        self.assertEqual(type_map[workload.OperationType.CreateIndex], VespaCreateIndex)
        self.assertEqual(type_map[workload.OperationType.DeleteIndex], VespaDeleteIndex)
        self.assertEqual(type_map[workload.OperationType.IndexStats], VespaIndicesStats)
        self.assertEqual(type_map[workload.OperationType.ClusterHealth], VespaClusterHealth)
        self.assertEqual(type_map[workload.OperationType.Refresh], VespaRefresh)
        self.assertEqual(type_map[workload.OperationType.ForceMerge], VespaForceMerge)
        self.assertEqual(type_map["warmup-knn-indices"], VespaWarmupIndicesRunner)

        # No-ops
        self.assertEqual(type_map[workload.OperationType.PutPipeline], VespaNoOp)
        self.assertEqual(type_map[workload.OperationType.DeletePipeline], VespaNoOp)
        self.assertEqual(type_map[workload.OperationType.CreateSearchPipeline], VespaNoOp)
        self.assertEqual(type_map[workload.OperationType.PutSettings], VespaNoOp)


class VespaRunnerYqlContractTests(TestCase):
    """Enforce the pre-translated YQL contract introduced by the engine refactor.

    The Vespa search runners no longer translate OpenSearch DSL → YQL at runtime.
    Workloads must provide a body with a "yql" key (produced by a Vespa-specific
    param source, e.g. the vectorsearch workload's "vespa-search-only" procedure).
    Any body without "yql" must raise BenchmarkError — silently falling through
    would produce empty result sets and silently invalid benchmarks.
    """

    # --- VespaVectorSearch ----------------------------------------------------

    @run_async
    async def test_vector_search_raises_without_yql(self):
        vespa_client = _make_vespa_client()
        params = {"body": {"query": {"knn": {"vec": {"vector": [1.0], "k": 10}}}}, "k": 10}

        runner = VespaVectorSearch()
        with self.assertRaises(exceptions.BenchmarkError) as ctx:
            await runner(vespa_client, params)
        self.assertIn("yql", str(ctx.exception))
        vespa_client.search.assert_not_called()

    @run_async
    async def test_vector_search_raises_with_empty_body(self):
        vespa_client = _make_vespa_client()
        runner = VespaVectorSearch()
        with self.assertRaises(exceptions.BenchmarkError):
            await runner(vespa_client, {"body": {}})
        vespa_client.search.assert_not_called()

    @mock.patch("osbenchmark.engine.vespa.runners.convert_vespa_response")
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_vector_search_accepts_pretranslated_yql(self, _mock_ctx, mock_convert):
        vespa_client = _make_vespa_client()
        vespa_client.search.return_value = _vespa_search_response()
        mock_convert.return_value = _opensearch_style_response()

        yql = "select * from sources * where {targetHits:10}nearestNeighbor(emb, q)"
        params = {"body": {"yql": yql}, "k": 10, "calculate-recall": False}

        runner = VespaVectorSearch()
        result = await runner(vespa_client, params)

        vespa_client.search.assert_called_once()
        sent_body = vespa_client.search.call_args.kwargs["body"]
        self.assertIn("yql", sent_body)
        # hits should be set to k for recall support
        self.assertEqual(sent_body["hits"], 10)
        self.assertTrue(result["success"])

    # --- VespaQuery -----------------------------------------------------------

    @run_async
    async def test_query_raises_without_yql(self):
        vespa_client = _make_vespa_client()
        params = {"body": {"query": {"match_all": {}}}}

        runner = VespaQuery()
        with self.assertRaises(exceptions.BenchmarkError) as ctx:
            await runner(vespa_client, params)
        self.assertIn("yql", str(ctx.exception))
        vespa_client.search.assert_not_called()

    @mock.patch("osbenchmark.engine.vespa.runners.convert_vespa_response")
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_query_accepts_pretranslated_yql(self, _mock_ctx, mock_convert):
        vespa_client = _make_vespa_client()
        vespa_client.search.return_value = _vespa_search_response()
        mock_convert.return_value = _opensearch_style_response()

        yql = "select * from sources * where true"
        params = {"body": {"yql": yql}}

        runner = VespaQuery()
        result = await runner(vespa_client, params)

        vespa_client.search.assert_called_once()
        sent_body = vespa_client.search.call_args.kwargs["body"]
        self.assertEqual(sent_body["yql"], yql)
        self.assertEqual(result["hits"], 1)

    # --- VespaScrollQuery -----------------------------------------------------

    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_scroll_query_raises_without_yql(self, _mock_ctx):
        vespa_client = _make_vespa_client()
        params = {"body": {"query": {"match_all": {}}}, "pages": 1, "results-per-page": 10}

        runner = VespaScrollQuery()
        with self.assertRaises(exceptions.BenchmarkError) as ctx:
            await runner(vespa_client, params)
        self.assertIn("yql", str(ctx.exception))
        vespa_client.search.assert_not_called()

    @mock.patch("osbenchmark.engine.vespa.runners.convert_vespa_response")
    @mock.patch("osbenchmark.engine.vespa.runners.request_context_holder")
    @run_async
    async def test_scroll_query_accepts_pretranslated_yql(self, _mock_ctx, mock_convert):
        vespa_client = _make_vespa_client()
        vespa_client.search.return_value = _vespa_search_response()
        mock_convert.return_value = {
            "took": 5,
            "timed_out": False,
            "hits": {"total": {"value": 1, "relation": "eq"}, "max_score": 1.0, "hits": []},
        }

        yql = "select * from sources * where true"
        params = {"body": {"yql": yql}, "pages": 1, "results-per-page": 10}

        runner = VespaScrollQuery()
        await runner(vespa_client, params)

        vespa_client.search.assert_called_once()
        sent_body = vespa_client.search.call_args.kwargs["body"]
        self.assertEqual(sent_body["yql"], yql)
