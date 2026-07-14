# SPDX-License-Identifier: Apache-2.0
"""Tests for osbenchmark.engine.clickhouse.helpers."""
# pylint: disable=protected-access,import-outside-toplevel

from unittest import TestCase, mock

import requests

from osbenchmark import exceptions
from osbenchmark.engine.clickhouse import helpers


class NsToMsTests(TestCase):
    """String -> milliseconds coercion for ClickHouse summary values."""

    def test_string_input_converts(self):
        # ClickHouse actually returns strings for summary values
        self.assertEqual(helpers._ns_to_ms({"elapsed_ns": "5000000"}), 5)

    def test_int_input_also_works(self):
        self.assertEqual(helpers._ns_to_ms({"elapsed_ns": 5_000_000}), 5)

    def test_missing_or_garbage_returns_zero(self):
        self.assertEqual(helpers._ns_to_ms(None), 0)
        self.assertEqual(helpers._ns_to_ms({}), 0)
        self.assertEqual(helpers._ns_to_ms({"elapsed_ns": "not-a-number"}), 0)


class QuoteIdentifierTests(TestCase):

    def test_plain_name(self):
        self.assertEqual(helpers.quote_identifier("foo"), "`foo`")

    def test_embedded_backtick(self):
        self.assertEqual(helpers.quote_identifier("f`o`o"), "`f``o``o`")

    def test_unicode(self):
        self.assertEqual(helpers.quote_identifier("日本語"), "`日本語`")

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            helpers.quote_identifier("")

    def test_none_raises(self):
        with self.assertRaises(ValueError):
            helpers.quote_identifier(None)

    def test_control_chars_raise(self):
        for bad in ("\x00bad", "b\rad", "b\nad"):
            with self.assertRaises(ValueError):
                helpers.quote_identifier(bad)


class ParseBulkBodyTests(TestCase):

    def test_bytes_input(self):
        body = (b'{"index":{"_id":"1"}}\n{"a":1}\n'
                b'{"index":{"_id":"2"}}\n{"a":2}\n')
        docs = helpers.parse_bulk_body(body)
        self.assertEqual(len(docs), 2)
        self.assertEqual(docs[0]["_id"], "1")
        self.assertEqual(docs[0]["_source"], {"a": 1})
        self.assertEqual(docs[0]["_action"], "index")

    def test_string_input(self):
        body = '{"index":{"_id":"1"}}\n{"a":1}'
        docs = helpers.parse_bulk_body(body)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["_source"], {"a": 1})

    def test_list_input(self):
        body = [{"index": {"_id": "1"}}, {"a": 1}]
        docs = helpers.parse_bulk_body(body)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["_source"], {"a": 1})

    def test_action_extraction_create(self):
        body = [{"create": {"_id": "5"}}, {"a": 5}]
        docs = helpers.parse_bulk_body(body)
        self.assertEqual(docs[0]["_action"], "create")
        self.assertEqual(docs[0]["_id"], "5")

    def test_missing_id_yields_none(self):
        body = [{"index": {}}, {"a": 1}]
        docs = helpers.parse_bulk_body(body)
        self.assertIsNone(docs[0]["_id"])

    def test_delete_action_rejected(self):
        body = [{"delete": {"_id": "1"}}, {"a": 1}]
        with self.assertRaises(exceptions.BenchmarkError):
            helpers.parse_bulk_body(body)

    def test_update_action_rejected(self):
        body = [{"update": {"_id": "1"}}, {"doc": {"a": 1}}]
        with self.assertRaises(exceptions.BenchmarkError):
            helpers.parse_bulk_body(body)

    def test_corruption_above_threshold_raises(self):
        # 3 corrupt lines out of 10 total -> raises
        body = "\n".join(["not-json", "not-json", "not-json"] +
                         ['{"index":{"_id":"1"}}', '{"a":1}'] * 3 +
                         ['{"index":{"_id":"2"}}'])
        with self.assertRaises(exceptions.BenchmarkError):
            helpers.parse_bulk_body(body)

    def test_already_normalized_list_returns_as_is(self):
        body = [{"_id": "1", "_source": {"a": 1}, "_action": "index"}]
        docs = helpers.parse_bulk_body(body)
        self.assertEqual(docs, body)


class RowsFromDocsTests(TestCase):

    def test_all_columns_present(self):
        docs = [{"_source": {"a": 1, "b": 2}}, {"_source": {"a": 3, "b": 4}}]
        rows = helpers.rows_from_docs(docs, ["a", "b"])
        self.assertEqual(rows, [(1, 2), (3, 4)])

    def test_missing_column_becomes_none(self):
        docs = [{"_source": {"a": 1}}]
        rows = helpers.rows_from_docs(docs, ["a", "b"])
        self.assertEqual(rows, [(1, None)])

    def test_strict_mode_raises(self):
        docs = [{"_id": "x", "_source": {"a": 1}}]
        with self.assertRaises(exceptions.BenchmarkError):
            helpers.rows_from_docs(docs, ["a", "b"], strict=True)

    def test_docs_have_extra_keys_true(self):
        docs = [{"_source": {"a": 1, "extra": 9}}]
        self.assertTrue(helpers.docs_have_extra_keys(docs, ["a"]))
        self.assertFalse(helpers.docs_have_extra_keys(docs, ["a", "extra"]))

    def test_empty_list_returns_empty(self):
        self.assertEqual(helpers.rows_from_docs([], ["a"]), [])
        self.assertFalse(helpers.docs_have_extra_keys([], ["a"]))


class CoerceParametersTests(TestCase):

    def test_none_returns_none(self):
        self.assertIsNone(helpers.coerce_parameters(None))

    def test_ndarray_becomes_list(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("numpy not available")
        arr = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        result = helpers.coerce_parameters({"v": arr})
        self.assertEqual(result, {"v": [1.0, 2.0, 3.0]})
        # Confirm scalars are Python floats, not numpy.float32
        for val in result["v"]:
            self.assertIsInstance(val, float)

    def test_scalar_generic_becomes_python(self):
        try:
            import numpy as np
        except ImportError:
            self.skipTest("numpy not available")
        val = np.float32(3.14)
        result = helpers.coerce_parameters({"x": val})
        self.assertIsInstance(result["x"], float)

    def test_non_numpy_passthrough(self):
        self.assertEqual(helpers.coerce_parameters({"a": 1, "b": [2, 3]}),
                         {"a": 1, "b": [2, 3]})


class ConvertQueryResultsTests(TestCase):

    def test_search_basic_rows(self):
        rows = [(1, "a"), (2, "b")]
        cols = ("id", "name")
        resp = helpers.convert_query_result_to_search_response(rows, cols)
        self.assertEqual(len(resp["hits"]["hits"]), 2)
        self.assertEqual(resp["hits"]["total"]["value"], 2)
        self.assertEqual(resp["hits"]["hits"][0]["_source"], {"id": 1, "name": "a"})

    def test_search_elapsed_string_to_int_took(self):
        resp = helpers.convert_query_result_to_search_response(
            [], (), elapsed_ns="5000000"
        )
        self.assertEqual(resp["took"], 5)

    def test_search_total_hits_override(self):
        resp = helpers.convert_query_result_to_search_response(
            [(1,)], ("id",), total_hits=999
        )
        self.assertEqual(resp["hits"]["total"]["value"], 999)

    def test_vector_score_populated(self):
        rows = [(1, 0.9, "x"), (2, 0.7, "y")]
        cols = ("id", "score", "name")
        resp = helpers.convert_query_result_for_vector_search(rows, cols)
        self.assertEqual(resp["hits"]["max_score"], 0.9)
        self.assertEqual(resp["hits"]["hits"][0]["_score"], 0.9)
        self.assertEqual(resp["hits"]["hits"][0]["_id"], "1")

    def test_vector_missing_score_column(self):
        rows = [(1, "x")]
        cols = ("id", "name")
        resp = helpers.convert_query_result_for_vector_search(rows, cols)
        self.assertIsNone(resp["hits"]["max_score"])
        self.assertIsNone(resp["hits"]["hits"][0]["_score"])


class BuildStatsAndVersionTests(TestCase):

    def test_build_stats_response_with_index(self):
        env = helpers.build_stats_response(rows=10, bytes_on_disk=1024, index_name="metrics")
        self.assertEqual(env["_all"]["primaries"]["docs"]["count"], 10)
        self.assertEqual(env["indices"]["metrics"]["total"]["store"]["size_in_bytes"], 1024)

    def test_build_stats_response_without_index(self):
        env = helpers.build_stats_response(rows=5, bytes_on_disk=512, index_name=None)
        self.assertEqual(env["indices"], {})
        self.assertEqual(env["_all"]["primaries"]["docs"]["count"], 5)

    def test_parse_version_common_shapes(self):
        self.assertEqual(helpers.parse_version("24.8.1.2684"), "24.8.1")
        self.assertEqual(helpers.parse_version("25.5.1.11-stable"), "25.5.1")
        self.assertEqual(helpers.parse_version("24.8.1.2684-cloud"), "24.8.1")

    def test_parse_version_fallback(self):
        self.assertEqual(helpers.parse_version(""), "24.8.0")
        self.assertEqual(helpers.parse_version(None), "24.8.0")
        self.assertEqual(helpers.parse_version("24.8"), "24.8.0")
        self.assertEqual(helpers.parse_version("head-fcbd7a4"), "24.8.0")


class WaitForClickHouseAndHostsTests(TestCase):

    @mock.patch("osbenchmark.engine.clickhouse.helpers.requests.get")
    def test_ready_on_first_attempt(self, mock_get):
        mock_get.return_value = mock.MagicMock(status_code=200, text="Ok.\n")
        client = mock.MagicMock(endpoint="http://h:8123", client_options={})
        self.assertTrue(helpers.wait_for_clickhouse(client, max_attempts=1, sleep_seconds=0))

    @mock.patch("osbenchmark.engine.clickhouse.helpers.time.sleep")
    @mock.patch("osbenchmark.engine.clickhouse.helpers.requests.get")
    def test_ssl_error_logs_warning(self, mock_get, _sleep):
        mock_get.side_effect = requests.exceptions.SSLError("cert issue")
        client = mock.MagicMock(endpoint="https://h:8443", client_options={"ssl_verify": True})
        with self.assertLogs("osbenchmark.engine.clickhouse.helpers", level="WARNING") as cm:
            self.assertFalse(helpers.wait_for_clickhouse(client, max_attempts=1, sleep_seconds=0))
        self.assertTrue(any("SSL" in msg for msg in cm.output))

    def test_parse_hosts_rejects_port_9000(self):
        with self.assertRaises(exceptions.SystemSetupError):
            helpers.parse_hosts([{"host": "h", "port": 9000}])

    def test_parse_hosts_ipv6(self):
        host, port, secure = helpers.parse_hosts([{"host": "[::1]", "port": 8123}])
        self.assertEqual(host, "::1")
        self.assertEqual(port, 8123)
        self.assertFalse(secure)

    def test_parse_hosts_secure_for_tls_ports(self):
        _, _, secure_8443 = helpers.parse_hosts([{"host": "h", "port": 8443}])
        _, _, secure_9440 = helpers.parse_hosts([{"host": "h", "port": 9440}])
        _, _, secure_8123 = helpers.parse_hosts([{"host": "h", "port": 8123}])
        self.assertTrue(secure_8443)
        self.assertTrue(secure_9440)
        self.assertFalse(secure_8123)


# -----------------------------------------------------------------
# Regression tests for review findings (P7)
# -----------------------------------------------------------------


class ParseBulkBodyStrictParseRegressionTests(TestCase):
    """F2: parse_bulk_body raises on ANY JSON parse error; even a single
    corrupt line silently mis-aligns the pair-consumer below."""

    def test_single_corrupt_line_raises(self):
        # 2 corrupt lines out of 200 total = 1% (previously silently tolerated).
        # Now: raises immediately.
        body_lines = ['{"index":{"_id":"' + str(i) + '"}}\n{"a":' + str(i) + '}'
                      for i in range(99)]
        body = "\n".join(body_lines[:50] + ["not-json", "still-not-json"] + body_lines[50:])
        with self.assertRaises(exceptions.BenchmarkError) as ctx:
            helpers.parse_bulk_body(body)
        self.assertIn("line", str(ctx.exception).lower())

    def test_zero_corrupt_lines_succeeds(self):
        body = '{"index":{"_id":"1"}}\n{"a":1}\n{"index":{"_id":"2"}}\n{"a":2}\n'
        docs = helpers.parse_bulk_body(body)
        self.assertEqual(len(docs), 2)


class ExtractActionRegressionTests(TestCase):
    """F10: _extract_action must raise on unknown action-meta so misaligned
    bulk bodies surface loudly instead of coercing to 'index'."""

    def test_unknown_key_raises(self):
        with self.assertRaises(exceptions.BenchmarkError):
            helpers._extract_action({"random": {"_id": "1"}})

    def test_empty_dict_raises(self):
        with self.assertRaises(exceptions.BenchmarkError):
            helpers._extract_action({})

    def test_recognized_key_still_works(self):
        self.assertEqual(helpers._extract_action({"index": {"_id": "1"}}), "index")
        self.assertEqual(helpers._extract_action({"create": {"_id": "1"}}), "create")


class ConvertQueryResultNullIdRegressionTests(TestCase):
    """F15: SQL NULL _id must coerce to '' (empty string), not the literal 'None'."""

    def test_search_response_null_id(self):
        rows = [(None, "a")]
        cols = ("_id", "name")
        resp = helpers.convert_query_result_to_search_response(rows, cols)
        self.assertEqual(resp["hits"]["hits"][0]["_id"], "")
        # Ensure the string 'None' didn't leak through
        self.assertNotEqual(resp["hits"]["hits"][0]["_id"], "None")

    def test_vector_response_null_id(self):
        rows = [(None, 0.9)]
        cols = ("id", "score")
        resp = helpers.convert_query_result_for_vector_search(rows, cols)
        self.assertEqual(resp["hits"]["hits"][0]["_id"], "")
        self.assertNotEqual(resp["hits"]["hits"][0]["_id"], "None")


class MultiHostWarnRegressionTests(TestCase):
    """F16: multi-host config must emit a WARNING pointing at the follow-up."""

    def test_two_hosts_logs_warning(self):
        with self.assertLogs("osbenchmark.engine.clickhouse.helpers",
                             level="WARNING") as cm:
            host, port, _ = helpers.parse_hosts([
                {"host": "h1", "port": 8123},
                {"host": "h2", "port": 8123},
            ])
        self.assertEqual(host, "h1")
        self.assertEqual(port, 8123)
        self.assertTrue(any("only the first" in m for m in cm.output))

    def test_single_host_no_warning(self):
        # Use a fresh logger context: assertNoLogs would be cleaner but is 3.10+;
        # we accept it may match INFO/DEBUG from prior tests. Guard by checking
        # that our WARNING pattern is NOT present.
        with self.assertLogs("osbenchmark.engine.clickhouse.helpers",
                             level="WARNING") as cm:
            helpers.parse_hosts([{"host": "h1", "port": 8123}])
            # Ensure the log records list has SOMETHING (assertLogs requires it).
            helpers.logger.warning("noise")
        self.assertFalse(any("only the first" in m for m in cm.output))
