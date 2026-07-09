# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import unittest
from datetime import datetime, timezone, timedelta
from unittest import TestCase
from unittest.mock import patch, MagicMock

from osbenchmark.engine.vespa.helpers import (
    map_field_name,
    is_leaf_value,
    date_to_epoch,
    transform_document_for_vespa,
    wrap_fields_with_assign,
    parse_bulk_body,
    convert_vespa_response,
    convert_metrics_to_stats,
    wait_for_vespa,
)


class MapFieldNameTests(TestCase):
    def test_dotted_path_to_underscore(self):
        self.assertEqual(map_field_name("log.file.path"), "log_file_path")

    def test_at_timestamp(self):
        self.assertEqual(map_field_name("@timestamp"), "timestamp")

    def test_known_mapping_entries(self):
        self.assertEqual(map_field_name("process.name"), "process_name")
        self.assertEqual(map_field_name("cloud.region"), "cloud_region")
        self.assertEqual(map_field_name("event.id"), "event_id")
        self.assertEqual(map_field_name("agent.name"), "agent_name")
        self.assertEqual(map_field_name("data_stream.type"), "data_stream_type")

    def test_unmapped_field_passthrough(self):
        self.assertEqual(map_field_name("simple_field"), "simple_field")

    def test_unmapped_dotted_field_uses_underscore_replace(self):
        self.assertEqual(map_field_name("unknown.nested.field"), "unknown_nested_field")


class IsLeafValueTests(TestCase):
    def test_geo_point_dict(self):
        self.assertTrue(is_leaf_value({"lat": 1, "lon": 2}))

    def test_type_coordinates(self):
        self.assertTrue(is_leaf_value({"type": "Point", "coordinates": [1, 2]}))

    def test_value_singleton(self):
        self.assertTrue(is_leaf_value({"value": 42}))

    def test_values_singleton(self):
        self.assertTrue(is_leaf_value({"values": [1, 2]}))

    def test_query_metadata(self):
        # Dicts with OpenSearch query DSL keys (query, analyzer, etc.) are treated as leaf data,
        # not nested document structure to flatten.
        self.assertTrue(is_leaf_value({"query": "test", "analyzer": "standard"}))

    def test_nested_dict(self):
        self.assertFalse(is_leaf_value({"inner": {"field": "val"}}))

    def test_non_dict_string(self):
        self.assertTrue(is_leaf_value("hello"))

    def test_non_dict_int(self):
        self.assertTrue(is_leaf_value(42))

    def test_non_dict_list(self):
        self.assertTrue(is_leaf_value([1, 2]))

    def test_non_dict_none(self):
        self.assertTrue(is_leaf_value(None))

    def test_dict_with_nested_dicts_and_no_data_keys(self):
        self.assertFalse(is_leaf_value({"a": {"b": 1}}))

    def test_dict_with_boost_key(self):
        # "boost" is a recognized query DSL data key, so this dict is leaf even though it's nested.
        self.assertTrue(is_leaf_value({"boost": 1.5, "value": "x"}))


class DateToEpochTests(TestCase):
    def test_iso8601_with_timezone(self):
        result = date_to_epoch("2023-01-15T10:30:00Z")
        expected = int(datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(result, expected)

    def test_iso8601_without_timezone(self):
        result = date_to_epoch("2023-01-15T10:30:00")
        expected = int(datetime(2023, 1, 15, 10, 30, 0).timestamp() * 1000)
        self.assertEqual(result, expected)

    def test_simple_date(self):
        result = date_to_epoch("2023-01-15")
        expected = int(datetime(2023, 1, 15).timestamp() * 1000)
        self.assertEqual(result, expected)

    def test_slash_date(self):
        result = date_to_epoch("2023/01/15")
        expected = int(datetime(2023, 1, 15).timestamp() * 1000)
        self.assertEqual(result, expected)

    def test_already_numeric_int_large(self):
        # Values >= 1e12 are assumed to already be in milliseconds — pass through unchanged.
        self.assertEqual(date_to_epoch(1673782200000), 1673782200000)

    def test_already_numeric_int_small(self):
        # Values < 1e12 are assumed to be seconds — multiplied by 1000 to get milliseconds.
        self.assertEqual(date_to_epoch(1673782200), 1673782200000)

    def test_already_numeric_float(self):
        self.assertEqual(date_to_epoch(1673782200.123), 1673782200123)

    def test_negative_milliseconds_passthrough(self):
        # Pre-epoch timestamp: abs() >= 1e12, so treated as already-ms. Not multiplied.
        self.assertEqual(date_to_epoch(-62135596800000), -62135596800000)

    def test_negative_seconds_multiplied(self):
        # Pre-epoch timestamp: abs() < 1e12, so treated as seconds and multiplied by 1000.
        self.assertEqual(date_to_epoch(-1000), -1000000)

    def test_fractional_seconds(self):
        result = date_to_epoch("2023-01-15T10:30:00.123Z")
        expected = int(datetime(2023, 1, 15, 10, 30, 0, 123000, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(result, expected)

    def test_fractional_seconds_with_positive_offset(self):
        result = date_to_epoch("2023-01-15T10:30:00.123+05:30")
        tz = timezone(timedelta(hours=5, minutes=30))
        expected = int(datetime(2023, 1, 15, 10, 30, 0, 123000, tzinfo=tz).timestamp() * 1000)
        self.assertEqual(result, expected)

    def test_fractional_seconds_with_negative_offset(self):
        result = date_to_epoch("2023-01-15T10:30:00.123-05:00")
        tz = timezone(timedelta(hours=-5))
        expected = int(datetime(2023, 1, 15, 10, 30, 0, 123000, tzinfo=tz).timestamp() * 1000)
        self.assertEqual(result, expected)

    def test_unparseable_returns_zero(self):
        self.assertEqual(date_to_epoch("not-a-date"), 0)

    def test_none_returns_zero(self):
        self.assertEqual(date_to_epoch(None), 0)

    def test_empty_string_returns_zero(self):
        self.assertEqual(date_to_epoch(""), 0)

    def test_boundary_value_1e12(self):
        # Exactly 1e12 hits the >= boundary — treated as ms, not seconds.
        self.assertEqual(date_to_epoch(1000000000000), 1000000000000)


class TransformDocumentForVespaTests(TestCase):
    def test_flat_document_passthrough(self):
        doc = {"status": 200, "message": "ok"}
        result = transform_document_for_vespa(doc)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["message"], "ok")

    def test_nested_object_flattened(self):
        doc = {"log": {"file": {"path": "x"}}}
        result = transform_document_for_vespa(doc)
        self.assertEqual(result["log_file_path"], "x")

    def test_timestamp_converted_from_at_timestamp(self):
        doc = {"@timestamp": "2023-01-15T10:30:00Z", "status": 200}
        result = transform_document_for_vespa(doc)
        expected_ts = int(datetime(2023, 1, 15, 10, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(result["timestamp"], expected_ts)
        self.assertEqual(result["status"], 200)

    def test_event_ingested_converted(self):
        doc = {"event": {"ingested": "2023-01-15T10:30:00Z"}}
        result = transform_document_for_vespa(doc)
        self.assertIsInstance(result["event_ingested"], int)
        self.assertGreater(result["event_ingested"], 0)

    def test_list_to_comma_separated(self):
        # Vespa doesn't support array fields in the same way — lists are joined into CSV strings.
        doc = {"tags": ["a", "b", "c"]}
        result = transform_document_for_vespa(doc)
        self.assertEqual(result["tags"], "a,b,c")

    def test_geo_point_preserved(self):
        # Geo point dicts are leaf values — they should NOT be flattened into location_lat/location_lon.
        doc = {"location": {"lat": 1.0, "lon": 2.0}}
        result = transform_document_for_vespa(doc)
        self.assertEqual(result["location"], {"lat": 1.0, "lon": 2.0})

    def test_deeply_nested_3_levels(self):
        doc = {"a": {"b": {"c": "val"}}}
        result = transform_document_for_vespa(doc)
        self.assertEqual(result["a_b_c"], "val")

    def test_empty_document(self):
        self.assertEqual(transform_document_for_vespa({}), {})

    def test_mixed_nested_and_flat(self):
        doc = {
            "@timestamp": "2023-01-15T10:30:00Z",
            "status": 200,
            "log": {"file": {"path": "/var"}}
        }
        result = transform_document_for_vespa(doc)
        self.assertIn("timestamp", result)
        self.assertIsInstance(result["timestamp"], int)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["log_file_path"], "/var")

    def test_mapped_field_via_original_path_lookup(self):
        # Flattening builds key "event_dataset" which isn't in FIELD_NAME_MAPPING directly,
        # so the code reconstructs the dot-path "event.dataset" and finds it there instead.
        doc = {"event": {"dataset": "test_ds"}}
        result = transform_document_for_vespa(doc)
        self.assertEqual(result["event_dataset"], "test_ds")


class ParseBulkBodyTests(TestCase):
    def test_bytes_input(self):
        body = b'{"index": {"_index": "test", "_id": "1"}}\n{"field": "value"}\n'
        result = parse_bulk_body(body)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_id"], "1")
        self.assertEqual(result[0]["_source"]["field"], "value")
        self.assertEqual(result[0]["_action"], "index")

    def test_string_input(self):
        body = '{"index": {"_index": "test", "_id": "1"}}\n{"field": "value"}\n'
        result = parse_bulk_body(body)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_id"], "1")

    def test_list_input(self):
        body = [
            {"index": {"_index": "test", "_id": "1"}},
            {"field": "value"},
            {"index": {"_index": "test", "_id": "2"}},
            {"field": "value2"}
        ]
        result = parse_bulk_body(body)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["_id"], "1")
        self.assertEqual(result[1]["_id"], "2")

    def test_index_action_extracted(self):
        body = b'{"index": {"_index": "x", "_id": "1"}}\n{"field": "val"}\n'
        result = parse_bulk_body(body)
        self.assertEqual(result[0]["_id"], "1")
        self.assertEqual(result[0]["_action"], "index")

    def test_update_action_extracted(self):
        # Update bodies wrap the actual fields in {"doc": {...}} — parser must unwrap it.
        body = [
            {"update": {"_id": "1"}},
            {"doc": {"field": "val"}}
        ]
        result = parse_bulk_body(body)
        self.assertEqual(result[0]["_action"], "update")
        self.assertEqual(result[0]["_source"]["field"], "val")

    def test_create_action_extracted(self):
        body = b'{"create": {"_id": "1"}}\n{"field": "val"}\n'
        result = parse_bulk_body(body)
        self.assertEqual(result[0]["_action"], "create")

    def test_id_generation_when_missing(self):
        body = '{"index": {"_index": "test"}}\n{"field": "val"}\n'
        result = parse_bulk_body(body)
        self.assertEqual(len(result), 1)
        # Should have a generated UUID
        self.assertIsNotNone(result[0]["_id"])
        self.assertNotEqual(result[0]["_id"], "")

    def test_empty_lines_skipped(self):
        body = '{"index": {"_id": "1"}}\n{"field": "val"}\n\n\n{"index": {"_id": "2"}}\n{"field": "val2"}\n'
        result = parse_bulk_body(body)
        self.assertEqual(len(result), 2)

    def test_invalid_json_line_skipped(self):
        body = 'not valid json\n{"index": {"_id": "1"}}\n{"field": "val"}\n'
        result = parse_bulk_body(body)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_id"], "1")

    def test_list_input_with_no_action_keys(self):
        # When no recognized action key (index/update/create) is found, defaults to "index"
        # and still pairs items as [action, doc]. The first item becomes the action metadata
        # (its content is ignored), and the second becomes _source.
        body = [{"field1": "val1"}, {"field2": "val2"}]
        result = parse_bulk_body(body)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_source"], {"field2": "val2"})

    def test_odd_length_list_drops_trailing_item(self):
        # range(0, len-1, 2) means the unpaired trailing action is silently dropped.
        body = [
            {"index": {"_index": "test", "_id": "1"}},
            {"field": "value1"},
            {"index": {"_index": "test", "_id": "2"}},
        ]
        result = parse_bulk_body(body)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_id"], "1")


class WrapFieldsWithAssignTests(TestCase):
    def test_simple_fields(self):
        self.assertEqual(wrap_fields_with_assign({"f": "v"}), {"f": {"assign": "v"}})

    def test_nested_values(self):
        result = wrap_fields_with_assign({"f": {"inner": 1}})
        self.assertEqual(result, {"f": {"assign": {"inner": 1}}})

    def test_empty(self):
        self.assertEqual(wrap_fields_with_assign({}), {})

    def test_multiple_fields(self):
        result = wrap_fields_with_assign({"a": 1, "b": "x"})
        self.assertEqual(result, {"a": {"assign": 1}, "b": {"assign": "x"}})


class ConvertVespaResponseTests(TestCase):
    def test_basic_response(self):
        vespa_resp = {
            "root": {
                "fields": {"totalCount": 5},
                "children": [
                    {"id": "doc1", "relevance": 1.5, "fields": {"status": 200}},
                    {"id": "doc2", "relevance": 1.0, "fields": {"status": 404}}
                ]
            }
        }
        result = convert_vespa_response(vespa_resp)
        self.assertEqual(result["hits"]["total"]["value"], 5)
        self.assertEqual(len(result["hits"]["hits"]), 2)

    def test_empty_response_no_children(self):
        result = convert_vespa_response({"root": {}})
        self.assertEqual(result["hits"]["hits"], [])
        self.assertEqual(result["hits"]["total"]["value"], 0)

    def test_empty_response_no_root(self):
        result = convert_vespa_response({})
        self.assertEqual(result["hits"]["hits"], [])
        self.assertEqual(result["hits"]["total"]["value"], 0)

    def test_relevance_to_score(self):
        vespa_resp = {
            "root": {
                "children": [{"id": "1", "relevance": 3.14, "fields": {}}]
            }
        }
        result = convert_vespa_response(vespa_resp)
        self.assertEqual(result["hits"]["hits"][0]["_score"], 3.14)

    def test_max_score_from_first_hit(self):
        # Vespa returns hits sorted by relevance, so the first hit's relevance is max_score.
        # This avoids scanning all hits just to find the maximum.
        vespa_resp = {
            "root": {
                "children": [
                    {"id": "1", "relevance": 5.0, "fields": {}},
                    {"id": "2", "relevance": 3.0, "fields": {}}
                ]
            }
        }
        result = convert_vespa_response(vespa_resp)
        self.assertEqual(result["hits"]["max_score"], 5.0)

    def test_timed_out_always_false(self):
        result = convert_vespa_response({"root": {"children": []}})
        self.assertFalse(result["timed_out"])

    def test_took_from_timing(self):
        # Vespa reports searchtime in seconds; OpenSearch "took" is in milliseconds.
        vespa_resp = {"root": {"children": []}, "timing": {"searchtime": 0.042}}
        result = convert_vespa_response(vespa_resp)
        self.assertEqual(result["took"], 42)

    def test_hit_id_and_fields(self):
        vespa_resp = {
            "root": {
                "children": [{"id": "my_id", "relevance": 1.0, "fields": {"msg": "hello"}}]
            }
        }
        result = convert_vespa_response(vespa_resp)
        hit = result["hits"]["hits"][0]
        self.assertEqual(hit["_id"], "my_id")
        self.assertEqual(hit["_source"], {"msg": "hello"})


class ConvertMetricsToStatsTests(TestCase):
    def test_returns_default_structure(self):
        result = convert_metrics_to_stats({}, "test_index")
        self.assertIn("_all", result)
        self.assertIn("primaries", result["_all"])
        self.assertIn("total", result["_all"])
        self.assertIn("docs", result["_all"]["primaries"])
        self.assertIn("count", result["_all"]["primaries"]["docs"])
        self.assertIn("store", result["_all"]["total"])
        self.assertIn("size_in_bytes", result["_all"]["total"]["store"])

    def test_with_index_param(self):
        result = convert_metrics_to_stats({}, "my_index")
        self.assertIn("_all", result)

    def test_with_none_index(self):
        # Index param is accepted but unused — stub always returns the same structure.
        result = convert_metrics_to_stats({}, None)
        self.assertIn("_all", result)


class WaitForVespaTests(TestCase):
    @patch("requests.get")
    def test_success_on_up_status(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": {"code": "up"}}
        mock_get.return_value = mock_response

        client = MagicMock()
        client.endpoint = "http://localhost:8080"

        result = wait_for_vespa(client)
        self.assertTrue(result)

    @patch("requests.get")
    def test_success_on_initializing_status(self, mock_get):
        # "initializing" is treated as ready — Vespa can serve requests while still warming up.
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": {"code": "initializing"}}
        mock_get.return_value = mock_response

        client = MagicMock()
        client.endpoint = "http://localhost:8080"

        result = wait_for_vespa(client)
        self.assertTrue(result)

    @patch("requests.get")
    @patch("osbenchmark.engine.vespa.helpers.time.sleep")
    def test_timeout_after_max_attempts(self, mock_sleep, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": {"code": "down"}}
        mock_get.return_value = mock_response

        client = MagicMock()
        client.endpoint = "http://localhost:8080"

        result = wait_for_vespa(client, max_attempts=3)
        self.assertFalse(result)
        self.assertEqual(mock_get.call_count, 3)

    @patch("requests.get")
    @patch("osbenchmark.engine.vespa.helpers.time.sleep")
    def test_retries_on_exception(self, mock_sleep, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": {"code": "up"}}
        mock_get.side_effect = [ConnectionError("fail"), mock_response]

        client = MagicMock()
        client.endpoint = "http://localhost:8080"

        result = wait_for_vespa(client, max_attempts=5)
        self.assertTrue(result)
        self.assertEqual(mock_get.call_count, 2)

    @patch("requests.get")
    @patch("osbenchmark.engine.vespa.helpers.time.sleep")
    def test_max_attempts_respected(self, mock_sleep, mock_get):
        mock_get.side_effect = ConnectionError("fail")

        client = MagicMock()
        client.endpoint = "http://localhost:8080"

        result = wait_for_vespa(client, max_attempts=2)
        self.assertFalse(result)
        self.assertEqual(mock_get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
