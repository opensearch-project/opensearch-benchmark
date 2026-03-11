# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import unittest
from datetime import datetime, timezone, timedelta
from unittest import TestCase
from unittest.mock import patch, MagicMock

from osbenchmark.database.clients.vespa.helpers import (
    map_field_name,
    is_leaf_value,
    date_to_epoch,
    transform_document_for_vespa,
    wrap_fields_with_assign,
    parse_bulk_body,
    convert_to_yql,
    _build_search_after_filter,
    build_where_clause,
    convert_knn_query,
    build_order_clause,
    build_limit_clause,
    build_grouping_clause,
    convert_aggregation,
    convert_date_histogram_agg,
    convert_terms_agg,
    convert_cardinality_agg,
    convert_range_agg,
    convert_histogram_agg,
    convert_auto_date_histogram_agg,
    convert_composite_agg,
    convert_multi_terms_agg,
    convert_metric_agg,
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


class ConvertToYqlTests(TestCase):
    def test_empty_body_none(self):
        yql, _ = convert_to_yql(None, "mytype")
        self.assertEqual(yql, "select * from mytype where true")

    def test_empty_body_empty_dict(self):
        # Empty dict is falsy in Python, so it takes the early-return path same as None.
        yql, _ = convert_to_yql({}, "mytype")
        self.assertEqual("select * from mytype where true", yql)

    def test_match_all_sets_ranking_unranked(self):
        # match_all has no scoring — tell Vespa to skip ranking for performance.
        body = {"query": {"match_all": {}}}
        _, params = convert_to_yql(body, "mytype")
        self.assertEqual(params["ranking"], "unranked")

    def test_with_sort(self):
        body = {"query": {"match_all": {}}, "sort": [{"timestamp": "desc"}]}
        yql, _ = convert_to_yql(body, "mytype")
        self.assertIn("order by timestamp desc", yql)

    def test_with_size_and_from(self):
        body = {"query": {"match_all": {}}, "size": 10, "from": 20}
        yql, _ = convert_to_yql(body, "mytype")
        self.assertIn("limit 10 offset 20", yql)

    def test_with_search_after_replaces_true(self):
        # When the only WHERE clause is "true" (from match_all), search_after replaces it
        # entirely with a range filter rather than appending "true and ...".
        body = {
            "query": {"match_all": {}},
            "sort": [{"timestamp": "desc"}],
            "search_after": [1000]
        }
        yql, _ = convert_to_yql(body, "mytype")
        self.assertIn("timestamp < 1000", yql)
        self.assertNotIn("where true", yql)

    def test_with_search_after_appends_to_existing_where(self):
        body = {
            "query": {"term": {"status": 200}},
            "sort": [{"timestamp": "desc"}],
            "search_after": [1000]
        }
        yql, _ = convert_to_yql(body, "mytype")
        self.assertIn("status = 200", yql)
        self.assertIn("and timestamp < 1000", yql)

    def test_with_aggregations(self):
        body = {
            "query": {"match_all": {}},
            "aggs": {"my_sum": {"sum": {"field": "x"}}}
        }
        yql, _ = convert_to_yql(body, "mytype")
        self.assertIn("| all(output(sum(x)))", yql)

    def test_aggs_key_takes_precedence_over_aggregations(self):
        # OpenSearch accepts both "aggs" and "aggregations" — when both are present,
        # "aggs" takes precedence (matches body.get("aggs", body.get("aggregations")) order).
        body = {
            "query": {"match_all": {}},
            "aggs": {"my_sum": {"sum": {"field": "x"}}},
            "aggregations": {"my_avg": {"avg": {"field": "y"}}},
        }
        yql, _ = convert_to_yql(body, "mytype")
        self.assertIn("sum(x)", yql)
        self.assertNotIn("avg(y)", yql)

    def test_with_request_timeout(self):
        body = {"query": {"match_all": {}}, "request-timeout": 30}
        _, params = convert_to_yql(body, "mytype")
        self.assertEqual(params["timeout"], "30s")

    def test_aggs_key_alias(self):
        body1 = {"aggs": {"my_sum": {"sum": {"field": "x"}}}}
        body2 = {"aggregations": {"my_sum": {"sum": {"field": "x"}}}}
        yql1, _ = convert_to_yql(body1, "mytype")
        yql2, _ = convert_to_yql(body2, "mytype")
        self.assertIn("all(output(sum(x)))", yql1)
        self.assertIn("all(output(sum(x)))", yql2)


class BuildWhereClauseTests(TestCase):  # pylint: disable=too-many-public-methods
    def _build(self, query, params=None):
        if params is None:
            params = {}
        return build_where_clause(query, "mytype", params)

    def test_empty_query(self):
        self.assertEqual(self._build({}), "true")

    def test_match_all_query(self):
        params = {}
        result = build_where_clause({"match_all": {}}, "mytype", params)
        self.assertEqual(result, "true")
        self.assertEqual(params["ranking"], "unranked")

    def test_term_query_numeric(self):
        self.assertEqual(self._build({"term": {"status": 200}}), "status = 200")

    def test_term_query_string(self):
        self.assertEqual(self._build({"term": {"status": "active"}}), 'status contains "active"')

    def test_term_query_dict_value_spec(self):
        self.assertEqual(
            self._build({"term": {"status": {"value": "active"}}}),
            'status contains "active"'
        )

    def test_term_query_with_special_chars(self):
        result = self._build({"term": {"msg": 'say "hello"'}})
        self.assertIn('\\"hello\\"', result)

    def test_terms_query(self):
        result = self._build({"terms": {"status": [200, 404]}})
        self.assertEqual(result, "(status = 200 or status = 404)")

    def test_terms_query_strings(self):
        result = self._build({"terms": {"tag": ["a", "b"]}})
        self.assertEqual(result, '(tag contains "a" or tag contains "b")')

    def test_terms_query_ignores_boost(self):
        # "boost" is a sibling key to the field in terms queries — must be skipped, not treated as a field.
        result = self._build({"terms": {"status": [200], "boost": 1.5}})
        self.assertEqual(result, "(status = 200)")

    def test_range_query_gte_lt(self):
        result = self._build({"range": {"age": {"gte": 18, "lt": 65}}})
        self.assertIn("age >= 18", result)
        self.assertIn("age < 65", result)

    def test_range_query_gt_lte(self):
        result = self._build({"range": {"age": {"gt": 18, "lte": 65}}})
        self.assertIn("age > 18", result)
        self.assertIn("age <= 65", result)

    def test_range_query_date_field_converted(self):
        result = self._build({"range": {"@timestamp": {"gte": "2023-01-01", "lt": "2023-01-03"}}})
        # Should contain epoch ms values, not the original date strings
        self.assertNotIn("2023-01-01", result)
        self.assertNotIn("2023-01-03", result)
        self.assertIn("timestamp >=", result)
        self.assertIn("timestamp <", result)

    def test_range_query_skips_format_and_timezone(self):
        # "format" and "time_zone" are OpenSearch-specific hints — Vespa doesn't use them,
        # so they must be silently ignored rather than emitted as range operators.
        result = self._build({"range": {"ts": {"gte": 1, "format": "epoch_millis", "time_zone": "UTC"}}})
        self.assertEqual(result, "ts >= 1")

    def test_match_query_string_value(self):
        result = self._build({"match": {"body": "test text"}})
        self.assertEqual(result, 'body contains "test text"')

    def test_match_query_dict_value_spec(self):
        result = self._build({"match": {"body": {"query": "test text"}}})
        self.assertEqual(result, 'body contains "test text"')

    def test_match_query_numeric_value(self):
        result = self._build({"match": {"count": 42}})
        self.assertEqual(result, "count = 42")

    def test_match_phrase_query(self):
        result = self._build({"match_phrase": {"body": "exact phrase"}})
        self.assertEqual(result, 'body contains phrase("exact", "phrase")')

    def test_match_phrase_dict_value_spec(self):
        result = self._build({"match_phrase": {"body": {"query": "exact phrase"}}})
        self.assertEqual(result, 'body contains phrase("exact", "phrase")')

    def test_bool_must(self):
        query = {"bool": {"must": [{"term": {"status": 200}}, {"match": {"body": "test"}}]}}
        result = self._build(query)
        self.assertIn("status = 200", result)
        self.assertIn('body contains "test"', result)
        self.assertIn(" and ", result)

    def test_bool_must_single(self):
        query = {"bool": {"must": {"term": {"status": 200}}}}
        result = self._build(query)
        self.assertEqual(result, "status = 200")

    def test_bool_should(self):
        query = {"bool": {"should": [{"term": {"status": 200}}, {"term": {"status": 404}}]}}
        result = self._build(query)
        self.assertIn("status = 200", result)
        self.assertIn("status = 404", result)
        self.assertIn(" or ", result)

    def test_bool_must_not(self):
        query = {"bool": {"must_not": [{"term": {"status": 500}}]}}
        result = self._build(query)
        self.assertIn("!(status = 500)", result)

    def test_bool_filter(self):
        query = {"bool": {"filter": [{"range": {"age": {"gte": 18}}}]}}
        result = self._build(query)
        self.assertIn("age >= 18", result)

    def test_bool_nested(self):
        query = {
            "bool": {
                "must": [
                    {"bool": {"should": [{"term": {"a": 1}}, {"term": {"b": 2}}]}}
                ]
            }
        }
        result = self._build(query)
        self.assertIn("a = 1", result)
        self.assertIn("b = 2", result)
        self.assertIn(" or ", result)

    def test_bool_empty(self):
        self.assertEqual(self._build({"bool": {}}), "true")

    def test_bool_filters_out_true_clauses(self):
        # match_all produces "true" — bool must filter those out to avoid "true and status = 200".
        query = {"bool": {"must": [{"match_all": {}}, {"term": {"status": 200}}]}}
        params = {}
        result = build_where_clause(query, "mytype", params)
        self.assertEqual(result, "status = 200")

    def test_query_string_with_or(self):
        result = self._build({"query_string": {"query": "foo OR bar"}})
        self.assertEqual(result, '(message contains "foo" or message contains "bar")')

    def test_query_string_with_and(self):
        result = self._build({"query_string": {"query": "foo AND bar"}})
        self.assertEqual(result, '(message contains "foo" and message contains "bar")')

    def test_query_string_with_field_colon(self):
        result = self._build({"query_string": {"query": "status:active"}})
        self.assertEqual(result, 'status contains "active"')

    def test_query_string_default_field(self):
        result = self._build({"query_string": {"query": "test", "default_field": "body"}})
        self.assertEqual(result, 'body contains "test"')

    def test_query_string_space_separated(self):
        # Without explicit OR/AND operators, space-separated terms default to OR.
        result = self._build({"query_string": {"query": "foo bar"}})
        self.assertEqual(result, '(message contains "foo" or message contains "bar")')

    def test_query_string_with_quotes_escaped(self):
        result = self._build({"query_string": {"query": 'say "hello"'}})
        self.assertEqual(result, '(message contains "say" or message contains "\\"hello\\"")')

    def test_prefix_query(self):
        result = self._build({"prefix": {"name": "pre"}})
        self.assertEqual(result, 'name contains ({prefix:true})"pre"')

    def test_prefix_query_dict_value_spec(self):
        result = self._build({"prefix": {"name": {"value": "pre"}}})
        self.assertEqual(result, 'name contains ({prefix:true})"pre"')

    def test_wildcard_query(self):
        result = self._build({"wildcard": {"name": "pat*"}})
        self.assertEqual(result, 'name matches "pat.*"')

    def test_wildcard_query_dict_value_spec(self):
        result = self._build({"wildcard": {"name": {"value": "pat*"}}})
        self.assertEqual(result, 'name matches "pat.*"')

    def test_exists_query(self):
        result = self._build({"exists": {"field": "name"}})
        self.assertEqual(result, "name != null")

    def test_exists_query_with_mapped_field(self):
        result = self._build({"exists": {"field": "@timestamp"}})
        self.assertEqual(result, "timestamp != null")

    def test_unsupported_query_falls_back_to_true(self):
        result = self._build({"unknown_type": {}})
        self.assertEqual(result, "true")

    def test_knn_query_routes_to_knn_converter(self):
        params = {}
        result = build_where_clause(
            {"knn": {"target_field": {"vector": [1.0, 2.0], "k": 100}}},
            "mytype", params
        )
        self.assertIn("nearestNeighbor", result)


class ConvertKnnQueryTests(TestCase):
    def test_nested_format(self):
        params = {}
        result = convert_knn_query({"target_field": {"vector": [1.0, 2.0], "k": 100}}, params)
        self.assertEqual(result, "{targetHits:100}nearestNeighbor(target_field, query_vector)")

    def test_flat_format_fallback(self):
        params = {}
        result = convert_knn_query({"field": "vec", "vector": [1.0], "k": 10}, params)
        self.assertEqual(result, "{targetHits:10}nearestNeighbor(vec, query_vector)")

    def test_vector_stored_in_query_params(self):
        params = {}
        convert_knn_query({"target_field": {"vector": [1.0, 2.0], "k": 100}}, params)
        self.assertEqual(params["input.query(query_vector)"], "[1.0,2.0]")

    def test_ranking_set(self):
        params = {}
        convert_knn_query({"target_field": {"vector": [1.0], "k": 10}}, params)
        self.assertEqual(params["ranking"], "vector-similarity")

    def test_field_name_mapped(self):
        params = {}
        result = convert_knn_query({"@timestamp": {"vector": [1.0], "k": 10}}, params)
        self.assertIn("nearestNeighbor(timestamp,", result)


class BuildOrderClauseTests(TestCase):
    def test_single_field_asc(self):
        self.assertEqual(build_order_clause([{"field": "asc"}]), "field asc")

    def test_single_field_desc(self):
        self.assertEqual(build_order_clause([{"field": "desc"}]), "field desc")

    def test_multiple_fields(self):
        result = build_order_clause([{"ts": "desc"}, {"id": "asc"}])
        self.assertEqual(result, "ts desc, id asc")

    def test_dict_direction_spec(self):
        result = build_order_clause([{"field": {"order": "desc"}}])
        self.assertEqual(result, "field desc")

    def test_score_sort_skipped(self):
        # _score sorting is implicit in Vespa ranking — no ORDER BY clause needed.
        self.assertEqual(build_order_clause([{"_score": "desc"}]), "")

    def test_score_among_other_fields(self):
        result = build_order_clause([{"_score": "desc"}, {"ts": "asc"}])
        self.assertEqual(result, "ts asc")

    def test_empty_sort(self):
        self.assertEqual(build_order_clause([]), "")

    def test_string_sort_item(self):
        self.assertEqual(build_order_clause(["field_name"]), "field_name asc")

    def test_field_name_mapping(self):
        result = build_order_clause([{"@timestamp": "desc"}])
        self.assertEqual(result, "timestamp desc")


class BuildLimitClauseTests(TestCase):
    def test_explicit_size(self):
        self.assertEqual(build_limit_clause({"size": 10}), "limit 10")

    def test_size_and_from(self):
        self.assertEqual(build_limit_clause({"size": 10, "from": 20}), "limit 10 offset 20")

    def test_default_size_is_10(self):
        # No "size" key in body defaults to 10, matching OpenSearch's default page size.
        self.assertEqual(build_limit_clause({}), "limit 10")

    def test_from_zero_no_offset(self):
        # from=0 is the default start — omit the offset clause to keep YQL clean.
        self.assertEqual(build_limit_clause({"size": 5, "from": 0}), "limit 5")


class BuildSearchAfterFilterTests(TestCase):
    def test_desc_sort(self):
        result = _build_search_after_filter([1000], [{"ts": "desc"}])
        self.assertEqual(result, "ts < 1000")

    def test_asc_sort(self):
        result = _build_search_after_filter([1000], [{"ts": "asc"}])
        self.assertEqual(result, "ts > 1000")

    def test_multiple_sort_fields(self):
        result = _build_search_after_filter(
            [100, 200],
            [{"f1": "desc"}, {"f2": "asc"}]
        )
        self.assertEqual(result, "f1 < 100 and f2 > 200")

    def test_score_field_skipped(self):
        # _score has no Vespa field equivalent — skip it to avoid invalid YQL range filters.
        result = _build_search_after_filter([1.0], [{"_score": "desc"}])
        self.assertEqual(result, "")

    def test_date_field_converts_value(self):
        result = _build_search_after_filter(
            ["2023-01-15T10:30:00Z"],
            [{"@timestamp": "desc"}]
        )
        self.assertIn("timestamp <", result)
        # Should be epoch ms, not the original string
        self.assertNotIn("2023-01-15", result)

    def test_dict_direction_spec(self):
        result = _build_search_after_filter([1000], [{"ts": {"order": "desc"}}])
        self.assertEqual(result, "ts < 1000")

    def test_empty_returns_empty(self):
        self.assertEqual(_build_search_after_filter([], []), "")


class BuildGroupingClauseTests(TestCase):
    def test_empty_aggs(self):
        self.assertEqual(build_grouping_clause({}), "")

    def test_single_metric_agg_wrapped_in_all(self):
        # Standalone metric aggs (output(...)) need an all() wrapper in Vespa grouping syntax.
        aggs = {"my_sum": {"sum": {"field": "x"}}}
        result = build_grouping_clause(aggs)
        self.assertEqual(result, "all(output(sum(x)))")

    def test_single_bucket_agg_wrapped_in_all(self):
        aggs = {"by_status": {"terms": {"field": "status"}}}
        result = build_grouping_clause(aggs)
        self.assertIn("all(group(status)", result)

    def test_mixed_metrics_and_buckets(self):
        # Metrics and buckets are wrapped in separate all() clauses: metrics are combined
        # into one all(...), while each bucket agg gets its own all(...).
        aggs = {
            "my_sum": {"sum": {"field": "x"}},
            "by_status": {"terms": {"field": "status"}}
        }
        result = build_grouping_clause(aggs)
        self.assertIn("all(output(sum(x)))", result)
        self.assertIn("all(group(status)", result)

    def test_multiple_bucket_aggs(self):
        aggs = {
            "by_status": {"terms": {"field": "status"}},
            "by_region": {"terms": {"field": "region"}}
        }
        result = build_grouping_clause(aggs)
        self.assertIn("all(group(status)", result)
        self.assertIn("all(group(region)", result)


class ConvertAggregationTests(TestCase):
    def test_routes_to_date_histogram(self):
        result = convert_aggregation("my_dh", {"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}})
        self.assertIn("group(floor(timestamp /", result)

    def test_routes_to_terms(self):
        result = convert_aggregation("my_terms", {"terms": {"field": "status"}})
        self.assertIn("group(status)", result)

    def test_routes_to_significant_terms_as_terms(self):
        # Vespa has no native significant_terms — we approximate it with a regular terms agg.
        result = convert_aggregation("sig", {"significant_terms": {"field": "status"}})
        self.assertIn("group(status)", result)

    def test_nested_sub_aggregations_recursion(self):
        result = convert_aggregation("top", {
            "terms": {"field": "status"},
            "aggs": {"inner_sum": {"sum": {"field": "x"}}}
        })
        self.assertIn("output(count())", result)
        self.assertIn("output(sum(x))", result)

    def test_nested_bucket_in_bucket(self):
        # A bucket agg nested inside another bucket needs its own all() wrapper in Vespa.
        result = convert_aggregation("top", {
            "terms": {"field": "status"},
            "aggs": {"inner": {"terms": {"field": "region"}}}
        })
        self.assertIn("all(group(region)", result)

    def test_unknown_agg_returns_empty(self):
        result = convert_aggregation("unk", {"unknown_type": {"field": "x"}})
        self.assertEqual(result, "")


class ConvertDateHistogramAggTests(TestCase):
    def test_fixed_interval_1h(self):
        result = convert_date_histogram_agg({"field": "timestamp", "fixed_interval": "1h"})
        self.assertIn("floor(timestamp / 3600000)", result)

    def test_fixed_interval_1s(self):
        result = convert_date_histogram_agg({"field": "timestamp", "fixed_interval": "1s"})
        self.assertIn("floor(timestamp / 1000)", result)

    def test_calendar_interval_day(self):
        result = convert_date_histogram_agg({"field": "timestamp", "calendar_interval": "day"})
        self.assertIn("floor(timestamp / 86400000)", result)

    def test_calendar_interval_month(self):
        # "month" uses a fixed 30-day approximation (2592000000ms) since Vespa floor()
        # can't handle variable-length calendar months.
        result = convert_date_histogram_agg({"field": "timestamp", "calendar_interval": "month"})
        self.assertIn("floor(timestamp / 2592000000)", result)

    def test_default_interval_hour(self):
        result = convert_date_histogram_agg({"field": "timestamp"})
        self.assertIn("floor(timestamp / 3600000)", result)

    def test_with_nested_content(self):
        result = convert_date_histogram_agg({"field": "timestamp"}, "output(sum(x))")
        self.assertIn("output(count()) output(sum(x))", result)

    def test_field_name_mapped(self):
        result = convert_date_histogram_agg({"field": "@timestamp", "fixed_interval": "1h"})
        self.assertIn("floor(timestamp / 3600000)", result)


class ConvertTermsAggTests(TestCase):
    def test_basic_terms(self):
        result = convert_terms_agg({"field": "status", "size": 10})
        self.assertEqual(result, "group(status) max(10) order(-count()) each(output(count()))")

    def test_default_size(self):
        result = convert_terms_agg({"field": "status"})
        self.assertIn("max(10)", result)

    def test_with_nested_sub_agg(self):
        result = convert_terms_agg({"field": "status"}, "output(sum(x))")
        self.assertIn("each(output(count()) output(sum(x)))", result)

    def test_field_name_mapped(self):
        result = convert_terms_agg({"field": "@timestamp"})
        self.assertIn("group(timestamp)", result)


class ConvertCardinalityAggTests(TestCase):
    def test_default_precision(self):
        result = convert_cardinality_agg({"field": "user_id"})
        self.assertIn("max(3000)", result)

    def test_custom_precision_threshold(self):
        result = convert_cardinality_agg({"field": "user_id", "precision_threshold": 5000})
        self.assertIn("max(5000)", result)

    def test_precision_capped_at_10000(self):
        # Capped to avoid unbounded group enumeration in Vespa, which could be expensive.
        result = convert_cardinality_agg({"field": "user_id", "precision_threshold": 50000})
        self.assertIn("max(10000)", result)


class ConvertRangeAggTests(TestCase):
    def test_predefined_buckets(self):
        result = convert_range_agg({
            "field": "age",
            "ranges": [{"from": 0, "to": 10}, {"from": 10, "to": 20}]
        })
        self.assertIn("predefined(age, bucket(0, 10), bucket(10, 20))", result)

    def test_open_ended_ranges(self):
        # Missing "from" or "to" maps to -inf/inf respectively in Vespa predefined buckets.
        result = convert_range_agg({
            "field": "age",
            "ranges": [{"to": -10}, {"from": 2000}]
        })
        self.assertIn("bucket(-inf, -10)", result)
        self.assertIn("bucket(2000, inf)", result)

    def test_with_nested_metrics(self):
        result = convert_range_agg(
            {"field": "age", "ranges": [{"from": 0, "to": 10}]},
            "output(sum(x))"
        )
        self.assertIn("output(count()) output(sum(x))", result)

    def test_empty_ranges(self):
        # No ranges defined — falls back to a plain group-by instead of predefined().
        result = convert_range_agg({"field": "age", "ranges": []})
        self.assertEqual(result, "group(age) each(output(count()))")


class ConvertHistogramAggTests(TestCase):
    def test_basic_histogram(self):
        result = convert_histogram_agg({"field": "price", "interval": 100})
        self.assertIn("floor(price / 100)", result)

    def test_with_nested_content(self):
        result = convert_histogram_agg({"field": "price", "interval": 100}, "output(sum(x))")
        self.assertIn("output(count()) output(sum(x))", result)

    def test_default_interval(self):
        result = convert_histogram_agg({"field": "price"})
        self.assertIn("floor(price / 100)", result)


class ConvertAutoDateHistogramAggTests(TestCase):
    def test_default_buckets(self):
        result = convert_auto_date_histogram_agg({"field": "timestamp", "buckets": 10})
        self.assertIn("max(10)", result)
        self.assertIn("floor(timestamp / 3600000)", result)

    def test_custom_buckets(self):
        result = convert_auto_date_histogram_agg({"field": "timestamp", "buckets": 50})
        self.assertIn("max(50)", result)

    def test_field_name_mapped(self):
        result = convert_auto_date_histogram_agg({"field": "@timestamp"})
        self.assertIn("floor(timestamp / 3600000)", result)


class ConvertCompositeAggTests(TestCase):
    def test_single_terms_source(self):
        result = convert_composite_agg({
            "sources": [{"status_terms": {"terms": {"field": "status"}}}],
            "size": 10
        })
        self.assertEqual(result, "group(status) max(10) each(output(count()))")

    def test_terms_and_terms_source(self):
        result = convert_composite_agg({
            "sources": [
                {"s1": {"terms": {"field": "status"}}},
                {"s2": {"terms": {"field": "region"}}}
            ],
            "size": 10
        })
        self.assertIn("group(status) max(10)", result)
        self.assertIn("group(region) max(10)", result)

    def test_date_histogram_source(self):
        # Composite date_histogram sources use floor(field/interval_ms) to bucket timestamps,
        # rather than grouping by raw millisecond values.
        result = convert_composite_agg({
            "sources": [{"ts": {"date_histogram": {"field": "timestamp", "calendar_interval": "day"}}}],
            "size": 10
        })
        self.assertIn("floor(timestamp / 86400000)", result)

    def test_three_sources(self):
        result = convert_composite_agg({
            "sources": [
                {"s1": {"terms": {"field": "a"}}},
                {"s2": {"terms": {"field": "b"}}},
                {"s3": {"terms": {"field": "c"}}}
            ],
            "size": 5
        })
        self.assertIn("group(a) max(5)", result)
        self.assertIn("group(b) max(5)", result)
        self.assertIn("group(c) max(5)", result)

    def test_empty_sources(self):
        result = convert_composite_agg({"sources": []})
        self.assertEqual(result, "")

    def test_custom_size(self):
        result = convert_composite_agg({
            "sources": [{"s1": {"terms": {"field": "status"}}}],
            "size": 25
        })
        self.assertIn("max(25)", result)


class ConvertMultiTermsAggTests(TestCase):
    def test_two_fields(self):
        result = convert_multi_terms_agg({
            "terms": [{"field": "process.name"}, {"field": "cloud.region"}]
        })
        self.assertIn("group(process_name)", result)
        self.assertIn("group(cloud_region)", result)
        self.assertIn("max(10)", result)
        self.assertIn("output(count())", result)

    def test_with_custom_size(self):
        result = convert_multi_terms_agg({
            "terms": [{"field": "a"}, {"field": "b"}],
            "size": 20
        })
        self.assertIn("max(20)", result)

    def test_with_nested_content(self):
        result = convert_multi_terms_agg(
            {"terms": [{"field": "a"}, {"field": "b"}]},
            "output(sum(x))"
        )
        self.assertIn("output(count()) output(sum(x))", result)

    def test_empty_terms_list(self):
        result = convert_multi_terms_agg({"terms": []})
        self.assertEqual(result, "")

    def test_single_field(self):
        result = convert_multi_terms_agg({"terms": [{"field": "status"}]})
        self.assertIn("group(status)", result)
        self.assertIn("output(count())", result)


class ConvertMetricAggTests(TestCase):
    def test_sum(self):
        self.assertEqual(convert_metric_agg("sum", {"field": "x"}), "output(sum(x))")

    def test_avg(self):
        self.assertEqual(convert_metric_agg("avg", {"field": "x"}), "output(avg(x))")

    def test_min(self):
        self.assertEqual(convert_metric_agg("min", {"field": "x"}), "output(min(x))")

    def test_max(self):
        self.assertEqual(convert_metric_agg("max", {"field": "x"}), "output(max(x))")

    def test_stats(self):
        result = convert_metric_agg("stats", {"field": "f"})
        self.assertIn("output(sum(f))", result)
        self.assertIn("output(avg(f))", result)
        self.assertIn("output(min(f))", result)
        self.assertIn("output(max(f))", result)
        self.assertIn("output(count())", result)

    def test_value_count(self):
        # value_count maps to count() — the field is intentionally ignored since Vespa's
        # count() counts all docs in the group, not non-null values of a specific field.
        self.assertEqual(convert_metric_agg("value_count", {"field": "x"}), "output(count())")

    def test_field_name_mapped(self):
        result = convert_metric_agg("sum", {"field": "@timestamp"})
        self.assertEqual(result, "output(sum(timestamp))")


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
    @patch("osbenchmark.database.clients.vespa.helpers.time.sleep")
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
    @patch("osbenchmark.database.clients.vespa.helpers.time.sleep")
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
    @patch("osbenchmark.database.clients.vespa.helpers.time.sleep")
    def test_max_attempts_respected(self, mock_sleep, mock_get):
        mock_get.side_effect = ConnectionError("fail")

        client = MagicMock()
        client.endpoint = "http://localhost:8080"

        result = wait_for_vespa(client, max_attempts=2)
        self.assertFalse(result)
        self.assertEqual(mock_get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
