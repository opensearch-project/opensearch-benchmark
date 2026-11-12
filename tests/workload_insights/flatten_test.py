# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import copy
import logging

import pytest

from osbenchmark.workload_insights.analyzers.flatten import (Flattener, _expand_dotted_names, log_skipped_fields,
                                                              mapping_properties)

PROPERTIES = {
    "id": {"type": "long"},
    "tags": {"type": "keyword"},
    "user": {"properties": {"name": {"type": "keyword"}, "age": {"type": "integer"}}},
    "comments": {"type": "nested", "properties": {"text": {"type": "text"}, "likes": {"type": "integer"}}},
    "embedding": {"type": "knn_vector", "dimension": 3},
    "sparse": {"type": "sparse_vector"},
    "location": {"type": "geo_point"},
    "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}},
}


class TestMappingProperties:
    def test_properties(self):
        assert mapping_properties({"properties": {"a": {"type": "long"}}}) == {"a": {"type": "long"}}

    def test_mappings(self):
        assert mapping_properties({"mappings": {"properties": {"a": {"type": "long"}}}}) == {"a": {"type": "long"}}

    def test_get_mapping_response(self):
        response = {"my-index": {"mappings": {"properties": {"a": {"type": "long"}}}}}

        assert mapping_properties(response) == {"a": {"type": "long"}}

    def test_get_mapping_response_with_multiple_indices_is_rejected(self):
        response = {"a": {"mappings": {"properties": {}}}, "b": {"mappings": {"properties": {}}}}

        with pytest.raises(ValueError):
            mapping_properties(response)

    def test_invalid_mapping_is_rejected(self):
        with pytest.raises(ValueError):
            mapping_properties({"something": "else"})


class TestFlattener:
    def test_field_paths_are_dotted_and_multi_fields_are_ignored(self):
        flattener = Flattener({"properties": PROPERTIES})

        assert sorted(flattener.fields) == [
            "comments.likes", "comments.text", "embedding", "id", "location", "sparse", "tags", "title", "user.age", "user.name"]

    def test_scalars_and_arrays_are_flattened_per_value(self):
        flattener = Flattener({"properties": PROPERTIES})

        values = flattener.flatten({"id": 1, "tags": ["a", "b", None], "user": {"name": "x"}})

        assert values == {"id": [1], "tags": ["a", "b"], "user.name": ["x"]}

    def test_nested_array_of_objects(self):
        flattener = Flattener({"properties": PROPERTIES})

        values = flattener.flatten({"comments": [{"text": "a", "likes": 1}, {"text": "b"}]})

        assert values == {"comments.text": ["a", "b"], "comments.likes": [1]}

    def test_vectors_and_geo_points_are_one_value(self):
        flattener = Flattener({"properties": PROPERTIES})

        values = flattener.flatten({"embedding": [0.1, 0.2, 0.3], "sparse": {"a": 0.5, "b": 0.1}, "location": [1.0, 2.0]})

        assert values == {"embedding": [[0.1, 0.2, 0.3]], "sparse": [{"a": 0.5, "b": 0.1}], "location": [[1.0, 2.0]]}

    @pytest.mark.parametrize("raw,expected", [
        ({"lat": 1, "lon": 2}, [{"lat": 1, "lon": 2}]),
        ("1,2", ["1,2"]),
        ("POINT (2 1)", ["POINT (2 1)"]),
        ([2.0, 1.0], [[2.0, 1.0]]),
        ([2, 1, 5], [[2, 1, 5]]),
        ([{"lat": 1, "lon": 2}, {"lat": 3, "lon": 4}], [{"lat": 1, "lon": 2}, {"lat": 3, "lon": 4}]),
        (["1,2", "3,4"], ["1,2", "3,4"]),
        ([[2.0, 1.0], [4.0, 3.0]], [[2.0, 1.0], [4.0, 3.0]]),
        ([{"lat": 1, "lon": 2}, None, "3,4"], [{"lat": 1, "lon": 2}, "3,4"]),
        ([], []),
    ])
    def test_geo_points_can_have_several_values(self, raw, expected):
        flattener = Flattener({"properties": {"loc": {"type": "geo_point"}}})

        assert flattener.flatten({"loc": raw}).get("loc", []) == expected

    def test_vectors_stay_one_value_even_if_they_look_like_a_list_of_lists(self):
        flattener = Flattener({"properties": PROPERTIES})

        assert flattener.flatten({"embedding": [[1, 2], [3, 4]]}) == {"embedding": [[[1, 2], [3, 4]]]}

    def test_missing_and_null_fields_have_no_values(self):
        flattener = Flattener({"properties": PROPERTIES})

        assert flattener.flatten({"id": None, "tags": []}) == {}

    def test_fields_not_in_the_mapping_are_skipped_and_collected(self):
        flattener = Flattener({"properties": PROPERTIES})

        values = flattener.flatten({"id": 1, "dynamic": "x", "user": {"name": "x", "nickname": "y"}})
        flattener.flatten({"dynamic": "z", "other": 1})

        assert values == {"id": [1], "user.name": ["x"]}
        assert flattener.skipped_paths == {"dynamic", "user.nickname", "other"}

    def test_dotted_keys_in_the_source_are_resolved(self):
        flattener = Flattener({"properties": PROPERTIES})

        values = flattener.flatten({"user.name": "x", "user.age": 3, "comments.likes": 5})

        assert values == {"user.name": ["x"], "user.age": [3], "comments.likes": [5]}
        assert not flattener.skipped_paths

    def test_dotted_key_for_an_object_with_nested_values(self):
        mapping = {"properties": {"a": {"properties": {"b": {"properties": {"c": {"type": "long"}}}}}}}

        values = Flattener(mapping).flatten({"a.b": {"c": 1}, "a": {"b.c": 2}})

        assert values == {"a.b.c": [1, 2]}

    def test_unknown_dotted_keys_are_skipped(self):
        flattener = Flattener({"properties": PROPERTIES})

        flattener.flatten({"user.nickname": "x", "id.foo": 1, "unknown.name": "y", "tags.x": "z"})

        assert flattener.skipped_paths == {"user.nickname", "id.foo", "unknown.name", "tags.x"}

    def test_dotted_names_in_the_mapping_are_expanded(self):
        mapping = {"properties": {"user.name": {"type": "keyword"}, "user.address.city": {"type": "keyword"}, "id": {"type": "long"}}}
        flattener = Flattener(mapping)

        assert sorted(flattener.fields) == ["id", "user.address.city", "user.name"]
        assert flattener.flatten({"user": {"name": "x", "address": {"city": "c"}}, "user.address.city": "d"}) == \
            {"user.name": ["x"], "user.address.city": ["c", "d"]}

    def test_dotted_and_nested_definitions_of_the_same_object_are_merged(self):
        mapping = {"properties": {"user.name": {"type": "keyword"}, "user": {"properties": {"age": {"type": "integer"}}}}}
        reversed_mapping = {"properties": {"user": {"properties": {"age": {"type": "integer"}}}, "user.name": {"type": "keyword"}}}

        assert sorted(Flattener(mapping).fields) == ["user.age", "user.name"]
        assert sorted(Flattener(reversed_mapping).fields) == ["user.age", "user.name"]

    @pytest.mark.parametrize("names", [["user", "user.name"], ["user.name", "user"]])
    def test_explicit_definition_keeps_its_attributes_when_merged_with_dotted_names(self, names):
        definitions = {
            "user": {"type": "nested", "dynamic": "strict", "properties": {"age": {"type": "integer"}}},
            "user.name": {"type": "keyword"},
        }

        expanded = _expand_dotted_names({name: definitions[name] for name in names})

        assert expanded["user"]["type"] == "nested"
        assert expanded["user"]["dynamic"] == "strict"
        assert expanded["user"]["properties"] == {"age": {"type": "integer"}, "name": {"type": "keyword"}}

    def test_dotted_names_below_an_explicit_object_without_properties(self):
        expanded = _expand_dotted_names({"user": {"type": "object", "enabled": True}, "user.name": {"type": "keyword"}})

        assert expanded == {"user": {"type": "object", "enabled": True, "properties": {"name": {"type": "keyword"}}}}

    def test_nested_objects_defined_with_dotted_names_stay_nested_containers(self):
        mapping = {"properties": {"comments.text": {"type": "text"}, "comments": {"type": "nested", "properties": {"likes": {"type": "long"}}}}}

        flattener = Flattener(mapping)

        assert sorted(flattener.fields) == ["comments.likes", "comments.text"]
        assert flattener.flatten({"comments": [{"likes": 1, "text": "a"}, {"likes": 2}]}) == {"comments.likes": [1, 2], "comments.text": ["a"]}

    def test_the_mapping_is_not_modified(self):
        mapping = {"properties": {
            "user": {"type": "nested", "properties": {"age": {"type": "integer"}}},
            "user.name": {"type": "keyword"},
            "a.b.c": {"type": "long"},
            "a": {"properties": {"d": {"type": "long"}}},
        }}
        original = copy.deepcopy(mapping)

        first = Flattener(mapping)
        second = Flattener(mapping)

        assert mapping == original
        assert sorted(first.fields) == sorted(second.fields) == ["a.b.c", "a.d", "user.age", "user.name"]

    def test_log_skipped_fields_logs_the_count(self, caplog):
        with caplog.at_level(logging.INFO):
            log_skipped_fields(logging.getLogger("test"), {"a", "b"})

        assert "Skipped [2] fields" in caplog.text
