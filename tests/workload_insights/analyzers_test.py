# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import pytest

from osbenchmark.workload_insights.analyzers import ANALYZERS, analyze
from osbenchmark.workload_insights.analyzers.base import Analyzer
from osbenchmark.workload_insights.analyzers.date import DateAnalyzer
from osbenchmark.workload_insights.analyzers.keyword import KeywordAnalyzer
from osbenchmark.workload_insights.analyzers.numeric import NumericAnalyzer


class TestBaseAnalyzer:
    def test_counts_per_document(self):
        stats = Analyzer().analyze("text", {}, [["a"], [], ["b", "c"], []])

        assert stats.type == "text"
        assert stats.sample_size == 4
        assert stats.null_ratio == 0.5
        # documents that have the field: one with a single value, one with an array of two
        assert stats.values_per_doc.min == 1 and stats.values_per_doc.max == 2 and stats.values_per_doc.mean == 1.5

    def test_values_per_doc_is_only_set_for_arrays(self):
        assert Analyzer().analyze("text", {}, [["a"], ["b"]]).values_per_doc is None

    def test_without_samples(self):
        stats = Analyzer().analyze("text", {}, [])

        assert stats.sample_size == 0
        assert stats.null_ratio == 0.0

    def test_all_values_missing(self):
        assert Analyzer().analyze("text", {}, [[], []]).null_ratio == 1.0


class TestNumericAnalyzer:
    def test_statistics_are_per_value(self):
        stats = NumericAnalyzer().analyze("long", {}, [[1], [2, 3], [4]])

        assert (stats.min, stats.max, stats.mean) == (1.0, 4.0, 2.5)
        assert stats.stddev == pytest.approx(1.118034, rel=1e-5)
        assert stats.percentiles == {"50": 2.5, "90": pytest.approx(3.7), "99": pytest.approx(3.97), "99.9": pytest.approx(3.997)}
        assert stats.values_per_doc.max == 2

    def test_numbers_as_strings_are_accepted_and_invalid_values_ignored(self):
        stats = NumericAnalyzer().analyze("double", {}, [["1.5"], [True], ["x"], [2]])

        assert (stats.min, stats.max, stats.mean) == (1.5, 2.0, 1.75)
        assert stats.sample_size == 4

    @pytest.mark.parametrize("bad", ["NaN", "nan", "inf", "-Infinity", float("nan"), float("inf")])
    def test_non_finite_values_are_ignored(self, bad):
        stats = NumericAnalyzer().analyze("double", {}, [[1], [bad], [3]])

        assert (stats.min, stats.max, stats.mean) == (1.0, 3.0, 2.0)
        assert stats.stddev == 1.0
        assert stats.sample_size == 3

    def test_only_non_finite_values(self):
        stats = NumericAnalyzer().analyze("double", {}, [["NaN"]])

        assert stats.min is None and stats.percentiles == {}

    @pytest.mark.parametrize("field_type", ["long", "unsigned_long"])
    def test_integers_above_2_pow_53_stay_exact(self, field_type):
        stats = NumericAnalyzer().analyze(field_type, {}, [[9007199254740993], [9007199254740995]])

        assert stats.min == 9007199254740993 and stats.max == 9007199254740995
        assert isinstance(stats.min, int)

    def test_long_bounds_stay_exact(self):
        stats = NumericAnalyzer().analyze("long", {}, [[-2 ** 63], [2 ** 63 - 1]])

        assert (stats.min, stats.max) == (-2 ** 63, 2 ** 63 - 1)

    def test_unsigned_long_above_long_max(self):
        stats = NumericAnalyzer().analyze("unsigned_long", {}, [[2 ** 64 - 1, "18446744073709551614"]])

        assert (stats.min, stats.max) == (18446744073709551614, 2 ** 64 - 1)

    def test_integer_strings_stay_exact_and_float_types_stay_float(self):
        assert NumericAnalyzer().analyze("long", {}, [["9007199254740993"]]).max == 9007199254740993
        stats = NumericAnalyzer().analyze("double", {}, [[1], [2]])
        assert stats.max == 2.0 and isinstance(stats.max, float)

    def test_no_values(self):
        stats = NumericAnalyzer().analyze("integer", {}, [[], []])

        assert stats.min is None and stats.percentiles == {}
        assert stats.null_ratio == 1.0


class TestKeywordAnalyzer:
    VALUES = [["a", "b"], ["a"], ["ccc"], []]

    def test_values_are_not_captured_by_default(self):
        stats = KeywordAnalyzer().analyze("keyword", {}, self.VALUES)

        assert stats.cardinality == 3
        assert stats.top_values == []
        assert (stats.length.min, stats.length.max, stats.length.mean) == (1.0, 3.0, 1.5)

    def test_top_values_are_captured_on_request(self):
        stats = KeywordAnalyzer(capture_values=True).analyze("keyword", {}, self.VALUES)

        assert [(v.value, v.count) for v in stats.top_values] == [("a", 2), ("b", 1), ("ccc", 1)]

    def test_booleans_are_counted_as_opensearch_indexes_them(self):
        stats = KeywordAnalyzer(capture_values=True).analyze("keyword", {}, [[True], ["true"], [False], ["false"], ["False"]])

        assert stats.cardinality == 3
        assert {v.value: v.count for v in stats.top_values} == {"true": 2, "false": 2, "False": 1}

    def test_numbers_are_counted_like_their_string_form(self):
        stats = KeywordAnalyzer(capture_values=True).analyze("keyword", {}, [[1], ["1"], [1.5]])

        assert stats.cardinality == 2
        assert stats.top_values[0].value == "1" and stats.top_values[0].count == 2

    def test_top_values_are_limited(self):
        stats = KeywordAnalyzer(capture_values=True).analyze("keyword", {}, [[str(i)] for i in range(50)])

        assert stats.cardinality == 50
        assert len(stats.top_values) == 10

    def test_no_values(self):
        stats = KeywordAnalyzer().analyze("keyword", {}, [[]])

        assert stats.cardinality == 0 and stats.length is None


class TestDateAnalyzer:
    def test_iso_strings(self):
        stats = DateAnalyzer().analyze("date", {}, [["2024-03-01T10:20:30Z"], ["2024-01-01T00:00:00+00:00"], ["2024-02-01"]])

        assert stats.min == "2024-01-01T00:00:00+00:00"
        assert stats.max == "2024-03-01T10:20:30+00:00"
        assert stats.format == "strict_date_optional_time"
        assert stats.granularity == "second"

    def test_epoch_millis(self):
        stats = DateAnalyzer().analyze("date", {}, [[1700000040000], [1700000100000]])

        assert stats.format == "epoch_millis"
        assert stats.granularity == "minute"

    def test_epoch_second_format_of_the_mapping(self):
        stats = DateAnalyzer().analyze("date", {"format": "epoch_second"}, [[1609459200]])

        assert stats.min == stats.max == "2021-01-01T00:00:00+00:00"
        assert stats.format == "epoch_second"
        assert stats.granularity == "day"

    def test_first_epoch_format_of_the_mapping_applies_to_numbers(self):
        analyzer = DateAnalyzer()

        assert analyzer.analyze("date", {"format": "epoch_second||epoch_millis"}, [[1609459200]]).min == "2021-01-01T00:00:00+00:00"
        assert analyzer.analyze("date", {"format": "strict_date_optional_time||epoch_millis"}, [[1609459200000]]).min == \
            "2021-01-01T00:00:00+00:00"

    @pytest.mark.parametrize("value,expected", [
        ("2021-01-01T00:00:00.12Z", "2021-01-01T00:00:00.120000+00:00"),
        ("2021-01-01T00:00:00.1Z", "2021-01-01T00:00:00.100000+00:00"),
        ("2021-01-01T00:00:00.123456789Z", "2021-01-01T00:00:00.123456+00:00"),
        ("2021-01-01T00:00:00,5Z", "2021-01-01T00:00:00.500000+00:00"),
        ("2021-01-01T00:00:00+0000", "2021-01-01T00:00:00+00:00"),
        ("2021-01-01T05:30:00+05:30", "2021-01-01T00:00:00+00:00"),
        ("2021-01-01T00:00:00-0130", "2021-01-01T01:30:00+00:00"),
        ("2021-01-01T10:15", "2021-01-01T10:15:00+00:00"),
        ("2021-01-01T10", "2021-01-01T10:00:00+00:00"),
        ("2021-03", "2021-03-01T00:00:00+00:00"),
        ("2021", "2021-01-01T00:00:00+00:00"),
        ("2021-01-01 07:08:09", "2021-01-01T07:08:09+00:00"),
    ])
    def test_iso_8601_variants(self, value, expected):
        assert DateAnalyzer._parse(value)[0].isoformat() == expected  # pylint: disable=protected-access

    @pytest.mark.parametrize("value", ["2021-13-01", "2021-02-30", "2021-01-01T25:00:00Z", "20210101T000000Z", "yesterday", "",
                                       "2021-01-01T00:00:00+99:99"])
    def test_invalid_iso_8601_values(self, value):
        assert DateAnalyzer._parse(value) is None  # pylint: disable=protected-access

    def test_mapping_format_wins_and_mixed_formats_are_not_inferred(self):
        assert DateAnalyzer().analyze("date", {"format": "yyyy-MM-dd"}, [["2024-01-01"]]).format == "yyyy-MM-dd"
        assert DateAnalyzer().analyze("date", {}, [["2024-01-01"], [1700000000000]]).format is None

    @pytest.mark.parametrize("values,granularity", [
        (["2024-01-01", "2024-01-05"], "day"),
        (["2024-01-01T05:00:00Z"], "hour"),
        (["2024-01-01T05:07:00Z"], "minute"),
        (["2024-01-01T05:07:09Z"], "second"),
        (["2024-01-01T05:07:09.250Z"], "millisecond"),
    ])
    def test_granularity(self, values, granularity):
        assert DateAnalyzer().analyze("date", {}, [[v] for v in values]).granularity == granularity

    def test_unparseable_values_are_ignored(self):
        stats = DateAnalyzer().analyze("date", {}, [["not a date"], [True], ["2024-01-01"]])

        assert stats.min == stats.max == "2024-01-01T00:00:00+00:00"
        assert stats.sample_size == 3

    def test_no_values(self):
        stats = DateAnalyzer().analyze("date", {}, [[]])

        assert stats.min is None and stats.granularity is None


class TestAnalyze:
    MAPPING = {"mappings": {"properties": {
        "count": {"type": "long"},
        "name": {"type": "keyword"},
        "created": {"type": "date", "format": "strict_date_optional_time"},
        "notes": {"type": "text"},
        "embedding": {"type": "knn_vector", "dimension": 2},
    }}}

    def test_registry_covers_the_supported_types(self):
        assert set(ANALYZERS) == {"long", "integer", "short", "byte", "unsigned_long", "double", "float", "half_float", "scaled_float",
                                  "keyword", "date", "date_nanos"}

    @pytest.mark.parametrize("field_type", ["half_float", "scaled_float", "unsigned_long"])
    def test_all_numeric_types_get_numeric_statistics(self, field_type):
        mapping = {"properties": {"f": {"type": field_type}}}

        first = 1 if field_type == "unsigned_long" else 1.5

        stats = analyze(mapping, [{"f": first}, {"f": 2}]).fields["f"]

        assert stats.type == field_type
        assert (stats.min, stats.max) == (first, 2)
        assert stats.percentiles

    def test_date_nanos_gets_date_statistics(self):
        mapping = {"properties": {"f": {"type": "date_nanos"}}}

        stats = analyze(mapping, [{"f": "2015-01-01T12:10:30.123456789Z"}]).fields["f"]

        assert stats.type == "date_nanos"
        assert stats.min == "2015-01-01T12:10:30.123456+00:00"
        assert stats.granularity == "millisecond"

    def test_analyzes_every_mapped_field(self):
        docs = [
            {"count": 1, "name": "a", "created": "2024-01-01T00:00:00Z", "notes": "hello", "embedding": [0.1, 0.2], "dynamic": 1},
            {"count": 3, "name": "b", "created": "2024-01-02T00:00:00Z"},
        ]

        result = analyze(self.MAPPING, docs)

        assert sorted(result.fields) == ["count", "created", "embedding", "name", "notes"]
        assert result.fields["count"].mean == 2.0
        assert result.fields["name"].cardinality == 2
        assert result.fields["created"].granularity == "day"
        # types without an analyzer (yet) only get the basic statistics
        assert result.fields["notes"].type == "text" and result.fields["notes"].null_ratio == 0.5
        assert result.fields["embedding"].null_ratio == 0.5

    def test_real_values_are_only_captured_on_request(self):
        docs = [{"name": "secret"}]

        assert analyze(self.MAPPING, docs).fields["name"].top_values == []
        assert [v.value for v in analyze(self.MAPPING, docs, capture_values=True).fields["name"].top_values] == ["secret"]

    def test_samples_can_be_a_generator(self):
        result = analyze(self.MAPPING, ({"count": i} for i in range(4)))

        assert result.fields["count"].sample_size == 4

    def test_without_samples(self):
        result = analyze(self.MAPPING, [])

        assert all(stats.sample_size == 0 for stats in result.fields.values())

    def test_skipped_fields_are_logged(self, caplog):
        with caplog.at_level("INFO"):
            analyze(self.MAPPING, [{"count": 1, "x": 1, "y": 2}])

        assert "Skipped [2] fields" in caplog.text
