# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import pytest
from pydantic import ValidationError

from osbenchmark.workload_insights.models import (AnalyzerResult, DateStats, FieldStats, KeywordStats, NumericStats,
                                                  ValueCount)


def test_unknown_statistics_are_rejected():
    with pytest.raises(ValidationError):
        FieldStats(type="text", sample_size=1, null_ratio=0.0, unknown=1)


def test_numeric_stats_only_accept_numeric_types():
    with pytest.raises(ValidationError):
        NumericStats(type="keyword", sample_size=1, null_ratio=0.0)


def test_result_serializes_type_specific_statistics():
    result = AnalyzerResult(fields={
        "count": NumericStats(type="long", sample_size=2, null_ratio=0.0, min=1, max=2),
        "name": KeywordStats(type="keyword", sample_size=2, null_ratio=0.5, cardinality=1, top_values=[ValueCount(value="a", count=1)]),
        "other": FieldStats(type="text", sample_size=2, null_ratio=0.0),
    })

    dumped = result.model_dump()

    assert dumped["fields"]["count"]["max"] == 2
    assert dumped["fields"]["name"]["top_values"] == [{"value": "a", "count": 1}]
    assert dumped["fields"]["other"] == {"type": "text", "sample_size": 2, "null_ratio": 0.0, "values_per_doc": None}


def _result():
    return AnalyzerResult(fields={
        "count": NumericStats(type="long", sample_size=2, null_ratio=0.0, min=2 ** 62, max=2 ** 62 + 1, mean=1.5, percentiles={"50": 1.0}),
        "name": KeywordStats(type="keyword", sample_size=2, null_ratio=0.5, cardinality=1, top_values=[ValueCount(value="a", count=1)]),
        "created": DateStats(type="date_nanos", sample_size=2, null_ratio=0.0, min="2024-01-01T00:00:00+00:00", granularity="day"),
        "other": FieldStats(type="text", sample_size=2, null_ratio=0.0),
    })


def test_result_round_trips_through_a_dict():
    result = _result()

    loaded = AnalyzerResult.model_validate(result.model_dump())

    assert loaded == result
    assert [type(stats) for stats in loaded.fields.values()] == [NumericStats, KeywordStats, DateStats, FieldStats]


def test_result_round_trips_through_json():
    result = _result()

    loaded = AnalyzerResult.model_validate_json(result.model_dump_json())

    assert loaded == result
    assert loaded.fields["count"].max == 2 ** 62 + 1


def test_numeric_stats_keep_integers_exact():
    stats = NumericStats(type="long", sample_size=1, null_ratio=0.0, min=9007199254740993)

    assert stats.min == 9007199254740993 and isinstance(stats.min, int)
