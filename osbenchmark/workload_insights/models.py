# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

"""
Output models of the workload insights analyzers.

An analysis result is keyed by field path. Every field carries the common statistics of :class:`FieldStats`; fields of a supported
mapping type carry additional, type specific statistics.
"""

from typing import Annotated, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    # fail early on schema mistakes instead of silently dropping or accepting unknown statistics
    model_config = ConfigDict(extra="forbid")


class CountStats(_Model):
    """Minimum, maximum and mean of a count or a length."""
    min: float
    max: float
    mean: float


class ValueCount(_Model):
    """A real value that occurs in the samples and how often it occurs."""
    value: str
    count: int


class FieldStats(_Model):
    """
    Statistics that every field has, also fields of an unsupported mapping type.

    :param type: The mapping type of the field.
    :param sample_size: The number of sampled documents. Counted per document.
    :param null_ratio: The ratio of sampled documents in which the field is missing, null or empty. Counted per document.
    :param values_per_doc: The number of values per document. Only set if at least one document has more than one value for the field,
                           i.e. an array. Only documents that have the field are considered.
    """
    type: str
    sample_size: int
    null_ratio: float
    values_per_doc: Optional[CountStats] = None


class NumericStats(FieldStats):
    """
    Statistics of numeric fields. Counted per value, i.e. every element of an array counts.

    min and max are integers for integer types so that values above 2^53 stay exact; the other statistics are floats.
    """
    type: Literal["long", "integer", "short", "byte", "unsigned_long", "double", "float", "half_float", "scaled_float"]
    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    percentiles: Dict[str, float] = {}


class KeywordStats(FieldStats):
    """
    Statistics of keyword fields. Counted per value.

    :param top_values: The most frequent values. Copies real values from the samples, so it is empty unless capturing values has been
                       requested.
    :param length: The distribution of the length of the values.
    """
    type: Literal["keyword"]
    cardinality: int = 0
    top_values: List[ValueCount] = []
    length: Optional[CountStats] = None


class DateStats(FieldStats):
    """
    Statistics of date fields. Counted per value.

    :param min: The earliest value as ISO 8601 timestamp in UTC.
    :param max: The latest value as ISO 8601 timestamp in UTC.
    :param format: The format of the values: the format of the mapping if it defines one, otherwise inferred from the values.
    :param granularity: The finest unit in which the values differ from midnight: one of millisecond (any sub-second precision),
                        second, minute, hour or day.
    """
    type: Literal["date", "date_nanos"]
    min: Optional[str] = None
    max: Optional[str] = None
    format: Optional[str] = None
    granularity: Optional[str] = None


# Validation picks the most specific model first and falls back to the basic FieldStats, so that a serialized result can be loaded
# back. A new type specific model has to be added here, before FieldStats.
AnyFieldStats = Annotated[Union[NumericStats, KeywordStats, DateStats, FieldStats], Field(union_mode="left_to_right")]


class AnalyzerResult(_Model):
    """The statistics of all analyzed fields, keyed by field path."""
    fields: Dict[str, AnyFieldStats]
