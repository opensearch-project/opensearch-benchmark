# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import math
from typing import Any, Dict, List, Optional, Union

import numpy as np

from osbenchmark.workload_insights.analyzers.base import Analyzer
from osbenchmark.workload_insights.models import FieldStats, NumericStats

PERCENTILES = (50, 90, 99, 99.9)
# the range of the values that OpenSearch can store in integer fields. Other values are invalid, e.g. in documents that were indexed
# with ignore_malformed.
INTEGER_RANGES = {
    "byte": (-2 ** 7, 2 ** 7 - 1),
    "short": (-2 ** 15, 2 ** 15 - 1),
    "integer": (-2 ** 31, 2 ** 31 - 1),
    "long": (-2 ** 63, 2 ** 63 - 1),
    "unsigned_long": (0, 2 ** 64 - 1),
}
INTEGER_TYPES = frozenset(INTEGER_RANGES)


class NumericAnalyzer(Analyzer):
    """Analyzes long, integer, short, byte, unsigned_long, double, float, half_float and scaled_float fields."""

    def analyze(self, field_type: str, definition: Dict[str, Any], per_doc_values: List[List[Any]]) -> FieldStats:
        integer = field_type in INTEGER_TYPES
        # like OpenSearch, coerce fractions of integer fields by truncating them unless the mapping disables coercion
        coerce = str(definition.get("coerce", True)).lower() != "false"
        numbers = []
        for values in per_doc_values:
            for value in values:
                number = self._to_number(value, integer, coerce)
                if number is not None and integer and not INTEGER_RANGES[field_type][0] <= number <= INTEGER_RANGES[field_type][1]:
                    number = None
                if number is None:
                    self.logger.debug("Ignoring non-numeric, non-finite or out-of-range value of type [%s] in numeric field.", type(value).__name__)
                else:
                    numbers.append(number)
        stats = {}
        if numbers:
            # min and max are taken from the original numbers: a float64 cannot hold every long exactly
            array = np.array([float(number) for number in numbers], dtype=float)
            stats = {
                "min": min(numbers),
                "max": max(numbers),
                "mean": float(array.mean()),
                "stddev": float(array.std()),
                "percentiles": {"%g" % p: float(v) for p, v in zip(PERCENTILES, np.percentile(array, PERCENTILES))}
            }
        return NumericStats(type=field_type, **self.common_stats(per_doc_values), **stats)

    @staticmethod
    def _to_number(value: Any, integer: bool, coerce: bool = True) -> Optional[Union[int, float]]:
        """
        :param integer: Whether the field is of an integer type.
        :param coerce: Whether fractions of integer fields are truncated (the OpenSearch default) or the value is invalid.
        :return: The value as int (for integer types) or float, or None if it is not a finite number.
        """
        # numeric fields accept numbers as strings in _source; booleans are not numbers
        if isinstance(value, bool):
            return None
        if not integer:
            return NumericAnalyzer._to_finite_float(value)
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                pass
        number = NumericAnalyzer._to_finite_float(value)
        if number is None:
            return None
        return int(number) if number.is_integer() or coerce else None

    @staticmethod
    def _to_finite_float(value: Any) -> Optional[float]:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        # NaN and infinity can still be in _source, e.g. of documents indexed with ignore_malformed
        return number if math.isfinite(number) else None
