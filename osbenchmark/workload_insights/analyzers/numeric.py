# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import math
from array import array
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
    """
    Analyzes long, integer, short, byte, unsigned_long, double, float, half_float and scaled_float fields.

    Min and max are kept as the original numbers, because a float64 cannot hold every long exactly. Mean and stddev are accumulated with
    Welford's algorithm (count, mean and the sum of squared differences from the mean) and combined with Chan's formula, which do not
    lose precision for large values like a sum of squares does. They are computed in float64 and are therefore approximate for integers
    beyond 2^53. The exact percentiles need all values, so these are kept as float64 (8 bytes each).
    """

    def __init__(self, field_type: str = "", definition: Optional[Dict[str, Any]] = None, capture_values: bool = False):
        super().__init__(field_type, definition, capture_values)
        self._integer = field_type in INTEGER_TYPES
        # like OpenSearch, coerce fractions of integer fields by truncating them unless the mapping disables coercion
        self._coerce = str(self.definition.get("coerce", True)).lower() != "false"
        self._count = 0
        self._mean = 0.0
        self._sum_squared_differences = 0.0
        self._min: Optional[Union[int, float]] = None
        self._max: Optional[Union[int, float]] = None
        self._floats = array("d")

    def _add_values(self, values: List[Any]) -> None:
        for value in values:
            number = self._to_number(value, self._integer, self._coerce)
            if number is not None and self._integer and not INTEGER_RANGES[self.field_type][0] <= number <= INTEGER_RANGES[self.field_type][1]:
                number = None
            if number is None:
                self.logger.debug("Ignoring non-numeric, non-finite or out-of-range value of type [%s] in numeric field.", type(value).__name__)
                continue
            self._min = number if self._min is None else min(self._min, number)
            self._max = number if self._max is None else max(self._max, number)
            as_float = float(number)
            self._floats.append(as_float)
            # Welford's algorithm
            self._count += 1
            delta = as_float - self._mean
            self._mean += delta / self._count
            self._sum_squared_differences += delta * (as_float - self._mean)

    def _merge_values(self, other: "NumericAnalyzer") -> None:  # pylint: disable=arguments-renamed
        # pylint: disable=protected-access
        if not other._count:
            return
        if not self._count:
            self._min, self._max = other._min, other._max
            self._count, self._mean = other._count, other._mean
            self._sum_squared_differences = other._sum_squared_differences
        else:
            self._min, self._max = min(self._min, other._min), max(self._max, other._max)
            # Chan's formula
            count = self._count + other._count
            delta = other._mean - self._mean
            self._sum_squared_differences += other._sum_squared_differences + delta * delta * self._count * other._count / count
            self._mean += delta * other._count / count
            self._count = count
        self._floats.extend(other._floats)

    def result(self) -> FieldStats:
        stats = {}
        if self._count:
            stats = {
                "min": self._min,
                "max": self._max,
                "mean": self._mean,
                "stddev": math.sqrt(self._sum_squared_differences / self._count),
                "percentiles": {"%g" % p: float(v) for p, v in zip(PERCENTILES, np.percentile(np.frombuffer(self._floats), PERCENTILES))}
            }
        return NumericStats(type=self.field_type, **self._common_stats(), **stats)

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
        exact = NumericAnalyzer._to_int(value)
        if exact is not None:
            return exact
        number = NumericAnalyzer._to_finite_float(value)
        if number is None:
            return None
        return int(number) if number.is_integer() or coerce else None

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        """
        :return: The value if it is an int or a string of an int, unless it is too large for the float64 that the statistics are
                 computed with: converting it raises an OverflowError.
        """
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                return None
        if isinstance(value, int) and NumericAnalyzer._to_finite_float(value) is not None:
            return value
        return None

    @staticmethod
    def _to_finite_float(value: Any) -> Optional[float]:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        # NaN and infinity can still be in _source, e.g. of documents indexed with ignore_malformed
        return number if math.isfinite(number) else None
