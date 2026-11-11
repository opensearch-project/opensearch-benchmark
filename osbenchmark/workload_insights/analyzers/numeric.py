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
INTEGER_TYPES = frozenset(["long", "integer", "short", "byte", "unsigned_long"])


class NumericAnalyzer(Analyzer):
    """Analyzes long, integer, short, byte, unsigned_long, double, float, half_float and scaled_float fields."""

    def analyze(self, field_type: str, definition: Dict[str, Any], per_doc_values: List[List[Any]]) -> FieldStats:
        integer = field_type in INTEGER_TYPES
        numbers = []
        for values in per_doc_values:
            for value in values:
                number = self._to_number(value, integer)
                if number is None:
                    self.logger.debug("Ignoring non-numeric or non-finite value of type [%s] in numeric field.", type(value).__name__)
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
    def _to_number(value: Any, integer: bool) -> Optional[Union[int, float]]:
        """
        :return: The value as int (for integer types, if it is one) or float, or None if it is not a finite number.
        """
        # numeric fields accept numbers as strings in _source; booleans are not numbers
        if isinstance(value, bool):
            return None
        if integer:
            if isinstance(value, int):
                return value
            try:
                return int(value)
            except (TypeError, ValueError):
                pass
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        # NaN and infinity can still be in _source, e.g. of documents indexed with ignore_malformed
        return number if math.isfinite(number) else None
