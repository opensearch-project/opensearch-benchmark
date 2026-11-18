# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from collections import Counter
from typing import Any, Dict, List, Optional

from osbenchmark.workload_insights.analyzers.base import Analyzer
from osbenchmark.workload_insights.models import CountStats, FieldStats, KeywordStats, ValueCount

TOP_N = 10


class KeywordAnalyzer(Analyzer):
    """Analyzes keyword fields."""

    def __init__(self, field_type: str = "", definition: Optional[Dict[str, Any]] = None, capture_values: bool = False):
        super().__init__(field_type, definition, capture_values)
        self._counter: Counter = Counter()

    def _add_values(self, values: List[Any]) -> None:
        self._counter.update(self._to_keyword(value) for value in values)

    def _merge_values(self, other: "KeywordAnalyzer") -> None:  # pylint: disable=arguments-renamed
        self._counter.update(other._counter)  # pylint: disable=protected-access

    def result(self) -> FieldStats:
        top_values = []
        if self.capture_values:
            top_values = [ValueCount(value=value, count=count) for value, count in self._counter.most_common(TOP_N)]
        return KeywordStats(
            type=self.field_type,
            **self._common_stats(),
            cardinality=len(self._counter),
            top_values=top_values,
            length=self._length_stats(self._counter)
        )

    @staticmethod
    def _to_keyword(value: Any) -> str:
        # OpenSearch indexes a JSON boolean as "true" or "false", not as the Python representation
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    @staticmethod
    def _length_stats(counter: Counter):
        if not counter:
            return None
        total = sum(counter.values())
        lengths = [len(value) for value in counter]
        return CountStats(min=min(lengths), max=max(lengths),
                          mean=sum(len(value) * count for value, count in counter.items()) / total)
