# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from collections import Counter
from typing import Any, Dict, List

from osbenchmark.workload_insights.analyzers.base import Analyzer
from osbenchmark.workload_insights.models import CountStats, FieldStats, KeywordStats, ValueCount

TOP_N = 10


class KeywordAnalyzer(Analyzer):
    """Analyzes keyword fields."""

    def analyze(self, field_type: str, definition: Dict[str, Any], per_doc_values: List[List[Any]]) -> FieldStats:
        counter = Counter(self._to_keyword(value) for values in per_doc_values for value in values)
        top_values = []
        if self.capture_values:
            top_values = [ValueCount(value=value, count=count) for value, count in counter.most_common(TOP_N)]
        return KeywordStats(
            type=field_type,
            **self.common_stats(per_doc_values),
            cardinality=len(counter),
            top_values=top_values,
            length=self._length_stats(counter)
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
