# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import logging
from typing import Any, Dict, List, Optional

from osbenchmark.workload_insights.models import CountStats, FieldStats


class Analyzer:
    """
    Base class of the per-field analyzers.

    An analyzer accumulates the statistics of one field incrementally: ``add()`` is called once per sampled document with the values of
    the field and ``result()`` returns the statistics. Memory does not grow with the number of documents (numeric fields keep their
    values for the percentiles), and analyzers of the same field that have seen different batches of documents can be combined with
    ``merge()``.

    Documents are counted for ``sample_size``, ``null_ratio`` and ``values_per_doc``; subclasses count values for the type specific
    statistics.
    """

    def __init__(self, field_type: str = "", definition: Optional[Dict[str, Any]] = None, capture_values: bool = False):
        """
        :param field_type: The mapping type of the field.
        :param definition: The mapping definition of the field, e.g. to read its format.
        :param capture_values: Whether real values from the samples may be copied into the result, e.g. the most frequent keywords.
                               Disabled by default because the result may be shared or committed.
        """
        self.field_type = field_type
        self.definition = definition if definition is not None else {}
        self.capture_values = capture_values
        self.logger = logging.getLogger(__name__)
        self._sample_size = 0
        # the documents in which the field has values, and the number of values in these documents
        self._docs_with_values = 0
        self._min_values = 0
        self._max_values = 0
        self._total_values = 0

    def add(self, values: List[Any]) -> None:
        """
        Adds one sampled document.

        :param values: The values of the field in the document. The list is empty if the field is missing, null or empty.
        """
        self._sample_size += 1
        if values:
            count = len(values)
            self._min_values = count if not self._docs_with_values else min(self._min_values, count)
            self._max_values = max(self._max_values, count)
            self._total_values += count
            self._docs_with_values += 1
            self._add_values(values)

    def merge(self, other: "Analyzer") -> None:
        """
        Adds the documents that another analyzer of the same type and field has seen to this analyzer.
        """
        if type(other) is not type(self) or other.field_type != self.field_type:  # pylint: disable=unidiomatic-typecheck
            raise TypeError("Cannot merge an analyzer of [%s] fields of type [%s] into one of type [%s]." %
                            (other.field_type, type(other).__name__, type(self).__name__))
        # pylint: disable=protected-access
        self._sample_size += other._sample_size
        if other._docs_with_values:
            self._min_values = other._min_values if not self._docs_with_values else min(self._min_values, other._min_values)
            self._max_values = max(self._max_values, other._max_values)
            self._total_values += other._total_values
            self._docs_with_values += other._docs_with_values
        self._merge_values(other)

    def result(self) -> FieldStats:
        """
        :return: The statistics of the field for the documents that have been added.
        """
        return FieldStats(type=self.field_type, **self._common_stats())

    def analyze(self, field_type: str, definition: Dict[str, Any], per_doc_values: List[List[Any]]) -> FieldStats:
        """
        Analyzes the values of all documents at once, independent of what has been added to this analyzer.

        :param field_type: The mapping type of the field.
        :param definition: The mapping definition of the field, e.g. to read its format.
        :param per_doc_values: One list of values per sampled document. The list is empty if the field is missing, null or empty.
        :return: The statistics of the field.
        """
        analyzer = type(self)(field_type, definition, self.capture_values)
        for values in per_doc_values:
            analyzer.add(values)
        return analyzer.result()

    def _add_values(self, values: List[Any]) -> None:
        """Hook for subclasses: accumulates the values of one document that has at least one value."""

    def _merge_values(self, other: "Analyzer") -> None:
        """Hook for subclasses: combines the type specific state of another analyzer."""

    def _common_stats(self) -> Dict[str, Any]:
        null_ratio = (self._sample_size - self._docs_with_values) / self._sample_size if self._sample_size else 0.0
        values_per_doc = None
        if self._max_values > 1:
            values_per_doc = CountStats(min=self._min_values, max=self._max_values, mean=self._total_values / self._docs_with_values)
        return {
            "sample_size": self._sample_size,
            "null_ratio": null_ratio,
            "values_per_doc": values_per_doc
        }
