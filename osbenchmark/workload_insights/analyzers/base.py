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

    An analyzer receives the values of one field for every sampled document and returns the statistics of the field. Documents are
    counted for ``sample_size``, ``null_ratio`` and ``values_per_doc``; subclasses count values for the type specific statistics.
    """

    def __init__(self, capture_values: bool = False):
        """
        :param capture_values: Whether real values from the samples may be copied into the result, e.g. the most frequent keywords.
                               Disabled by default because the result may be shared or committed.
        """
        self.capture_values = capture_values
        self.logger = logging.getLogger(__name__)

    def analyze(self, field_type: str, definition: Dict[str, Any], per_doc_values: List[List[Any]]) -> FieldStats:
        """
        :param field_type: The mapping type of the field.
        :param definition: The mapping definition of the field, e.g. to read its format.
        :param per_doc_values: One list of values per sampled document. The list is empty if the field is missing, null or empty.
        :return: The statistics of the field.
        """
        return FieldStats(type=field_type, **self.common_stats(per_doc_values))

    @staticmethod
    def common_stats(per_doc_values: List[List[Any]]) -> Dict[str, Any]:
        sample_size = len(per_doc_values)
        counts = [len(values) for values in per_doc_values if values]
        null_ratio = (sample_size - len(counts)) / sample_size if sample_size else 0.0
        return {
            "sample_size": sample_size,
            "null_ratio": null_ratio,
            "values_per_doc": count_stats(counts) if any(count > 1 for count in counts) else None
        }


def count_stats(counts: List[float]) -> Optional[CountStats]:
    if not counts:
        return None
    return CountStats(min=min(counts), max=max(counts), mean=sum(counts) / len(counts))
