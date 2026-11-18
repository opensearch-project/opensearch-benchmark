# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import logging
from typing import Any, Dict, Iterable

from osbenchmark.workload_insights.analyzers.base import Analyzer
from osbenchmark.workload_insights.analyzers.date import DateAnalyzer
from osbenchmark.workload_insights.analyzers.flatten import Flattener, log_skipped_fields
from osbenchmark.workload_insights.analyzers.keyword import KeywordAnalyzer
from osbenchmark.workload_insights.analyzers.numeric import NumericAnalyzer
from osbenchmark.workload_insights.models import AnalyzerResult

# Maps a mapping type to its analyzer. Types without an analyzer (yet) get the basic statistics (type, sample_size, null_ratio).
ANALYZERS = {
    "long": NumericAnalyzer,
    "integer": NumericAnalyzer,
    "short": NumericAnalyzer,
    "byte": NumericAnalyzer,
    "unsigned_long": NumericAnalyzer,
    "double": NumericAnalyzer,
    "float": NumericAnalyzer,
    "half_float": NumericAnalyzer,
    "scaled_float": NumericAnalyzer,
    "keyword": KeywordAnalyzer,
    "date": DateAnalyzer,
    "date_nanos": DateAnalyzer,
}


def create_analyzer(field_type: str, definition: Dict[str, Any], capture_values: bool = False) -> Analyzer:
    """
    :param field_type: The mapping type of the field.
    :param definition: The mapping definition of the field.
    :param capture_values: Whether real values from the samples may be copied into the result.
    :return: The analyzer of the field type. It accumulates the statistics of the field with ``add()`` and returns them with ``result()``.
             Analyzers of the same field that have seen different batches of documents can be combined with ``merge()``.
    """
    return ANALYZERS.get(field_type, Analyzer)(field_type, definition, capture_values)


def analyze(mapping: Dict[str, Any], samples: Iterable[Dict[str, Any]], capture_values: bool = False) -> AnalyzerResult:
    """
    Analyzes sampled documents per field. The documents are consumed one by one, so ``samples`` can be a stream.

    :param mapping: The index mapping, as returned by GET /<index>/_mapping.
    :param samples: The ``_source`` of the sampled documents.
    :param capture_values: Whether real values from the samples may be copied into the result (e.g. the most frequent keywords).
                           Disabled by default because the result may be shared or committed.
    :return: The statistics per field path. Fields that are in the samples but not in the mapping are skipped.
    """
    logger = logging.getLogger(__name__)
    flattener = Flattener(mapping)
    analyzers = {path: create_analyzer(node.type, node.definition, capture_values) for path, node in flattener.fields.items()}
    for doc in samples:
        values = flattener.flatten(doc)
        for path, analyzer in analyzers.items():
            analyzer.add(values.get(path, []))
    log_skipped_fields(logger, flattener.skipped_paths)
    return AnalyzerResult(fields={path: analyzer.result() for path, analyzer in analyzers.items()})
