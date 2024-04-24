#  SPDX-License-Identifier: Apache-2.0
#
#  The OpenSearch Contributors require contributions made to
#  this file be licensed under the Apache-2.0 license or a
#  compatible open source license.
#  Modifications Copyright OpenSearch Contributors. See
#  GitHub history for details.
#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from functools import partial
from osbenchmark.results_publisher import format_as_markdown, format_as_csv
from osbenchmark import exceptions
from osbenchmark.utils import io as rio

VALUE_KEY = "Value"
TASK_KEY = "Task"
UNIT_KEY = "Unit"
KEYS = [
    "Cumulative indexing time of primary shards",
    "Min cumulative indexing time across primary shards",
    "Median cumulative indexing time across primary shards",
    "Max cumulative indexing time across primary shards",
    "Cumulative indexing throttle time of primary shards",
    "Min cumulative indexing throttle time across primary shards",
    "Median cumulative indexing throttle time across primary shards",
    "Max cumulative indexing throttle time across primary shards",
    "Cumulative merge time of primary shards",
    "Cumulative merge count of primary shards",
    "Min cumulative merge time across primary shards",
    "Median cumulative merge time across primary shards",
    "Max cumulative merge time across primary shards",
    "Cumulative merge throttle time of primary shards",
    "Min cumulative merge throttle time across primary shards",
    "Median cumulative merge throttle time across primary shards",
    "Max cumulative merge throttle time across primary shards",
    "Cumulative refresh time of primary shards",
    "Cumulative refresh count of primary shards",
    "Min cumulative refresh time across primary shards",
    "Median cumulative refresh time across primary shards",
    "Max cumulative refresh time across primary shards",
    "Cumulative flush time of primary shards",
    "Cumulative flush count of primary shards",
    "Min cumulative flush time across primary shards",
    "Median cumulative flush time across primary shards",
    "Max cumulative flush time across primary shards",
    "Total Young Gen GC time",
    "Total Young Gen GC count",
    "Total Old Gen GC time",
    "Total Old Gen GC count",
    "Store size",
    "Translog size",
    "Heap used for segments",
    "Heap used for doc values",
    "Heap used for terms",
    "Heap used for norms",
    "Heap used for points",
    "Heap used for stored fields",
    "Segment count",
    "Min Throughput",
    "Mean Throughput",
    "Median Throughput",
    "Max Throughput",
    "50th percentile latency",
    "90th percentile latency",
    "99th percentile latency",
    "99.9th percentile latency",
    "99.99th percentile latency",
    "100th percentile latency",
    "50th percentile service time",
    "90th percentile service time",
    "99th percentile service time",
    "99.9th percentile service time",
    "99.99th percentile service time",
    "100th percentile service time",
    "error rate",
    "Total time"
]


class TuningPublisher:
    def __init__(self, config):
        self.results_file = config.opts("results_publishing", "output.path", mandatory=False)
        self.results_format = config.opts("results_publishing", "format")
        self.numbers_align = config.opts("results_publishing", "numbers.align",
                                         mandatory=False, default_value="right")
        self.cwd = config.opts("node", "benchmark.cwd")

    def publish(self, results):
        write_results(self.results_format, self.numbers_align, self.results_file, self.cwd, results)


def construct_data(results, keys):
    lines = []
    for key in keys:
        last_result_with_key = None
        for result in results:
            if key in result.output:
                last_result_with_key = result

        if not last_result_with_key:
            continue
        line = [key, last_result_with_key.output[key][TASK_KEY]]
        for result in results:
            if key not in result.output:
                line.append("")
            else:
                line.append(result.output[key][VALUE_KEY])
        line.append(last_result_with_key.output[key][UNIT_KEY])
        lines.append(line)
    return lines


def write_results(results_format, numbers_align, results_file, cwd, results):
    if results_format == "markdown":
        formatter = partial(format_as_markdown, numbers_align=numbers_align)
    elif results_format == "csv":
        formatter = format_as_csv
    else:
        raise exceptions.SystemSetupError("Unknown publish format '%s'" % results_format)

    headers = ["Metric", "Task"]
    for result in results:
        headers.append(str(result))
    headers.append("Unit")

    lines = construct_data(results, KEYS)

    if len(results_file) > 0:
        normalized_results_file = rio.normalize_path(results_file, cwd)
        # ensure that the parent folder already exists when we try to write the file...
        rio.ensure_dir(rio.dirname(normalized_results_file))
        with open(normalized_results_file, mode="a+", encoding="utf-8") as f:
            f.writelines(formatter(headers, lines))
