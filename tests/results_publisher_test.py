# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from unittest import TestCase
from unittest.mock import Mock, patch

from osbenchmark import results_publisher

# pylint: disable=protected-access
class FormatterTests(TestCase):
    def setUp(self):
        self.empty_header = ["Header"]
        self.empty_data = []

        self.metrics_header = ["Metric", "Task", "Baseline", "Contender", "Diff", "Unit"]
        self.metrics_data = [
            ["Min Throughput", "index", "17300", "18000", "700", "ops/s"],
            ["Median Throughput", "index", "17500", "18500", "1000", "ops/s"],
            ["Max Throughput", "index", "17700", "19000", "1300", "ops/s"]
        ]
        self.numbers_align = "right"

    def test_formats_as_markdown(self):
        formatted = results_publisher.format_as_markdown(self.empty_header, self.empty_data, self.numbers_align)
        # 1 header line, 1 separation line + 0 data lines
        self.assertEqual(1 + 1 + 0, len(formatted.splitlines()))

        formatted = results_publisher.format_as_markdown(self.metrics_header, self.metrics_data, self.numbers_align)
        # 1 header line, 1 separation line + 3 data lines
        self.assertEqual(1 + 1 + 3, len(formatted.splitlines()))

    def test_formats_as_csv(self):
        formatted = results_publisher.format_as_csv(self.empty_header, self.empty_data)
        # 1 header line, no separation line + 0 data lines
        self.assertEqual(1 + 0, len(formatted.splitlines()))

        formatted = results_publisher.format_as_csv(self.metrics_header, self.metrics_data)
        # 1 header line, no separation line + 3 data lines
        self.assertEqual(1 + 3, len(formatted.splitlines()))

    @patch('osbenchmark.results_publisher.convert.to_bool')
    def test_publish_throughput_handles_different_metrics(self, mock_to_bool):
        config = Mock()

        # Configure mock to return appropriate values for different calls
        def config_opts_side_effect(*args, **kwargs):
            if args[0] == "results_publishing":
                if args[1] == "output.processingtime":
                    return False
                elif args[1] == "percentiles":
                    return None
            return Mock()

        config.opts.side_effect = config_opts_side_effect

        publisher = results_publisher.ComparisonResultsPublisher(config)

        # Mock for regular test execution
        regular_stats = Mock()
        regular_stats.metrics.return_value = {
            "throughput": {
                "min": 100,
                "max": 200,
                "mean": 150,
                "median": 160,
                "unit": "ops/s"
            }
        }

        # Mock for aggregated test execution
        aggregated_stats = Mock()
        aggregated_stats.metrics.return_value = {
            "throughput": {
                "overall_min": 95,
                "overall_max": 205,
                "min": 100,
                "max": 200,
                "mean": 150,
                "median": 160,
                "unit": "ops/s"
            }
        }

        # Test with regular stats
        result_regular = publisher._publish_throughput(regular_stats, regular_stats, "test_task")
        self.assertEqual(len(result_regular), 4)
        self.assertEqual(result_regular[0][2], 100)  # baseline min
        self.assertEqual(result_regular[3][3], 200)  # contender max

        # Test with aggregated stats
        result_aggregated = publisher._publish_throughput(aggregated_stats, aggregated_stats, "test_task")
        self.assertEqual(len(result_aggregated), 4)
        self.assertEqual(result_aggregated[0][2], 95)  # baseline overall_min
        self.assertEqual(result_aggregated[3][3], 205)  # contender overall_max
