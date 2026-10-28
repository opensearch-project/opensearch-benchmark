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

from unittest import TestCase, mock

from osbenchmark import benchmark, config
REGISTERED_ENGINES = "osbenchmark.benchmark.registered_engines"


class DatabaseTypeArgumentTests(TestCase):
    @mock.patch("sys.argv", ["opensearch-benchmark", "execute-test"])
    def test_defaults_to_opensearch(self):
        parser = benchmark.create_arg_parser()

        args = parser.parse_args(["execute-test"])

        self.assertEqual("opensearch", args.database_type)

    @mock.patch("sys.argv", ["opensearch-benchmark", "execute-test"])
    @mock.patch(REGISTERED_ENGINES, return_value=["opensearch", "vespa"])
    def test_accepts_registered_database_type(self, _):
        parser = benchmark.create_arg_parser()

        args = parser.parse_args(["execute-test", "--database-type", "vespa"])

        self.assertEqual("vespa", args.database_type)

    @mock.patch("sys.argv", ["opensearch-benchmark", "execute-test"])
    @mock.patch(REGISTERED_ENGINES, return_value=["opensearch"])
    @mock.patch("sys.stderr")
    def test_rejects_unregistered_database_type(self, *_):
        parser = benchmark.create_arg_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["execute-test", "--database-type", "vespa"])

    @mock.patch("sys.argv", ["opensearch-benchmark", "execute-test"])
    @mock.patch(REGISTERED_ENGINES, return_value=["opensearch", "vespa"])
    @mock.patch("osbenchmark.benchmark.print_test_execution_id")
    def test_configures_selected_database_type(self, *_):
        parser = benchmark.create_arg_parser()
        args = parser.parse_args([
            "execute-test",
            "--workload",
            "test-workload",
            "--database-type",
            "vespa",
        ])
        cfg = config.Config()

        benchmark.configure_test(parser, args, cfg)

        self.assertEqual("vespa", cfg.opts("database", "type"))
