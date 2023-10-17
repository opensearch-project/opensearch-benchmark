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

import os
import re
import unittest.mock as mock

import pytest

from osbenchmark import config, exceptions, test_execution_orchestrator


@pytest.fixture
def running_in_docker():
    os.environ["BENCHMARK_RUNNING_IN_DOCKER"] = "true"
    # just yield anything to signal the fixture is ready
    yield True
    del os.environ["BENCHMARK_RUNNING_IN_DOCKER"]


@pytest.fixture
def benchmark_only_pipeline():
    test_pipeline_name = "benchmark-only"
    original = test_execution_orchestrator.pipelines[test_pipeline_name]
    pipeline = test_execution_orchestrator.Pipeline(test_pipeline_name, "Pipeline intended for unit-testing", mock.Mock())
    yield pipeline
    # restore prior pipeline!
    test_execution_orchestrator.pipelines[test_pipeline_name] = original


@pytest.fixture
def unittest_pipeline():
    pipeline = test_execution_orchestrator.Pipeline("unit-test-pipeline", "Pipeline intended for unit-testing", mock.Mock())
    yield pipeline
    del test_execution_orchestrator.pipelines[pipeline.name]


def test_finds_available_pipelines():
    expected = [
        ["from-sources", "Builds and provisions OpenSearch, runs a benchmark and publishes results."],
        ["from-distribution",
         "Downloads an OpenSearch distribution, provisions it, runs a benchmark and publishes results."],
        ["benchmark-only", "Assumes an already running OpenSearch instance, runs a benchmark and publishes results"],
    ]

    assert expected == test_execution_orchestrator.available_pipelines()


def test_prevents_running_an_unknown_pipeline():
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "test_execution.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "test_execution", "pipeline", "invalid")
    cfg.add(config.Scope.benchmark, "builder", "distribution.version", "5.0.0")

    with pytest.raises(
            exceptions.SystemSetupError,
            match=r"Unknown pipeline \[invalid]. List the available pipelines with [\S]+? list pipelines."):
        test_execution_orchestrator.run(cfg)


def test_passes_benchmark_only_pipeline_in_docker(running_in_docker, benchmark_only_pipeline):
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "test_execution.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "test_execution", "pipeline", "benchmark-only")

    test_execution_orchestrator.run(cfg)

    benchmark_only_pipeline.target.assert_called_once_with(cfg)


def test_fails_without_benchmark_only_pipeline_in_docker(running_in_docker, unittest_pipeline):
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "test_execution.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "test_execution", "pipeline", "unit-test-pipeline")

    with pytest.raises(
            exceptions.SystemSetupError,
            match=re.escape(
                "Only the [benchmark-only] pipeline is supported by the Benchmark Docker image.\n"
                "Add --pipeline=benchmark-only in your Benchmark arguments and try again.\n"
                "For more details read the docs for the benchmark-only pipeline in "
                "https://opensearch.org/docs\n"
            )):
        test_execution_orchestrator.run(cfg)


def test_runs_a_known_pipeline(unittest_pipeline):
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "test_execution.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "test_execution", "pipeline", "unit-test-pipeline")
    cfg.add(config.Scope.benchmark, "builder", "distribution.version", "")

    test_execution_orchestrator.run(cfg)

    unittest_pipeline.target.assert_called_once_with(cfg)

def test_runs_a_default_pipeline(benchmark_only_pipeline):
    # with no pipeline specified, should default to benchmark-only
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "test_execution.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")

    test_execution_orchestrator.run(cfg)

    benchmark_only_pipeline.target.assert_called_once_with(cfg)
