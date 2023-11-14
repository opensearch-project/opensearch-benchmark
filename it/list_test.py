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

import it


@it.all_benchmark_configs
def test_list_test_executions(cfg):
    assert it.osbenchmark(cfg, "list test_executions") == 0


@it.benchmark_in_mem
def test_list_cluster_configs(cfg):
    assert it.osbenchmark(cfg, "list cluster-configs") == 0
    assert it.osbenchmark(cfg, "list cluster-configs --cluster-config-repository=default") == 0


@it.benchmark_in_mem
def test_list_opensearch_plugins(cfg):
    assert it.osbenchmark(cfg, "list opensearch-plugins") == 0


@it.benchmark_in_mem
def test_list_workloads(cfg):
    assert it.osbenchmark(cfg, "list workloads") == 0
    assert it.osbenchmark(cfg, "list workloads --workload-repository=default "
                           "--workload-revision=cba4e45dda37ac03abbd3c9dd4532475dac355e9") == 0


@it.benchmark_in_mem
def test_list_telemetry(cfg):
    assert it.osbenchmark(cfg, "list telemetry") == 0
