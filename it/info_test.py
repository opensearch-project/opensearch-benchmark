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
from osbenchmark.utils import process


@it.benchmark_in_mem
def test_workload_info_with_test_procedure(cfg):
    assert it.osbenchmark(cfg, "info --workload=geonames --test-procedure=append-no-conflicts") == 0


@it.benchmark_in_mem
def test_workload_info_with_workload_repo(cfg):
    assert it.osbenchmark(cfg, "info --workload-repository=default --workload=geonames") == 0


@it.benchmark_in_mem
def test_workload_info_with_task_filter(cfg):
    assert it.osbenchmark(cfg, "info --workload=geonames --test-procedure=append-no-conflicts --include-tasks=\"type:search\"") == 0


@it.benchmark_in_mem
def test_workload_info_fails_with_wrong_workload_params(cfg):
    # simulate a typo in workload parameter
    cmd = it.osbenchmark_command_line_for(cfg, "info --workload=geonames --workload-params='conflict_probability:5,number-of-replicas:1'")
    output = process.run_subprocess_with_output(cmd)
    expected = "Some of your workload parameter(s) \"number-of-replicas\" are not used by this workload; " \
               "perhaps you intend to use \"number_of_replicas\" instead.\n\nAll workload parameters you " \
               "provided are:\n- conflict_probability\n- number-of-replicas\n\nAll parameters exposed by this workload"

    assert expected in "\n".join(output)
