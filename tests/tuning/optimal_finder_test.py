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

import pytest
from osbenchmark.tuning.optimal_finder import find_optimal_result, get_successful_results
from osbenchmark.tuning.result import Result


@pytest.fixture()
def results():
    result1 = Result("id1", 0, 0, 0)
    result2 = Result("id2", 0, 0, 0)
    result3 = Result("id3", 0, 0, 0)
    result4 = Result("id4", 0, 0, 0)
    return [result1, result2, result3, result4]


def test_find_optimal_result(results):
    results[0].set_output(True, 25, None)
    results[1].set_output(True, 15, None)
    results[2].set_output(True, 45, None)
    results[3].set_output(True, 125, None)
    assert find_optimal_result(results).test_id == "id2"


def test_get_successful_results_all_failed(results):
    results[0].set_output(False, 25, None)
    results[1].set_output(False, 15, None)
    results[2].set_output(False, 45, None)
    results[3].set_output(False, 125, None)
    assert len(get_successful_results(results, 0)) == 0


def test_get_successful_ids_error_rate(results):
    results[0].set_output(False, 25, {"error rate": {"Value": 0.1}})
    results[1].set_output(True, 15, {"error rate": {"Value": 0.2}})
    results[2].set_output(True, 45, {"error rate": {"Value": 0.3}})
    results[3].set_output(True, 125, {"error rate": {"Value": 0.4}})
    assert len(get_successful_results(results, 0.21)) == 1
    assert len(get_successful_results(results, 0.31)) == 2
    assert len(get_successful_results(results, 0.4)) == 3
