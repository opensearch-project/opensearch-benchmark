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

import uuid

import pytest

import it


@pytest.fixture(scope="module")
def test_cluster():
    cluster = it.TestCluster("in-memory-it")
    # test with a recent distribution
    dist = it.DISTRIBUTIONS[-1]
    port = 19200
    test_execution_id = str(uuid.uuid4())

    it.wait_until_port_is_free(port_number=port)
    cluster.install(distribution_version=dist, node_name="benchmark-node", cluster_config="4gheap", http_port=port)
    cluster.start(test_execution_id=test_execution_id)
    yield cluster
    cluster.stop()


@it.benchmark_in_mem
def test_create_workload(cfg, tmp_path, test_cluster):
    # prepare some data
    cmd = f"--test-mode --pipeline=benchmark-only --target-hosts=127.0.0.1:{test_cluster.http_port} " \
          f" --workload=geonames --test-procedure=append-no-conflicts-index-only --quiet"
    assert it.execute_test(cfg, cmd) == 0

    # create the workload
    workload_name = f"test-workload-{uuid.uuid4()}"
    workload_path = tmp_path / workload_name

    assert it.osbenchmark(cfg, f"create-workload --target-hosts=127.0.0.1:{test_cluster.http_port} --indices=geonames "
                           f"--workload={workload_name} --output-path={tmp_path}") == 0

    expected_files = ["workload.json",
                      "geonames.json",
                      "geonames-documents-1k.json",
                      "geonames-documents.json",
                      "geonames-documents-1k.json.bz2",
                      "geonames-documents.json.bz2"]

    for f in expected_files:
        full_path = workload_path / f
        assert full_path.exists(), f"Expected file to exist at path [{full_path}]"

    # run a benchmark with the created workload
    cmd = f"--test-mode --pipeline=benchmark-only --target-hosts=127.0.0.1:{test_cluster.http_port} --workload-path={workload_path}"
    assert it.execute_test(cfg, cmd) == 0
