# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#  Licensed to Elasticsearch B.V. under one or more contributor
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


@it.random_benchmark_config
def test_tar_distributions(cfg):
    port = 19200
    for dist in it.DISTRIBUTIONS:
        for workload in it.WORKLOADS:
            it.wait_until_port_is_free(port_number=port)
            assert it.execute_test(cfg, f"--distribution-version=\"{dist}\" --workload=\"{workload}\" "
                                f"--test-mode --cluster-config=4gheap --target-hosts=127.0.0.1:{port}") == 0


@it.random_benchmark_config
def test_docker_distribution(cfg):
    port = 19200
    # only test the most recent Docker distribution
    dist = it.DISTRIBUTIONS[-1]
    it.wait_until_port_is_free(port_number=port)
    assert it.execute_test(cfg, f"--pipeline=\"docker\" --distribution-version=\"{dist}\" "
                        f"--workload=\"geonames\" --test-procedure=\"append-no-conflicts-index-only\" --test-mode "
                        f"--cluster-config=4gheap --target-hosts=127.0.0.1:{port}") == 0
