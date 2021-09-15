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


@it.all_rally_configs
def test_list_test_executions(cfg):
    assert it.esrally(cfg, "list test_executions") == 0


@it.rally_in_mem
def test_list_provision_config_instances(cfg):
    assert it.esrally(cfg, "list provision_config_instances") == 0
    assert it.esrally(cfg, "list provision_config_instances --provision-config-repository=default") == 0


@it.rally_in_mem
def test_list_opensearch_plugins(cfg):
    assert it.esrally(cfg, "list opensearch-plugins") == 0


@it.rally_in_mem
def test_list_workloads(cfg):
    assert it.esrally(cfg, "list workloads") == 0
    assert it.esrally(cfg, "list workloads --workload-repository=default "
                           "--workload-revision=4080dc9850d07e23b6fc7cfcdc7cf57b14e5168d") == 0


@it.rally_in_mem
def test_list_telemetry(cfg):
    assert it.esrally(cfg, "list telemetry") == 0
