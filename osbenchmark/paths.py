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


def benchmark_confdir():
    default_home = os.path.expanduser("~")
    old_path = os.path.join(default_home, ".benchmark")
    new_path = os.path.join(default_home, ".osb")

    # Create .benchmark directory if it doesn't exist
    if not os.path.exists(old_path):
        os.makedirs(old_path, exist_ok=True)

    # Create .osb directory if it doesn't exist
    if not os.path.exists(new_path):
        os.makedirs(new_path, exist_ok=True)

    # Create symlink from .osb to .benchmark if it doesn't exist
    if not os.path.islink(new_path):
        try:
            os.symlink(old_path, new_path, target_is_directory=True)
        except OSError:
            print(f"Warning: Failed to create symlink from {new_path} to {old_path}")

    return os.path.join(os.getenv("BENCHMARK_HOME", default_home), ".osb")


def benchmark_root():
    return os.path.dirname(os.path.realpath(__file__))


def test_excecutions_root(cfg):
    return os.path.join(cfg.opts("node", "root.dir"), "test_executions")


def test_execution_root(cfg, test_execution_id=None):
    if not test_execution_id:
        test_execution_id = cfg.opts("system", "test_execution.id")
    return os.path.join(test_excecutions_root(cfg), test_execution_id)

def aggregated_results_root(cfg, test_execution_id=None):
    if not test_execution_id:
        test_execution_id = cfg.opts("system", "test_execution.id")
    return os.path.join(cfg.opts("node", "root.dir"), "aggregated_results", test_execution_id)

def install_root(cfg=None):
    install_id = cfg.opts("system", "install.id")
    return os.path.join(test_excecutions_root(cfg), install_id)


# There is a weird bug manifesting in jenkins that is somehow saying the following line has an invalid docstring
# So to work around it, we are adding this disable, even though the docstring is perfectly fine.
# pylint: disable=invalid-docstring-quote
def logs():
    """
    :return: The absolute path to the directory that contains Benchmark's log file.
    """
    return os.path.join(benchmark_confdir(), "logs")
