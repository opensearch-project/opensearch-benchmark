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

import errno
import functools
import json
import os
import random
import socket
import time
import datetime

import pytest

from osbenchmark import client, config, version, paths
from osbenchmark.utils import process

CONFIG_NAMES = ["in-memory-it", "os-it"]
DISTRIBUTIONS = ["1.3.9", "2.5.0"]
WORKLOADS = ["geonames", "nyc_taxis", "http_logs", "nested"]
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def all_benchmark_configs(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", CONFIG_NAMES)
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def random_benchmark_config(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", [random.choice(CONFIG_NAMES)])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def benchmark_in_mem(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", ["in-memory-it"])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def benchmark_os(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", ["os-it"])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def osbenchmark_command_line_for(cfg, command_line):
    return f"opensearch-benchmark {command_line} --configuration-name='{cfg}'"


def osbenchmark(cfg, command_line):
    """
    This method should be used for benchmark invocations of the all commands besides test_execution.
    These commands may have different CLI options than test_execution.
    """
    cmd = osbenchmark_command_line_for(cfg, command_line)
    print(f'\n{datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")} Invoking OSB: {cmd}')
    err, retcode = process.run_subprocess_with_stderr(cmd)
    if retcode != 0:
        print(err)
    return retcode


def execute_test(cfg, command_line):
    """
    This method should be used for benchmark invocations of the test_execution command.
    It sets up some defaults for how the integration tests expect to run test_executions.
    """
    return osbenchmark(cfg, f"execute-test {command_line} --kill-running-processes --on-error='abort'")


def shell_cmd(command_line):
    """
    Executes a given command_line in a subshell.

    :param command_line: (str) The command to execute
    :return: (int) the exit code
    """

    return os.system(command_line)


def command_in_docker(command_line, python_version):
    docker_command = f"docker run --rm -v {ROOT_DIR}:/benchmark_ro:ro python:{python_version} bash -c '{command_line}'"

    return shell_cmd(docker_command)


def wait_until_port_is_free(port_number=39200, timeout=120):
    start = time.perf_counter()
    end = start + timeout
    while time.perf_counter() < end:
        c = socket.socket()
        connect_result = c.connect_ex(("127.0.0.1", port_number))
        # noinspection PyBroadException
        try:
            if connect_result == errno.ECONNREFUSED:
                c.close()
                return
            else:
                c.close()
                time.sleep(0.5)
        except Exception:
            pass

    raise TimeoutError(f"Port [{port_number}] is occupied after [{timeout}] seconds")


def check_prerequisites():
    if process.run_subprocess_with_logging("docker ps") != 0:
        raise AssertionError("Docker must be installed and the daemon must be up and running to run integration tests.")
    if process.run_subprocess_with_logging("docker-compose --help") != 0:
        raise AssertionError("Docker Compose is required to run integration tests.")


class ConfigFile:
    def __init__(self, config_name):
        self.user_home = os.getenv("BENCHMARK_HOME", os.path.expanduser("~"))
        self.benchmark_home = paths.benchmark_confdir()
        if config_name is not None:
            self.config_file_name = f"benchmark-{config_name}.ini"
        else:
            self.config_file_name = "benchmark.ini"
        self.source_path = os.path.join(os.path.dirname(__file__), "resources", self.config_file_name)
        self.target_path = os.path.join(self.benchmark_home, self.config_file_name)


class TestCluster:
    def __init__(self, cfg):
        self.cfg = cfg
        self.installation_id = None
        self.http_port = None

    def install(self, distribution_version, node_name, provision_config_instance, http_port):
        self.http_port = http_port
        transport_port = http_port + 100
        try:
            err, retcode = process.run_subprocess_with_stderr(
                "opensearch-benchmark install --configuration-name={cfg} --distribution-version={dist} --build-type=tar "
                "--http-port={http_port} --node={node_name} --master-nodes="
                "{node_name} --provision-config-instance={provision_config_instance} "
                "--seed-hosts=\"127.0.0.1:{transport_port}\"".format(cfg=self.cfg,
                                                                     dist=distribution_version,
                                                                     http_port=http_port,
                                                                     node_name=node_name,
                                                                     provision_config_instance=provision_config_instance,
                                                                     transport_port=transport_port))
            if retcode != 0:
                raise AssertionError("Failed to install OpenSearch {}.".format(distribution_version), err)
            self.installation_id = json.loads(err)["installation-id"]
        except BaseException as e:
            raise AssertionError("Failed to install OpenSearch {}.".format(distribution_version), e)

    def start(self, test_execution_id):
        cmd = "start --runtime-jdk=\"bundled\" --installation-id={} --test-execution-id={}".format(self.installation_id, test_execution_id)
        if osbenchmark(self.cfg, cmd) != 0:
            raise AssertionError("Failed to start OpenSearch test cluster.")
        opensearch = client.OsClientFactory(hosts=[{"host": "127.0.0.1", "port": self.http_port}], client_options={}).create()
        client.wait_for_rest_layer(opensearch)

    def stop(self):
        if self.installation_id:
            if osbenchmark(self.cfg, "stop --installation-id={}".format(self.installation_id)) != 0:
                raise AssertionError("Failed to stop OpenSearch test cluster.")

    def __str__(self):
        return f"TestCluster[installation-id={self.installation_id}]"


class OsMetricsStore:
    VERSION = "1.3.9"

    def __init__(self):
        self.cluster = TestCluster("in-memory-it")

    def start(self):
        self.cluster.install(distribution_version=OsMetricsStore.VERSION,
                             node_name="metrics-store",
                             provision_config_instance="defaults",
                             http_port=10200)
        self.cluster.start(test_execution_id="metrics-store")

    def stop(self):
        self.cluster.stop()


def install_integration_test_config():
    def copy_config(name):
        source_path = os.path.join(os.path.dirname(__file__), "resources", f"benchmark-{name}.ini")
        f = config.ConfigFile(name)
        f.store_default_config(template_path=source_path)

    for n in CONFIG_NAMES:
        copy_config(n)


def remove_integration_test_config():
    for config_name in CONFIG_NAMES:
        os.remove(config.ConfigFile(config_name).location)


OS_METRICS_STORE = OsMetricsStore()


def get_license():
    with open(os.path.join(ROOT_DIR, 'LICENSE')) as license_file:
        return license_file.readlines()[1].strip()


def build_docker_image():
    benchmark_version = version.__version__

    env_variables = os.environ.copy()
    env_variables['BENCHMARK_VERSION'] = benchmark_version
    env_variables['BENCHMARK_LICENSE'] = get_license()

    command = f"docker build -t opensearchproject/benchmark:{benchmark_version}" \
        f" --build-arg BENCHMARK_VERSION --build-arg BENCHMARK_LICENSE " \
              f"-f {ROOT_DIR}/docker/Dockerfiles/Dockerfile-dev {ROOT_DIR}"

    if process.run_subprocess_with_logging(command, env=env_variables) != 0:
        raise AssertionError("It was not possible to build the docker image from Dockerfile-dev")


def setup_module():
    check_prerequisites()
    install_integration_test_config()
    OS_METRICS_STORE.start()


def teardown_module():
    OS_METRICS_STORE.stop()
    remove_integration_test_config()
