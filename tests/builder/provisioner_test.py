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
# pylint: disable=protected-access

import os
import tempfile
import unittest.mock as mock
from unittest import TestCase

from osbenchmark import exceptions
from osbenchmark.builder import provisioner, provision_config

HOME_DIR = os.path.expanduser("~")


class BareProvisionerTests(TestCase):
    @mock.patch("glob.glob", lambda p: ["/opt/opensearch-1.0.0"])
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_without_plugins(self, mock_rm, mock_ensure_dir, mock_decompress):
        apply_config_calls = []

        def null_apply_config(source_root_path, target_root_path, config_vars):
            apply_config_calls.append((source_root_path, target_root_path, config_vars))

        installer = provisioner.OpenSearchInstaller(provision_config_instance=
        provision_config.ProvisionConfigInstance(
            names="unit-test-provision-config-instance",
            root_path=None,
            config_paths=[HOME_DIR + "/.benchmark/benchmarks/provision_configs/default/my-provision-config-instance"],
            variables={"heap": "4g", "runtime.jdk": "8", "runtime.jdk.bundled": "true"}),
            java_home="/usr/local/javas/java8",
            node_name="benchmark-node-0",
            node_root_dir=HOME_DIR + "/.benchmark/benchmarks/test_executions/unittest",
            all_node_ips=["10.17.22.22", "10.17.22.23"],
            all_node_names=["benchmark-node-0", "benchmark-node-1"],
            ip="10.17.22.23",
            http_port=9200)

        p = provisioner.BareProvisioner(os_installer=installer,
                                        plugin_installers=[],
                                        apply_config=null_apply_config)

        node_config = p.prepare({"opensearch": "/opt/opensearch-1.0.0.tar.gz"})
        self.assertEqual("8", node_config.provision_config_instance_runtime_jdks)
        self.assertEqual("/opt/opensearch-1.0.0", node_config.binary_path)
        self.assertEqual(["/opt/opensearch-1.0.0/data"], node_config.data_paths)

        self.assertEqual(1, len(apply_config_calls))
        source_root_path, target_root_path, config_vars = apply_config_calls[0]

        self.assertEqual(HOME_DIR + "/.benchmark/benchmarks/provision_configs/default/my-provision-config-instance", source_root_path)
        self.assertEqual("/opt/opensearch-1.0.0", target_root_path)
        self.assertEqual({
            "cluster_settings": {
            },
            "heap": "4g",
            "runtime.jdk": "8",
            "runtime.jdk.bundled": "true",
            "cluster_name": "benchmark-provisioned-cluster",
            "node_name": "benchmark-node-0",
            "data_paths": ["/opt/opensearch-1.0.0/data"],
            "log_path": HOME_DIR + "/.benchmark/benchmarks/test_executions/unittest/logs/server",
            "heap_dump_path": HOME_DIR + "/.benchmark/benchmarks/test_executions/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "all_node_names": "[\"benchmark-node-0\",\"benchmark-node-1\"]",
            "minimum_master_nodes": 2,
            "install_root_path": "/opt/opensearch-1.0.0"
        }, config_vars)

    class NoopHookHandler:
        def __init__(self, plugin):
            self.hook_calls = {}

        def can_load(self):
            return False

        def invoke(self, phase, variables, **kwargs):
            self.hook_calls[phase] = {
                "variables": variables,
                "kwargs": kwargs
            }


class NoopHookHandler:
    def __init__(self, component):
        self.hook_calls = {}

    def can_load(self):
        return False

    def invoke(self, phase, variables, **kwargs):
        self.hook_calls[phase] = {
            "variables": variables,
            "kwargs": kwargs,
        }


class OpenSearchInstallerTests(TestCase):
    @mock.patch("glob.glob", lambda p: ["/install/opensearch-5.0.0-SNAPSHOT"])
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_default_data_paths(self, mock_rm, mock_ensure_dir, mock_decompress):
        installer = provisioner.OpenSearchInstaller(provision_config_instance=provision_config.ProvisionConfigInstance(names="defaults",
                                                                    root_path=None,
                                                                    config_paths="/tmp"),
                                                       java_home="/usr/local/javas/java8",
                                                       node_name="benchmark-node-0",
                                                       all_node_ips=["10.17.22.22", "10.17.22.23"],
                                                       all_node_names=["benchmark-node-0", "benchmark-node-1"],
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       node_root_dir=HOME_DIR + "/.benchmark/benchmarks/test_executions/unittest")

        installer.install("/data/builds/distributions")
        self.assertEqual(installer.os_home_path, "/install/opensearch-5.0.0-SNAPSHOT")

        self.assertEqual({
            "cluster_name": "benchmark-provisioned-cluster",
            "node_name": "benchmark-node-0",
            "data_paths": ["/install/opensearch-5.0.0-SNAPSHOT/data"],
            "log_path": HOME_DIR + "/.benchmark/benchmarks/test_executions/unittest/logs/server",
            "heap_dump_path": HOME_DIR + "/.benchmark/benchmarks/test_executions/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "all_node_names": "[\"benchmark-node-0\",\"benchmark-node-1\"]",
            "minimum_master_nodes": 2,
            "install_root_path": "/install/opensearch-5.0.0-SNAPSHOT"
        }, installer.variables)

        self.assertEqual(installer.data_paths, ["/install/opensearch-5.0.0-SNAPSHOT/data"])

    @mock.patch("glob.glob", lambda p: ["/install/opensearch-5.0.0-SNAPSHOT"])
    @mock.patch("osbenchmark.utils.io.decompress")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("shutil.rmtree")
    def test_prepare_user_provided_data_path(self, mock_rm, mock_ensure_dir, mock_decompress):
        installer = provisioner.OpenSearchInstaller(provision_config_instance=provision_config.ProvisionConfigInstance(names="defaults",
                                                                    root_path=None,
                                                                    config_paths="/tmp",
                                                                    variables={"data_paths": "/tmp/some/data-path-dir"}),
                                                       java_home="/usr/local/javas/java8",
                                                       node_name="benchmark-node-0",
                                                       all_node_ips=["10.17.22.22", "10.17.22.23"],
                                                       all_node_names=["benchmark-node-0", "benchmark-node-1"],
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       node_root_dir="~/.benchmark/benchmarks/test_executions/unittest")

        installer.install("/data/builds/distributions")
        self.assertEqual(installer.os_home_path, "/install/opensearch-5.0.0-SNAPSHOT")

        self.assertEqual({
            "cluster_name": "benchmark-provisioned-cluster",
            "node_name": "benchmark-node-0",
            "data_paths": ["/tmp/some/data-path-dir"],
            "log_path": "~/.benchmark/benchmarks/test_executions/unittest/logs/server",
            "heap_dump_path": "~/.benchmark/benchmarks/test_executions/unittest/heapdump",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "all_node_names": "[\"benchmark-node-0\",\"benchmark-node-1\"]",
            "minimum_master_nodes": 2,
            "install_root_path": "/install/opensearch-5.0.0-SNAPSHOT"
        }, installer.variables)

        self.assertEqual(installer.data_paths, ["/tmp/some/data-path-dir"])

    def test_invokes_hook_with_java_home(self):
        installer = provisioner.OpenSearchInstaller(provision_config_instance=provision_config.ProvisionConfigInstance(names="defaults",
                                                                    root_path="/tmp",
                                                                    config_paths="/tmp/templates",
                                                                    variables={"data_paths": "/tmp/some/data-path-dir"}),
                                                       java_home="/usr/local/javas/java8",
                                                       node_name="benchmark-node-0",
                                                       all_node_ips=["10.17.22.22", "10.17.22.23"],
                                                       all_node_names=["benchmark-node-0", "benchmark-node-1"],
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       node_root_dir="~/.benchmark/benchmarks/test_executions/unittest",
                                                       hook_handler_class=NoopHookHandler)

        self.assertEqual(0, len(installer.hook_handler.hook_calls))
        installer.invoke_install_hook(provision_config.BootstrapPhase.post_install, {"foo": "bar"})
        self.assertEqual(1, len(installer.hook_handler.hook_calls))
        self.assertEqual({"foo": "bar"}, installer.hook_handler.hook_calls["post_install"]["variables"])
        self.assertEqual({"env": {"JAVA_HOME": "/usr/local/javas/java8"}},
                         installer.hook_handler.hook_calls["post_install"]["kwargs"])

    def test_invokes_hook_no_java_home(self):
        installer = provisioner.OpenSearchInstaller(provision_config_instance=provision_config.ProvisionConfigInstance(names="defaults",
                                                                    root_path="/tmp",
                                                                    config_paths="/tmp/templates",
                                                                    variables={"data_paths": "/tmp/some/data-path-dir"}),
                                                       java_home=None,
                                                       node_name="benchmark-node-0",
                                                       all_node_ips=["10.17.22.22", "10.17.22.23"],
                                                       all_node_names=["benchmark-node-0", "benchmark-node-1"],
                                                       ip="10.17.22.23",
                                                       http_port=9200,
                                                       node_root_dir="~/.benchmark/benchmarks/test_executions/unittest",
                                                       hook_handler_class=NoopHookHandler)

        self.assertEqual(0, len(installer.hook_handler.hook_calls))
        installer.invoke_install_hook(provision_config.BootstrapPhase.post_install, {"foo": "bar"})
        self.assertEqual(1, len(installer.hook_handler.hook_calls))
        self.assertEqual({"foo": "bar"}, installer.hook_handler.hook_calls["post_install"]["variables"])
        self.assertEqual({"env": {}}, installer.hook_handler.hook_calls["post_install"]["kwargs"])


class PluginInstallerTests(TestCase):
    @mock.patch("osbenchmark.utils.process.run_subprocess_with_logging")
    def test_install_plugin_successfully(self, installer_subprocess):
        installer_subprocess.return_value = "output", 0

        plugin = provision_config.PluginDescriptor(name="unit-test-plugin", config="default", variables={"active": True})
        installer = provisioner.PluginInstaller(plugin,
                                                java_home="/usr/local/javas/java8",
                                                hook_handler_class=NoopHookHandler)

        installer.install(os_home_path="/opt/opensearch")

        installer_subprocess.assert_called_with(
            '/opt/opensearch/bin/opensearch-plugin install --batch "unit-test-plugin"',
            env={"JAVA_HOME": "/usr/local/javas/java8"}, capture_output=True)

    @mock.patch("osbenchmark.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_bundled_jdk(self, installer_subprocess):
        installer_subprocess.return_value = "output", 0

        plugin = provision_config.PluginDescriptor(name="unit-test-plugin", config="default", variables={"active": True})
        installer = provisioner.PluginInstaller(plugin,
                                                # bundled JDK
                                                java_home=None,
                                                hook_handler_class=NoopHookHandler)

        installer.install(os_home_path="/opt/opensearch")

        installer_subprocess.assert_called_with(
            '/opt/opensearch/bin/opensearch-plugin install --batch "unit-test-plugin"',
            env={}, capture_output=True)

    @mock.patch("osbenchmark.utils.process.run_subprocess_with_logging")
    def test_install_unknown_plugin(self, installer_subprocess):
        # unknown plugin
        installer_subprocess.return_value = "output", 64

        plugin = provision_config.PluginDescriptor(name="unknown")
        installer = provisioner.PluginInstaller(plugin,
                                                java_home="/usr/local/javas/java8",
                                                hook_handler_class=NoopHookHandler)

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            installer.install(os_home_path="/opt/opensearch")
        self.assertEqual("Unknown plugin [unknown]", ctx.exception.args[0])

        installer_subprocess.assert_called_with(
            '/opt/opensearch/bin/opensearch-plugin install --batch "unknown"',
            env={"JAVA_HOME": "/usr/local/javas/java8"}, capture_output=True)

    @mock.patch("osbenchmark.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_io_error(self, installer_subprocess):
        # I/O error
        installer_subprocess.return_value = "output", 74

        plugin = provision_config.PluginDescriptor(name="simple")
        installer = provisioner.PluginInstaller(plugin,
                                                java_home="/usr/local/javas/java8",
                                                hook_handler_class=NoopHookHandler)

        with self.assertRaises(exceptions.SupplyError) as ctx:
            installer.install(os_home_path="/opt/opensearch")
        self.assertEqual("I/O error while trying to install [simple]", ctx.exception.args[0])

        installer_subprocess.assert_called_with(
            '/opt/opensearch/bin/opensearch-plugin install --batch "simple"',
            env={"JAVA_HOME": "/usr/local/javas/java8"}, capture_output=True)

    @mock.patch("osbenchmark.utils.process.run_subprocess_with_logging")
    def test_install_plugin_with_unknown_error(self, installer_subprocess):
        # some other error
        installer_subprocess.return_value = "output", 12987

        plugin = provision_config.PluginDescriptor(name="simple")
        installer = provisioner.PluginInstaller(plugin,
                                                java_home="/usr/local/javas/java8",
                                                hook_handler_class=NoopHookHandler)

        with self.assertRaises(exceptions.BenchmarkError) as ctx:
            installer.install(os_home_path="/opt/opensearch")
        self.assertEqual("Unknown error 'output' while trying to install [simple] (installer return code [12987]). Please check the logs.",
                         ctx.exception.args[0])

        installer_subprocess.assert_called_with(
            '/opt/opensearch/bin/opensearch-plugin install --batch "simple"',
            env={"JAVA_HOME": "/usr/local/javas/java8"}, capture_output=True)

    def test_pass_plugin_properties(self):
        plugin = provision_config.PluginDescriptor(name="unit-test-plugin",
                                       config="default",
                                       config_paths=["/etc/plugin"],
                                       variables={"active": True})
        installer = provisioner.PluginInstaller(plugin,
                                                java_home="/usr/local/javas/java8",
                                                hook_handler_class=NoopHookHandler)

        self.assertEqual("unit-test-plugin", installer.plugin_name)
        self.assertEqual({"active": True}, installer.variables)
        self.assertEqual(["/etc/plugin"], installer.config_source_paths)

    def test_invokes_hook_with_java_home(self):
        plugin = provision_config.PluginDescriptor(name="unit-test-plugin",
                                       config="default",
                                       config_paths=["/etc/plugin"],
                                       variables={"active": True})
        installer = provisioner.PluginInstaller(plugin,
                                                java_home="/usr/local/javas/java8",
                                                hook_handler_class=NoopHookHandler)

        self.assertEqual(0, len(installer.hook_handler.hook_calls))
        installer.invoke_install_hook(provision_config.BootstrapPhase.post_install, {"foo": "bar"})
        self.assertEqual(1, len(installer.hook_handler.hook_calls))
        self.assertEqual({"foo": "bar"}, installer.hook_handler.hook_calls["post_install"]["variables"])
        self.assertEqual({"env": {"JAVA_HOME": "/usr/local/javas/java8"}},
                         installer.hook_handler.hook_calls["post_install"]["kwargs"])

    def test_invokes_hook_no_java_home(self):
        plugin = provision_config.PluginDescriptor(name="unit-test-plugin",
                                       config="default",
                                       config_paths=["/etc/plugin"],
                                       variables={"active": True})
        installer = provisioner.PluginInstaller(plugin,
                                                java_home=None,
                                                hook_handler_class=NoopHookHandler)

        self.assertEqual(0, len(installer.hook_handler.hook_calls))
        installer.invoke_install_hook(provision_config.BootstrapPhase.post_install, {"foo": "bar"})
        self.assertEqual(1, len(installer.hook_handler.hook_calls))
        self.assertEqual({"foo": "bar"}, installer.hook_handler.hook_calls["post_install"]["variables"])
        self.assertEqual({"env": {}}, installer.hook_handler.hook_calls["post_install"]["kwargs"])


class DockerProvisionerTests(TestCase):
    maxDiff = None
    @mock.patch("uuid.uuid4")
    def test_provisioning_with_defaults(self, uuid4):
        uuid4.return_value = "9dbc682e-d32a-4669-8fbe-56fb77120dd4"
        node_root_dir = tempfile.gettempdir()
        log_dir = os.path.join(node_root_dir, "logs", "server")
        heap_dump_dir = os.path.join(node_root_dir, "heapdump")
        data_dir = os.path.join(node_root_dir, "data", "9dbc682e-d32a-4669-8fbe-56fb77120dd4")

        benchmark_root = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, "osbenchmark"))

        c = provision_config.ProvisionConfigInstance("unit-test-provision-config-instance", None, "/tmp", variables={
            "docker_image": "opensearchproject/opensearch"
        })

        docker = provisioner.DockerProvisioner(provision_config_instance=c,
                                               node_name="benchmark-node-0",
                                               ip="10.17.22.33",
                                               http_port=39200,
                                               node_root_dir=node_root_dir,
                                               distribution_version="1.1.0",
                                               benchmark_root=benchmark_root)

        self.assertDictEqual({
            "cluster_name": "benchmark-provisioned-cluster",
            "node_name": "benchmark-node-0",
            "install_root_path": "/usr/share/opensearch",
            "data_paths": ["/usr/share/opensearch/data"],
            "log_path": "/var/log/opensearch",
            "heap_dump_path": "/usr/share/opensearch/heapdump",
            "discovery_type": "single-node",
            "network_host": "0.0.0.0",
            "http_port": "39200",
            "transport_port": "39300",
            "cluster_settings": {
            },
            "docker_image": "opensearchproject/opensearch"
        }, docker.config_vars)

        self.assertDictEqual({
            "os_data_dir": data_dir,
            "os_log_dir": log_dir,
            "os_heap_dump_dir": heap_dump_dir,
            "os_version": "1.1.0",
            "docker_image": "opensearchproject/opensearch",
            "http_port": 39200,
            "mounts": {}
        }, docker.docker_vars(mounts={}))

        docker_cfg = docker._render_template_from_file(docker.docker_vars(mounts={}))

        self.assertEqual(
"""version: '3'
services:
  opensearch-node1:
    image: opensearchproject/opensearch:1.1.0
    container_name: opensearch-node1
    labels:
      io.benchmark.description: "opensearch-benchmark"
    environment:
      - cluster.name=opensearch-cluster
      - node.name=opensearch-node1
      - discovery.seed_hosts=opensearch-node1
      - DISABLE_INSTALL_DEMO_CONFIG=true
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    volumes:
      - %s:/usr/share/opensearch/data
      - %s:/var/log/opensearch
      - %s:/usr/share/opensearch/heapdump
    ports:
      - 39200:39200
      - 9200:9200
      - 9600:9600
    networks:
      - opensearch-net
    healthcheck:
          test: curl -f http://localhost:39200 -u admin:admin --insecure
          interval: 5s
          timeout: 2s
          retries: 10

volumes:
  opensearch-data1:
networks:
  opensearch-net:""" % (data_dir, log_dir, heap_dump_dir), docker_cfg)

    @mock.patch("uuid.uuid4")
    def test_provisioning_with_variables(self, uuid4):
        uuid4.return_value = "86f42ae0-5840-4b5b-918d-41e7907cb644"
        node_root_dir = tempfile.gettempdir()
        log_dir = os.path.join(node_root_dir, "logs", "server")
        heap_dump_dir = os.path.join(node_root_dir, "heapdump")
        data_dir = os.path.join(node_root_dir, "data", "86f42ae0-5840-4b5b-918d-41e7907cb644")

        benchmark_root = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir, "osbenchmark"))

        c = provision_config.ProvisionConfigInstance("unit-test-provision-config-instance", None, "/tmp", variables={
            "docker_image": "opensearchproject/opensearch",
            "docker_mem_limit": "256m",
            "docker_cpu_count": 2
        })

        docker = provisioner.DockerProvisioner(provision_config_instance=c,
                                               node_name="benchmark-node-0",
                                               ip="10.17.22.33",
                                               http_port=39200,
                                               node_root_dir=node_root_dir,
                                               distribution_version="1.1.0",
                                               benchmark_root=benchmark_root)

        docker_cfg = docker._render_template_from_file(docker.docker_vars(mounts={}))

        self.assertEqual(
"""version: '3'
services:
  opensearch-node1:
    image: opensearchproject/opensearch:1.1.0
    container_name: opensearch-node1
    labels:
      io.benchmark.description: "opensearch-benchmark"
    cpu_count: 2
    mem_limit: 256m
    environment:
      - cluster.name=opensearch-cluster
      - node.name=opensearch-node1
      - discovery.seed_hosts=opensearch-node1
      - DISABLE_INSTALL_DEMO_CONFIG=true
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    volumes:
      - %s:/usr/share/opensearch/data
      - %s:/var/log/opensearch
      - %s:/usr/share/opensearch/heapdump
    ports:
      - 39200:39200
      - 9200:9200
      - 9600:9600
    networks:
      - opensearch-net
    healthcheck:
          test: curl -f http://localhost:39200 -u admin:admin --insecure
          interval: 5s
          timeout: 2s
          retries: 10

volumes:
  opensearch-data1:
networks:
  opensearch-net:""" % (data_dir, log_dir, heap_dump_dir), docker_cfg)


class CleanupTests(TestCase):
    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_preserves(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        provisioner.cleanup(
            preserve=True,
            install_dir="./benchmark/test_executions/install",
            data_paths=["./benchmark/test_executions/data"])

        self.assertEqual(mock_path_exists.call_count, 0)
        self.assertEqual(mock_rm.call_count, 0)

    @mock.patch("shutil.rmtree")
    @mock.patch("os.path.exists")
    def test_cleanup(self, mock_path_exists, mock_rm):
        mock_path_exists.return_value = True

        provisioner.cleanup(
            preserve=False,
            install_dir="./benchmark/test_executions/install",
            data_paths=["./benchmark/test_executions/data"])

        expected_dir_calls = [mock.call("/tmp/some/data-path-dir"), mock.call("/benchmark-root/workload/test_procedure/es-bin")]
        mock_path_exists.mock_calls = expected_dir_calls
        mock_rm.mock_calls = expected_dir_calls
