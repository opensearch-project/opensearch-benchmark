# pylint: disable=protected-access

import os
from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.installers.opensearch_installer import OpenSearchInstaller
from osbenchmark.builder.models.host import Host
from osbenchmark.builder.models.node import Node
from osbenchmark.builder.provision_config import ProvisionConfigInstance


class OpenSearchInstallerTests(TestCase):
    def setUp(self):
        self.host = Host(name="fake", address="10.17.22.23", metadata={}, node=None)
        self.binaries = {OpenSearchInstaller.OPENSEARCH_BINARY_KEY: "/data/builds/distributions"}
        self.all_node_ips = ["10.17.22.22", "10.17.22.23"]

        self.test_execution_root = "fake_root"
        self.node_id = "abdefg"
        self.cluster_name = "my-cluster"

        self.executor = Mock()
        self.hook_handler_class = Mock()

        self.provision_config_instance = ProvisionConfigInstance(
            names="defaults",
            root_path="fake",
            config_paths=["/tmp"],
            variables={
                "test_execution_root": self.test_execution_root,
                "cluster_name": self.cluster_name,
                "node": {
                    "port": "9200"
                },
                "preserve_install": False
            }
        )
        self.installer = OpenSearchInstaller(self.provision_config_instance, self.executor, self.hook_handler_class)

    @mock.patch("os.walk")
    @mock.patch("osbenchmark.utils.io.is_plain_text")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("uuid.uuid4")
    def test_install(self, uuid, ensure_dir, is_plain_text, os_walk):
        # Create directory x 3, extract OpenSearch binary, delete prebundled config, create directory, copy file
        self.executor.execute.side_effect = [None, None, None, None, None, None, None]
        is_plain_text.return_value = False
        os_walk.return_value = [("fake", "fake", ["fake"])]
        uuid.return_value = self.node_id

        node = self.installer.install(self.host, self.binaries, self.all_node_ips)
        self.assertEqual(node.binary_path, os.path.join(self.test_execution_root, self.node_id, "install/opensearch*"))
        self.assertEqual(node.data_paths, [os.path.join(self.test_execution_root, self.node_id, "install/opensearch*/data")])
        self.assertEqual(node.port, 9200)
        self.assertEqual(node.root_dir, os.path.join(self.test_execution_root, self.node_id))
        self.assertEqual(node.name, self.node_id)

    @mock.patch("os.walk")
    @mock.patch("osbenchmark.utils.io.is_plain_text")
    @mock.patch("osbenchmark.utils.io.ensure_dir")
    @mock.patch("uuid.uuid4")
    def test_config_vars(self, uuid, ensure_dir, is_plain_text, os_walk):
        # Create directory x 3, extract OpenSearch binary, delete prebundled config, create directory, copy file
        self.executor.execute.side_effect = [None, None, None, None, None, None, None]
        is_plain_text.return_value = False
        os_walk.return_value = [("fake", "fake", ["fake"])]
        uuid.return_value = self.node_id

        node = self.installer.install(self.host, self.binaries, self.all_node_ips)
        config_vars = self.installer._get_config_vars(self.host, node, "abc", "def", self.all_node_ips)

        self.assertEqual({
            "cluster_name": self.cluster_name,
            "node_name": self.node_id,
            "data_paths": os.path.join(self.test_execution_root, self.node_id, "install/opensearch*/data"),
            "log_path": "abc",
            "heap_dump_path": "def",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "minimum_master_nodes": 2,
            "install_root_path": os.path.join(self.test_execution_root, self.node_id, "install/opensearch*"),
            "node": {"port": "9200"},
            "test_execution_root": self.test_execution_root,
            "preserve_install": False
        }, config_vars)

    def test_cleanup(self):
        self.executor.execute.return_value = None

        self.host.node = Node(binary_path="/fake", data_paths=["/fake1", "/fake2"],
                              name=None, pid=None, telemetry=None, port=None, root_dir=None)
        self.installer.cleanup(self.host)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "rm -r /fake1"),
            mock.call(self.host, "rm -r /fake2"),
            mock.call(self.host, "rm -r /fake")
        ])

    def test_cleanup_preserve_install(self):
        self.executor.execute.return_value = None

        self.provision_config_instance.variables["preserve_install"] = True
        self.host.node = Node(binary_path="/fake", data_paths=["/fake1", "/fake2"],
                              name=None, pid=None, telemetry=None, port=None, root_dir=None)
        self.installer.cleanup(self.host)

        self.executor.execute.assert_has_calls([])
