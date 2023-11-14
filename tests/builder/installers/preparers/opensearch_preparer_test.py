import os
from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.installers.preparers.opensearch_preparer import OpenSearchPreparer
from osbenchmark.builder.models.host import Host
from osbenchmark.builder.models.node import Node
from osbenchmark.builder.provision_config import ProvisionConfigInstance
from osbenchmark.builder.utils.binary_keys import BinaryKeys


class OpenSearchPreparerTests(TestCase):
    def setUp(self):
        self.node_id = "abdefg"
        self.node = Node(binary_path="/fake_binary_path", data_paths=["/fake1", "/fake2"],
                         name=self.node_id, pid=None, telemetry=None, port=9200, root_dir=None,
                         log_path="/fake/logpath", heap_dump_path="/fake/heap")
        self.host = Host(name="fake", address="10.17.22.23", metadata={}, node=None)
        self.binaries = {BinaryKeys.OPENSEARCH: "/data/builds/distributions"}
        self.all_node_ips = ["10.17.22.22", "10.17.22.23"]

        self.test_execution_root = "fake_root"
        self.cluster_name = "my-cluster"

        self.executor = Mock()
        self.hook_handler_class = Mock()

        self.cluster_config = ProvisionConfigInstance(
            names="defaults",
            root_path="fake",
            config_paths=["/tmp"],
            variables={
                "test_execution_root": self.test_execution_root,
                "cluster_name": self.cluster_name,
                "node": {
                    "port": "9200"
                }
            }
        )
        self.preparer = OpenSearchPreparer(self.cluster_config, self.executor, self.hook_handler_class)
        self.preparer.path_manager = Mock()

    @mock.patch("uuid.uuid4")
    def test_prepare(self, uuid):
        # extract OpenSearch binary
        self.executor.execute.side_effect = [None]
        uuid.return_value = self.node_id

        node = self.preparer.prepare(self.host, self.binaries)
        self.assertEqual(node.binary_path, os.path.join(self.test_execution_root, self.node_id, "install/opensearch*"))
        self.assertEqual(node.data_paths, [os.path.join(self.test_execution_root, self.node_id, "install/opensearch*/data")])
        self.assertEqual(node.port, 9200)
        self.assertEqual(node.root_dir, os.path.join(self.test_execution_root, self.node_id))
        self.assertEqual(node.name, self.node_id)

    def test_config_vars(self):
        config_vars = self.preparer.get_config_vars(self.host, self.node, self.all_node_ips)

        self.assertEqual({
            "cluster_name": self.cluster_name,
            "node_name": self.node_id,
            "data_paths": "/fake1",
            "log_path": "/fake/logpath",
            "heap_dump_path": "/fake/heap",
            "node_ip": "10.17.22.23",
            "network_host": "10.17.22.23",
            "http_port": "9200",
            "transport_port": "9300",
            "all_node_ips": "[\"10.17.22.22\",\"10.17.22.23\"]",
            "minimum_master_nodes": 2,
            "install_root_path": "/fake_binary_path",
            "node": {"port": "9200"},
            "test_execution_root": self.test_execution_root
        }, config_vars)
