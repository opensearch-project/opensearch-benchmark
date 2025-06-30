from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.installers.preparers.plugin_preparer import PluginPreparer
from osbenchmark.builder.models.host import Host
from osbenchmark.builder.models.node import Node
from osbenchmark.builder.cluster_config import PluginDescriptor


class PluginPreparerTest(TestCase):
    def setUp(self):
        self.node = Node(binary_path="/fake_binary_path", data_paths=["/fake1", "/fake2"], name=None,
                         pid=None, telemetry=None, port=None, root_dir=None, log_path=None, heap_dump_path=None)
        self.host = Host(name="fake", address="10.17.22.23", metadata={}, node=self.node)
        self.binaries = {"unit-test-plugin": "/data/builds/distributions"}
        self.all_node_ips = []

        self.executor = Mock()
        self.hook_handler_class = Mock()
        self.plugin = PluginDescriptor(name="unit-test-plugin", config_paths=["default"], variables={"active": True})

        self.plugin_preparer = PluginPreparer(self.plugin, self.executor, self.hook_handler_class)

    def test_plugin_install_with_binary_path(self):
        self.plugin_preparer.prepare(self.host, self.binaries)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "/fake_binary_path/bin/opensearch-plugin install --batch \"/data/builds/distributions\"")
        ])

    def test_plugin_install_without_binary_path(self):
        self.plugin_preparer.prepare(self.host, {})

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "/fake_binary_path/bin/opensearch-plugin install --batch \"unit-test-plugin\"")
        ])

    def test_config_vars(self):
        config_vars = self.plugin_preparer.get_config_vars(self.host, self.node, self.all_node_ips)

        self.assertEqual(config_vars, {"active": True})
