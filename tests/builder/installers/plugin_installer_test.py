from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.installers.plugin_installer import PluginInstaller
from osbenchmark.builder.models.host import Host
from osbenchmark.builder.models.node import Node
from osbenchmark.builder.provision_config import PluginDescriptor


class PluginInstallerTest(TestCase):
    def setUp(self):
        self.node = Node(binary_path="/fake_binary_path", data_paths=["/fake1", "/fake2"], name=None,
                         pid=None, telemetry=None, port=None, root_dir=None, log_path=None, heap_dump_path=None)
        self.host = Host(name="fake", address="10.17.22.23", metadata={}, node=self.node)
        self.binaries = {"unit-test-plugin": "/data/builds/distributions"}
        self.all_node_ips = []

        self.executor = Mock()
        self.plugin = PluginDescriptor(name="unit-test-plugin", config_paths=["default"], variables={"active": True})

        self.plugin_installer = PluginInstaller(self.plugin, self.executor)
        self.plugin_installer.config_applier = Mock()

    def test_plugin_install_with_binary_path(self):
        self.plugin_installer.install(self.host, self.binaries, self.all_node_ips)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "/fake_binary_path/bin/opensearch-plugin install --batch \"/data/builds/distributions\"")
        ])
        self.plugin_installer.config_applier.apply_configs.assert_has_calls([
            mock.call(self.host, self.node, ["default"], {"active": True})
        ])

    def test_plugin_install_without_binary_path(self):
        self.plugin_installer.install(self.host, {}, self.all_node_ips)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "/fake_binary_path/bin/opensearch-plugin install --batch \"unit-test-plugin\"")
        ])
        self.plugin_installer.config_applier.apply_configs.assert_has_calls([
            mock.call(self.host, self.node, ["default"], {"active": True})
        ])

    def test_config_vars_override(self):
        self.plugin_installer.install(self.host, {}, self.all_node_ips, {"my": "override"})

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "/fake_binary_path/bin/opensearch-plugin install --batch \"unit-test-plugin\"")
        ])
        self.plugin_installer.config_applier.apply_configs.assert_has_calls([
            mock.call(self.host, self.node, ["default"], {"my": "override"})
        ])
