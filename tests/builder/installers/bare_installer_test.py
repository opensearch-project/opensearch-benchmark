from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.installers.bare_installer import BareInstaller
from osbenchmark.builder.installers.preparers.plugin_preparer import PluginPreparer
from osbenchmark.builder.models.host import Host
from osbenchmark.builder.provision_config import ProvisionConfigInstance, BootstrapPhase


class BareInstallerTests(TestCase):
    def setUp(self):
        self.host = Host(name="fake", address="10.17.22.23", metadata={}, node=None)
        self.binaries = {}
        self.all_node_ips = ["10.17.22.22", "10.17.22.23"]

        self.test_execution_root = "fake_root"
        self.node_id = "abdefg"
        self.cluster_name = "my-cluster"

        self.executor = Mock()
        self.preparer = Mock()
        self.preparer2 = Mock()

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
        self.installer = BareInstaller(self.provision_config_instance, self.executor, self.preparer)
        self.installer.config_applier = Mock()
        self.installer.java_home_resolver = Mock()

        self.preparer.prepare.return_value = "fake node"
        self.preparer.get_config_vars.return_value = {"fake": "config"}
        self.preparer.get_config_paths.return_value = ["/tmp"]
        self.preparer2.prepare.return_value = "second node"
        self.preparer2.get_config_vars.return_value = {"new": "var"}
        self.preparer2.get_config_paths.return_value = ["/fake"]
        self.installer.java_home_resolver.resolve_java_home.return_value = (None, "/path/to/java/home")

    def test_install_only_opensearch(self):
        node = self.installer.install(self.host, self.binaries, self.all_node_ips)
        self.assertEqual(node, "fake node")

        self.preparer.prepare.assert_has_calls([
            mock.call(self.host, self.binaries)
        ])
        self.preparer.get_config_vars.assert_has_calls([
            mock.call(self.host, "fake node", self.all_node_ips)
        ])
        self.installer.config_applier.apply_configs.assert_has_calls([
            mock.call(self.host, "fake node", ["/tmp"], {"fake": "config"})
        ])
        self.installer.java_home_resolver.resolve_java_home.assert_has_calls([
            mock.call(self.host, self.provision_config_instance)
        ])
        self.preparer.invoke_install_hook.assert_has_calls([
            mock.call(self.host, BootstrapPhase.post_install, {"fake": "config"}, {"JAVA_HOME": "/path/to/java/home"})
        ])

    def test_install_no_java_home(self):
        self.installer.java_home_resolver.resolve_java_home.return_value = (None, None)

        self.installer.install(self.host, self.binaries, self.all_node_ips)

        self.preparer.invoke_install_hook.assert_has_calls([
            mock.call(self.host, BootstrapPhase.post_install, {"fake": "config"}, {})
        ])

    def test_multiple_nodes_installed(self):
        self.installer.preparers = [self.preparer, self.preparer2]

        with self.assertRaises(AssertionError):
            self.installer.install(self.host, self.binaries, self.all_node_ips)

    def test_no_nodes_installed(self):
        self.preparer.prepare.return_value = None

        with self.assertRaises(AssertionError):
            self.installer.install(self.host, self.binaries, self.all_node_ips)

    def test_opensearch_and_plugin_installation(self):
        self.preparer2.prepare.return_value = None
        self.preparer2.get_plugin_name.return_value = "my-plugin"
        self.preparer2.mock_add_spec(PluginPreparer)
        self.installer.preparers = [self.preparer, self.preparer2]

        node = self.installer.install(self.host, self.binaries, self.all_node_ips)
        self.assertEqual(node, "fake node")

        self.preparer.prepare.assert_has_calls([
            mock.call(self.host, self.binaries)
        ])
        self.preparer2.prepare.assert_has_calls([
            mock.call(self.host, self.binaries)
        ])
        self.preparer.get_config_vars.assert_has_calls([
            mock.call(self.host, "fake node", self.all_node_ips)
        ])
        self.preparer2.get_config_vars.assert_has_calls([
            mock.call(self.host, None, self.all_node_ips)
        ])

        expected_config_vars = {"fake": "config", "new": "var", "cluster_settings": {"plugin.mandatory": ["my-plugin"]}}
        self.installer.config_applier.apply_configs.assert_has_calls([
            mock.call(self.host, "fake node", ["/tmp"], expected_config_vars),
            mock.call(self.host, None, ["/fake"], expected_config_vars)
        ])
        self.installer.java_home_resolver.resolve_java_home.assert_has_calls([
            mock.call(self.host, self.provision_config_instance)
        ])
        self.preparer.invoke_install_hook.assert_has_calls([
            mock.call(self.host, BootstrapPhase.post_install, expected_config_vars, {"JAVA_HOME": "/path/to/java/home"})
        ])
        self.preparer2.invoke_install_hook.assert_has_calls([
            mock.call(self.host, BootstrapPhase.post_install, expected_config_vars, {"JAVA_HOME": "/path/to/java/home"})
        ])
