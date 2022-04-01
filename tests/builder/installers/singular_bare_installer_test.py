from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.installers.singular_bare_installer import SingularBareInstaller
from osbenchmark.builder.models.host import Host
from osbenchmark.builder.provision_config import ProvisionConfigInstance, BootstrapPhase


class SingularBareInstallerTests(TestCase):
    def setUp(self):
        self.host = Host(name="fake", address="10.17.22.23", metadata={}, node=None)
        self.binaries = {}
        self.all_node_ips = ["10.17.22.22", "10.17.22.23"]

        self.test_execution_root = "fake_root"
        self.node_id = "abdefg"
        self.cluster_name = "my-cluster"

        self.executor = Mock()
        self.preparer = Mock()

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
        self.installer = SingularBareInstaller(self.provision_config_instance, self.executor, self.preparer)
        self.installer.config_applier = Mock()
        self.installer.java_home_resolver = Mock()

        self.preparer.prepare.return_value = "fake node"
        self.preparer.get_config_vars.return_value = {"fake": "config"}

    def test_install(self):
        self.installer.java_home_resolver.resolve_java_home.return_value = (None, "/path/to/java/home")

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
