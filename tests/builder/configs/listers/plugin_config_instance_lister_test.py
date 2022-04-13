import os
from unittest import TestCase
from unittest.mock import Mock

from osbenchmark.builder.configs.listers.plugin_config_instance_lister import PluginConfigInstanceLister
from osbenchmark.builder.models.plugin_config_instance import PluginConfigInstance


class PluginConfigInstanceListerTest(TestCase):
    def setUp(self):
        self.config_path_resolver = Mock()
        self.plugin_config_instance_lister = PluginConfigInstanceLister(self.config_path_resolver)

        builder_tests_root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.config_path_resolver.resolve_config_path.return_value = os.path.join(builder_tests_root_dir, "data", "plugins", "v1")

    def test_list_plugin_config_instances(self):
        plugin_config_instances = self.plugin_config_instance_lister.list_plugin_config_instances()
        print(plugin_config_instances)

        self.assertEqual(plugin_config_instances, [
            PluginConfigInstance(name="complex-plugin", format_version="v1", config_names=["config-a"]),
            PluginConfigInstance(name="complex-plugin", format_version="v1", config_names=["config-b"]),
            PluginConfigInstance(name="my-analysis-plugin", format_version="v1", is_core_plugin=True),
            PluginConfigInstance(name="my-core-plugin-with-config", format_version="v1", is_core_plugin=True),
            PluginConfigInstance(name="my-ingest-plugin", format_version="v1", is_core_plugin=True)
        ])
