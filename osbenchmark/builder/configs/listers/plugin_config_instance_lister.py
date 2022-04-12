import logging
import os

from osbenchmark.builder.models.config_instance_types import ConfigInstanceTypes
from osbenchmark.builder.models.plugin_config_instance import PluginConfigInstance
from osbenchmark.utils import io


class PluginConfigInstanceLister:
    def __init__(self, config_path_resolver):
        self.logger = logging.getLogger(__name__)
        self.config_path_resolver = config_path_resolver

    def list_plugin_config_instances(self):
        plugin_config_instances = []
        for config_format_version in ConfigInstanceTypes.PLUGIN.supported_config_format_versions:
            plugins_root_directory = self.config_path_resolver.resolve_config_path(ConfigInstanceTypes.PLUGIN.config_type,
                                                                                   config_format_version)

            plugin_config_instances += self._list_core_plugins(plugins_root_directory, config_format_version)
            plugin_config_instances += self._list_configured_plugins(plugins_root_directory, config_format_version)

        return sorted(plugin_config_instances, key=lambda plugin_config_instance: (
            plugin_config_instance.format_version, plugin_config_instance.name,
            plugin_config_instance.config_names[0] if plugin_config_instance.config_names else None))

    def _list_core_plugins(self, plugins_root_directory, config_format_version):
        core_plugins_path = os.path.join(plugins_root_directory, "core-plugins.txt")

        return self._parse_core_plugins(core_plugins_path, config_format_version) if os.path.exists(core_plugins_path) else []

    def _parse_core_plugins(self, core_plugins_path, config_format_version):
        with open(core_plugins_path, mode="rt", encoding="utf-8") as core_plugins_file:
            return [PluginConfigInstance(name=line.strip().split(",")[0],
                                         format_version=f"v{config_format_version}",
                                         is_core_plugin=True) for line in core_plugins_file if not line.startswith("#")]

    def _list_configured_plugins(self, plugins_root_directory, config_format_version):
        configured_plugins = []

        # each directory is a plugin, each .ini is a config (just go one level deep)
        for plugin_directory in os.listdir(plugins_root_directory):
            plugin_path = os.path.join(plugins_root_directory, plugin_directory)
            if os.path.isdir(plugin_path):
                configured_plugins += self._parse_plugins_in_directory(plugin_path, plugin_directory, config_format_version)

        return configured_plugins

    def _parse_plugins_in_directory(self, plugin_path, plugin_directory, config_format_version):
        return [self._parse_plugin_from_config_file(plugin_config_file, plugin_directory, config_format_version)
                for plugin_config_file in os.listdir(plugin_path) if self._is_config_file(plugin_path, plugin_config_file)]

    def _is_config_file(self, plugin_path, plugin_config_file):
        return os.path.isfile(os.path.join(plugin_path, plugin_config_file)) and io.has_extension(plugin_config_file, ".ini")

    def _parse_plugin_from_config_file(self, plugin_config_file, plugin_directory, config_format_version):
        file_name, _ = io.splitext(plugin_config_file)
        plugin_name = self._file_to_plugin_name(plugin_directory)
        config_name = io.basename(file_name)

        return PluginConfigInstance(name=plugin_name,
                                    format_version=f"v{config_format_version}",
                                    config_names=[config_name])

    def _file_to_plugin_name(self, file_name):
        return file_name.replace("_", "-")
