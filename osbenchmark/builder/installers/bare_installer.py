from osbenchmark.builder.installers.installer import Installer
from osbenchmark.builder.installers.preparers.plugin_preparer import PluginPreparer
from osbenchmark.builder.provision_config import BootstrapPhase
from osbenchmark.builder.utils.config_applier import ConfigApplier
from osbenchmark.builder.utils.java_home_resolver import JavaHomeResolver
from osbenchmark.builder.utils.path_manager import PathManager
from osbenchmark.builder.utils.template_renderer import TemplateRenderer


class BareInstaller(Installer):
    def __init__(self, provision_config_instance, executor, preparers):
        super().__init__(executor)
        self.provision_config_instance = provision_config_instance
        if isinstance(preparers, list):
            self.preparers = preparers
        else:
            self.preparers = [preparers]
        self.template_renderer = TemplateRenderer()
        self.path_manager = PathManager(executor)
        self.config_applier = ConfigApplier(executor, self.template_renderer, self.path_manager)
        self.java_home_resolver = JavaHomeResolver(executor)

    def install(self, host, binaries, all_node_ips):
        preparer_to_node = self._prepare_nodes(host, binaries)
        config_vars = self._get_config_vars(host, preparer_to_node, all_node_ips)
        self._apply_configs(host, preparer_to_node, config_vars)
        self._invoke_install_hooks(host, config_vars)

        return self._get_node(preparer_to_node)

    def _prepare_nodes(self, host, binaries):
        preparer_to_node = {}
        for preparer in self.preparers:
            preparer_to_node[preparer] = preparer.prepare(host, binaries)

        return preparer_to_node

    def _get_config_vars(self, host, preparer_to_node, all_node_ips):
        config_vars = {}

        for preparer, node in preparer_to_node.items():
            config_vars.update(preparer.get_config_vars(host, node, all_node_ips))

        plugin_names = [preparer.get_plugin_name() for preparer in self.preparers if isinstance(preparer, PluginPreparer)]
        if plugin_names:
            # as a safety measure, prevent the cluster to startup if something went wrong during plugin installation
            config_vars["cluster_settings"] = {}
            config_vars["cluster_settings"]["plugin.mandatory"] = plugin_names

        return config_vars

    def _apply_configs(self, host, preparer_to_node, config_vars):
        for preparer, node in preparer_to_node.items():
            self.config_applier.apply_configs(host, node, preparer.get_config_paths(), config_vars)

    def _invoke_install_hooks(self, host, config_vars):
        _, java_home = self.java_home_resolver.resolve_java_home(host, self.provision_config_instance)

        env = {}
        if java_home:
            env["JAVA_HOME"] = java_home

        config_vars_copy = config_vars.copy()
        for preparer in self.preparers:
            preparer.invoke_install_hook(host, BootstrapPhase.post_install, config_vars_copy, env)

    def _get_node(self, preparer_to_node):
        nodes_list = list(filter(lambda node: node is not None, preparer_to_node.values()))

        assert len(nodes_list) == 1, f"Exactly one node must be provisioned per host, but found nodes: {nodes_list}"

        return nodes_list[0]

    def cleanup(self, host):
        for preparer in self.preparers:
            preparer.cleanup(host)
