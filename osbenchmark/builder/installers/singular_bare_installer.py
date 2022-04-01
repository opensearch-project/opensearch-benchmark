from osbenchmark.builder.installers.installer import Installer
from osbenchmark.builder.provision_config import BootstrapPhase
from osbenchmark.builder.utils.config_applier import ConfigApplier
from osbenchmark.builder.utils.java_home_resolver import JavaHomeResolver
from osbenchmark.builder.utils.path_manager import PathManager
from osbenchmark.builder.utils.template_renderer import TemplateRenderer


class SingularBareInstaller(Installer):
    def __init__(self, config, executor, preparer):
        super().__init__(executor)
        self.config = config
        self.preparer = preparer
        self.template_renderer = TemplateRenderer()
        self.path_manager = PathManager(executor)
        self.config_applier = ConfigApplier(executor, self.template_renderer, self.path_manager)
        self.java_home_resolver = JavaHomeResolver(executor)

    def install(self, host, binaries, all_node_ips):
        node = self.preparer.prepare(host, binaries)
        config_vars = self.preparer.get_config_vars(host, node, all_node_ips)
        self.config_applier.apply_configs(host, node, self.config.config_paths, config_vars)
        self._install_invoke_hook(host, config_vars)

        return node

    def _install_invoke_hook(self, host, config_vars):
        _, java_home = self.java_home_resolver.resolve_java_home(host, self.config)

        env = {}
        if java_home:
            env["JAVA_HOME"] = java_home

        self.preparer.invoke_install_hook(host, BootstrapPhase.post_install, config_vars.copy(), env)

    def cleanup(self, host):
        self.preparer.cleanup(host)
