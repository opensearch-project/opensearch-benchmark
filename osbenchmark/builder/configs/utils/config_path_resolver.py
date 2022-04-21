import os

from osbenchmark.exceptions import SystemSetupError
from osbenchmark.utils.repo import BenchmarkRepository


class ConfigPathResolver:
    def __init__(self, cfg):
        self.cfg = cfg

    def resolve_config_path(self, config_type, config_version):
        config_root_path = self._get_config_root_path()
        root_path = os.path.join(config_root_path, config_type, f"v{config_version}")
        if not os.path.exists(root_path):
            raise SystemSetupError(f"Path {root_path} for {config_type} does not exist.")

        return root_path

    def _get_config_root_path(self):
        root_path = self.cfg.opts("builder", "provision_config.path", mandatory=False)
        if root_path:
            return root_path
        else:
            distribution_version = self.cfg.opts("builder", "distribution.version", mandatory=False)
            repo_name = self.cfg.opts("builder", "repository.name")
            repo_revision = self.cfg.opts("builder", "repository.revision")
            offline = self.cfg.opts("system", "offline.mode")
            default_directory = self.cfg.opts("provision_configs", "%s.dir" % repo_name, mandatory=False)
            root = self.cfg.opts("node", "root.dir")
            provision_config_repositories = self.cfg.opts("builder", "provision_config.repository.dir")
            provision_configs_dir = os.path.join(root, provision_config_repositories)

            current_provision_config_repo = BenchmarkRepository(
                default_directory, provision_configs_dir,
                repo_name, "provision_configs", offline)

            current_provision_config_repo.set_provision_configs_dir(repo_revision, distribution_version, self.cfg)
            return current_provision_config_repo.repo_dir
