from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.configs.utils.config_path_resolver import ConfigPathResolver
from osbenchmark.exceptions import SystemSetupError


class ConfigPathResolverTest(TestCase):
    def setUp(self):
        self.config_type = "red"
        self.config_format_version = "36"

        self.cfg = Mock()
        self.config_path_resolver = ConfigPathResolver(self.cfg)

    @mock.patch('os.path.exists')
    def test_cluster_config_path_defined(self, path_exists):
        path_exists.return_value = True
        # opts("builder", "provision_config.path")
        self.cfg.opts.return_value = "/path/to/configs"

        config_path = self.config_path_resolver.resolve_config_path(self.config_type, self.config_format_version)
        self.assertEqual(config_path, "/path/to/configs/red/v36")

    @mock.patch('osbenchmark.utils.git.fetch')
    @mock.patch('osbenchmark.utils.repo.BenchmarkRepository')
    @mock.patch('osbenchmark.utils.repo.BenchmarkRepository.set_provision_configs_dir')
    @mock.patch('os.path.exists')
    def test_cluster_config_path_not_defined(self, path_exists, set_repo, benchmark_repo, git_fetch):
        path_exists.return_value = True

        # opts("builder", "provision_config.path"), opts("builder", "distribution.version"), opts("builder", "repository.name"),
        # opts("builder", "repository.revision"), opts("system", "offline.mode"), opts("provision_configs", "%s.dir" % repo_name),
        # opts("node", "root.dir"), opts("builder", "provision_config.repository.dir")
        self.cfg.opts.side_effect = [None, "1.0", "fake-repo", "fake-revision", False, "fake-repo.dir", "/root_dir", "repo_dir"]

        config_path = self.config_path_resolver.resolve_config_path(self.config_type, self.config_format_version)
        self.assertEqual(config_path, "/root_dir/repo_dir/fake-repo/red/v36")

    @mock.patch('os.path.exists')
    def test_cluster_config_path_does_not_exist(self, path_exists):
        path_exists.return_value = False
        # opts("builder", "provision_config.path")
        self.cfg.opts.return_value = "/path/to/configs"

        with self.assertRaises(SystemSetupError):
            self.config_path_resolver.resolve_config_path(self.config_type, self.config_format_version)
