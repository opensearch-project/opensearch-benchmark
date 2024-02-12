from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.repositories.opensearch_distribution_repository_provider import \
    OpenSearchDistributionRepositoryProvider
from osbenchmark.builder.provision_config import ProvisionConfigInstance


class OpenSearchDistributionRepositoryProviderTest(TestCase):
    def setUp(self):
        self.host = None
        self.cluster_config = ProvisionConfigInstance(names=None, config_paths=None, root_path=None, variables={
            "system": {
                "runtime": {
                    "jdk": {
                        "bundled": True
                    }
                }
            },
            "distribution": {
                "repository": "release",
                "release": {
                    "cache": True
                }
            }
        })
        self.repository_url_provider = Mock()
        self.os_distro_repo_provider = OpenSearchDistributionRepositoryProvider(self.cluster_config,
                                                                                self.repository_url_provider)


    def test_get_url_bundled_jdk(self):
        self.os_distro_repo_provider.get_download_url(self.host)
        self.os_distro_repo_provider.repository_url_provider.render_url_for_key.assert_has_calls([
            mock.call(None, self.cluster_config.variables, "distribution.jdk.bundled.release_url")
        ])

    def test_get_url_unbundled_jdk(self):
        self.cluster_config.variables["system"]["runtime"]["jdk"]["bundled"] = False

        self.os_distro_repo_provider.get_download_url(self.host)
        self.os_distro_repo_provider.repository_url_provider.render_url_for_key.assert_has_calls([
            mock.call(None, self.cluster_config.variables, "distribution.jdk.unbundled.release_url")
        ])

    def test_get_file_name(self):
        file_name = self.os_distro_repo_provider.get_file_name_from_download_url(
            "https://artifacts.opensearch.org/releases/bundle/opensearch/1.2.3/opensearch-1.2.3-linux-arm64.tar.gz")

        self.assertEqual(file_name, "opensearch-1.2.3-linux-arm64.tar.gz")

    def test_is_cache_enabled_true(self):
        is_cache_enabled = self.os_distro_repo_provider.is_cache_enabled()
        self.assertEqual(is_cache_enabled, True)

    def test_is_cache_enabled_false(self):
        self.cluster_config.variables["distribution"]["release"]["cache"] = False
        is_cache_enabled = self.os_distro_repo_provider.is_cache_enabled()
        self.assertEqual(is_cache_enabled, False)
