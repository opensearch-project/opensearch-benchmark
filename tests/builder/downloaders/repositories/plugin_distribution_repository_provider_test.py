from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.repositories.plugin_distribution_repository_provider import \
    PluginDistributionRepositoryProvider
from osbenchmark.builder.provision_config import PluginDescriptor


class PluginDistributionRepositoryProviderTest(TestCase):
    def setUp(self):
        self.executor = Mock()

        self.host = None
        self.plugin = PluginDescriptor(name="my-plugin", variables={"distribution": {"repository": "release"}})
        self.plugin_distro_repo_provider = PluginDistributionRepositoryProvider(self.plugin, self.executor)
        self.plugin_distro_repo_provider.repository_url_provider = Mock()

    def test_get_plugin_url(self):
        self.plugin_distro_repo_provider.get_download_url(self.host)
        self.plugin_distro_repo_provider.repository_url_provider.render_url_for_key.assert_has_calls([
            mock.call(None, {"distribution": {"repository": "release"}}, "plugin.my-plugin.release.url", mandatory=False)
        ])
