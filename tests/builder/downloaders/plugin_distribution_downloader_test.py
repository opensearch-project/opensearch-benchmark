from unittest import TestCase
from unittest.mock import Mock

from osbenchmark.builder.downloaders.plugin_distribution_downloader import PluginDistributionDownloader
from osbenchmark.builder.cluster_config import PluginDescriptor


class PluginDistributionDownloaderTest(TestCase):
    def setUp(self):
        self.host = None

        self.executor = Mock()
        self.plugin = PluginDescriptor(name="my plugin")

        self.distribution_repository_provider = Mock()
        self.plugin_distro_downloader = PluginDistributionDownloader(self.plugin, self.executor, self.distribution_repository_provider)

    def test_plugin_url_exists(self):
        self.plugin_distro_downloader.distribution_repository_provider.get_download_url.return_value = "https://fake"

        binaries_map = self.plugin_distro_downloader.download(self.host)
        self.assertEqual(binaries_map, {"my plugin": "https://fake"})

    def test_plugin_url_does_not_exist(self):
        self.plugin_distro_downloader.distribution_repository_provider.get_download_url.return_value = None

        binaries_map = self.plugin_distro_downloader.download(self.host)
        self.assertEqual(binaries_map, {})
