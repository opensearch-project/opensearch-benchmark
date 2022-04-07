from unittest import TestCase
from unittest.mock import Mock

from osbenchmark.builder.downloaders.core_plugin_source_downloader import CorePluginSourceDownloader
from osbenchmark.builder.provision_config import PluginDescriptor


class CorePluginSourceDownloaderTest(TestCase):
    def setUp(self):
        self.host = None

        self.executor = Mock()
        self.plugin = PluginDescriptor(name="my-plugin")
        self.builder = Mock()
        self.opensearch_source_dir = "/fake/path"

        self.source_downloader = CorePluginSourceDownloader(self.plugin, self.executor, self.builder, self.opensearch_source_dir)
        self.source_downloader.source_repository_provider = Mock()

    def test_download(self):
        plugin_binary = self.source_downloader.download(self.host)
        self.assertEqual(plugin_binary, {"my-plugin": "file:///fake/path/plugins/my-plugin/build/distributions/*.zip"})
