from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.external_plugin_source_downloader import ExternalPluginSourceDownloader
from osbenchmark.builder.cluster_config import PluginDescriptor


class ExternalPluginSourceDownloaderTest(TestCase):
    def setUp(self):
        self.executor = Mock()
        self.source_repo_provider = Mock()
        self.binary_builder = Mock()

        self.host = None
        self.plugin_config_instance = PluginDescriptor(name="my-plugin", variables={
            "source": {
                "remote": {
                    "repo": {
                        "url": "https//fake.url.com"
                    }
                },
                "revision": "current",
                "build": {
                    "command": "gradle build",
                    "artifact": {
                        "subdir": "plugin/subdir"
                    }
                },
            }
        })
        self.plugin_src_dir = "/fake/dir/for/plugin"

        self.external_plugin_source_downloader = ExternalPluginSourceDownloader(self.plugin_config_instance, self.executor,
                                                                                self.source_repo_provider, self.binary_builder,
                                                                                self.plugin_src_dir)

    def test_download_with_build(self):
        plugin_binary = self.external_plugin_source_downloader.download(self.host)
        self.assertEqual(plugin_binary, {"my-plugin": "file:///fake/dir/for/plugin/plugin/subdir/*.zip"})
        self.source_repo_provider.fetch_repository.assert_has_calls([
            mock.call(self.host, "https//fake.url.com", "current", self.plugin_src_dir)
        ])
        self.binary_builder.build.assert_has_calls([
            mock.call(self.host, ["gradle build"], override_source_directory=self.plugin_src_dir)
        ])

    def test_download_without_build(self):
        self.binary_builder = None

        plugin_binary = self.external_plugin_source_downloader.download(self.host)
        self.assertEqual(plugin_binary, {"my-plugin": "file:///fake/dir/for/plugin/plugin/subdir/*.zip"})
        self.source_repo_provider.fetch_repository.assert_has_calls([
            mock.call(self.host, "https//fake.url.com", "current", self.plugin_src_dir)
        ])
