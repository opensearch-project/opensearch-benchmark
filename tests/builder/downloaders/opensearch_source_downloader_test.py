from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.opensearch_source_downloader import OpenSearchSourceDownloader
from osbenchmark.builder.provision_config import ProvisionConfigInstance
from osbenchmark.builder.utils.binary_keys import BinaryKeys


class OpenSearchSourceDownloaderTest(TestCase):
    def setUp(self):
        self.host = None

        self.executor = Mock()
        self.source_repository_provider = Mock()
        self.binary_builder = Mock()
        self.template_renderer = Mock()
        self.artifact_variables_provider = Mock()

        self.cluster_config = ProvisionConfigInstance(names="fake", root_path="also fake", config_paths="fake2", variables={
            "source": {
                "root": {
                    "dir": "/fake/dir/for/source"
                },
                "opensearch": {
                    "subdir": "opensearch_sub-dir"
                },
                "remote": {
                    "repo": {
                        "url": "https://git.remote.fake"
                    }
                },
                "revision": "current",
                "artifact_path_pattern": "{{OSNAME}}.tar.gz",
                "build": {
                    "command": "gradle build"
                },
                "clean": {
                    "command": "gradle clean"
                }
            }
        })

        self.opensearch_source_downloader = OpenSearchSourceDownloader(self.cluster_config, self.executor,
                                                                       self.source_repository_provider, self.binary_builder,
                                                                       self.template_renderer, self.artifact_variables_provider)

    def test_download(self):
        self.artifact_variables_provider.get_artifact_variables.return_value = {"OSNAME": "fake_OS"}
        self.template_renderer.render_template_string.side_effect = ["fake clean", "fake build", "fake artifact path"]

        opensearch_binary = self.opensearch_source_downloader.download(self.host)
        self.assertEqual(opensearch_binary, {BinaryKeys.OPENSEARCH: "/fake/dir/for/source/opensearch_sub-dir/fake artifact path"})
        self.source_repository_provider.fetch_repository.assert_has_calls([
            mock.call(self.host, "https://git.remote.fake", "current", "/fake/dir/for/source/opensearch_sub-dir")
        ])
        self.binary_builder.build.assert_has_calls([
            mock.call(self.host, ["fake clean", "fake build"])
        ])
        self.artifact_variables_provider.get_artifact_variables.assert_has_calls([
            mock.call(self.host)
        ])
        self.template_renderer.render_template_string.assert_has_calls([
            mock.call("gradle clean", {"OSNAME": "fake_OS"}),
            mock.call("gradle build", {"OSNAME": "fake_OS"}),
            mock.call("{{OSNAME}}.tar.gz", {"OSNAME": "fake_OS"})
        ])
