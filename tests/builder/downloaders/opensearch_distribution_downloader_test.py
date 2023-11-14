from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.opensearch_distribution_downloader import OpenSearchDistributionDownloader
from osbenchmark.builder.provision_config import ProvisionConfigInstance
from osbenchmark.exceptions import ExecutorError


class OpenSearchDistributionDownloaderTest(TestCase):
    def setUp(self):
        self.host = None

        self.executor = Mock()
        self.cluster_config = ProvisionConfigInstance(names="fake", root_path="also fake", config_paths="fake2", variables={
            "node": {
                "root": {
                    "dir": "/fake/dir/for/download"
                }
            },
            "distribution": {
                "version": "1.2.3"
            }
        })

        self.path_manager = Mock()
        self.distribution_repository_provider = Mock()
        self.os_distro_downloader = OpenSearchDistributionDownloader(self.cluster_config, self.executor, self.path_manager,
                                                                     self.distribution_repository_provider)


        self.os_distro_downloader.distribution_repository_provider.get_download_url.return_value = "https://fake/download.tar.gz"
        self.os_distro_downloader.distribution_repository_provider.get_file_name_from_download_url.return_value = "my-distro"
        self.os_distro_downloader.distribution_repository_provider.is_cache_enabled.return_value = True

    def test_download_distro(self):
        # Check if file exists, download via curl
        self.executor.execute.side_effect = [ExecutorError("file doesn't exist"), None]

        binary_map = self.os_distro_downloader.download(self.host)
        self.assertEqual(binary_map, {"opensearch": "/fake/dir/for/download/distributions/my-distro"})

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "test -f /fake/dir/for/download/distributions/my-distro"),
            mock.call(self.host, "curl -o /fake/dir/for/download/distributions/my-distro https://fake/download.tar.gz")
        ])

    def test_download_distro_exists_and_cache_enabled(self):
        # Check if file exists, download via curl
        self.executor.execute.side_effect = [None]

        binary_map = self.os_distro_downloader.download(self.host)
        self.assertEqual(binary_map, {"opensearch": "/fake/dir/for/download/distributions/my-distro"})

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "test -f /fake/dir/for/download/distributions/my-distro")
        ])

    def test_download_distro_exists_and_cache_disabled(self):
        self.os_distro_downloader.distribution_repository_provider.is_cache_enabled.return_value = False
        # Check if file exists, download via curl
        self.executor.execute.side_effect = [None, None]

        binary_map = self.os_distro_downloader.download(self.host)
        self.assertEqual(binary_map, {"opensearch": "/fake/dir/for/download/distributions/my-distro"})

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "test -f /fake/dir/for/download/distributions/my-distro"),
            mock.call(self.host, "curl -o /fake/dir/for/download/distributions/my-distro https://fake/download.tar.gz")
        ])
