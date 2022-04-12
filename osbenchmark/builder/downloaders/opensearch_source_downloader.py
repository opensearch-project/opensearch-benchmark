import logging
import os

from osbenchmark.builder.downloaders.downloader import Downloader


class OpenSearchSourceDownloader(Downloader):
    def __init__(self, provision_config_instance, executor, source_repository_provider):
        super().__init__(executor)
        self.logger = logging.getLogger(__name__)
        self.provision_config_instance = provision_config_instance
        self.source_repository_provider = source_repository_provider

    def download(self, host):
        self._fetch(host)

    def _fetch(self, host):
        plugin_remote_url = self.provision_config_instance.variables["source"]["remote"]["repo"]["url"]
        plugin_revision = self.provision_config_instance.variables["source"]["revision"]

        node_root_dir = self.provision_config_instance.variables["node"]["src"]["root"]["dir"]
        opensearch_source_subdir = self.provision_config_instance.variables["source"]["opensearch"]["src"]["subdir"]

        opensearch_source_path = os.path.join(node_root_dir, opensearch_source_subdir)

        self.source_repository_provider.fetch_repository(host, plugin_remote_url, plugin_revision, opensearch_source_path)

    def _prepare(self, host):