from osbenchmark.builder.downloaders.downloader import Downloader
from osbenchmark.builder.downloaders.repositories.plugin_distribution_repository_provider import \
    PluginDistributionRepositoryProvider


class PluginDistributionDownloader(Downloader):
    def __init__(self, plugin, executor):
        super().__init__(executor)
        self.plugin = plugin
        self.distribution_repository_provider = PluginDistributionRepositoryProvider(plugin, executor)

    def download(self, host):
        plugin_url = self.distribution_repository_provider.get_download_url(host)
        return {self.plugin.name: plugin_url} if plugin_url else {}
