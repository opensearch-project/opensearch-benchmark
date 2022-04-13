from osbenchmark.builder.downloaders.downloader import Downloader


class PluginDistributionDownloader(Downloader):
    def __init__(self, plugin, executor, distribution_repository_provider):
        super().__init__(executor)
        self.plugin = plugin
        self.distribution_repository_provider = distribution_repository_provider

    def download(self, host):
        plugin_url = self.distribution_repository_provider.get_download_url(host)
        return {self.plugin.name: plugin_url} if plugin_url else {}
