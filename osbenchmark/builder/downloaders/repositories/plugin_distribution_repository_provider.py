from osbenchmark.builder.downloaders.repositories.repository_url_provider import RepositoryUrlProvider


class PluginDistributionRepositoryProvider:
    def __init__(self, plugin, executor):
        self.plugin = plugin
        self.repository_url_provider = RepositoryUrlProvider(executor)

    def get_download_url(self, host):
        distribution_repository = self.plugin.variables["distribution"]["repository"]

        default_key = f"plugin.{self.plugin.name}.{distribution_repository}.url"
        return self.repository_url_provider.render_url_for_key(host, self.plugin.variables, default_key, mandatory=False)
