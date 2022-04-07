from osbenchmark.builder.downloaders.downloader import Downloader
from osbenchmark.builder.downloaders.repositories.source_repository_provider import SourceRepositoryProvider


class CorePluginSourceDownloader(Downloader):
    def __init__(self, plugin, executor, builder, opensearch_source_dir):
        super().__init__(executor)
        self.plugin = plugin
        self.source_repository_provider = SourceRepositoryProvider(executor, "OpenSearch")
        self.builder = builder
        self.opensearch_source_dir = opensearch_source_dir

    def download(self, host):
        self._fetch(host)
        self._prepare(host)

        return {self.plugin.name: self._get_zip_path()}

    def _fetch(self, host):
        return self.source_repository_provider.fetch_repository(host, None, "current", self.opensearch_source_dir)

    def _prepare(self, host):
        if self.builder:
            self.builder.build(host, [f"gradlew :plugins:{self.plugin.name}:assemble"])

    def _get_zip_path(self):
        return f"file://{self.opensearch_source_dir}/plugins/{self.plugin.name}/build/distributions/*.zip"
