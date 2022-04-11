from osbenchmark.builder.downloaders.downloader import Downloader


class CorePluginSourceDownloader(Downloader):
    def __init__(self, plugin, executor, source_repository_provider, binary_builder, opensearch_source_dir):
        super().__init__(executor)
        self.plugin = plugin
        self.source_repository_provider = source_repository_provider
        self.binary_builder = binary_builder
        self.opensearch_source_dir = opensearch_source_dir

    def download(self, host):
        self._fetch(host)
        self._prepare(host)

        return {self.plugin.name: self._get_zip_path()}

    def _fetch(self, host):
        return self.source_repository_provider.fetch_repository(host, None, "current", self.opensearch_source_dir)

    def _prepare(self, host):
        if self.binary_builder:
            self.binary_builder.build(host, [f"gradlew :plugins:{self.plugin.name}:assemble"])

    def _get_zip_path(self):
        return f"file://{self.opensearch_source_dir}/plugins/{self.plugin.name}/build/distributions/*.zip"
