from osbenchmark.builder.downloaders.downloader import Downloader


class ExternalPluginSourceDownloader(Downloader):
    def __init__(self, plugin_config_instance, executor, source_repository_provider, binary_builder, plugin_source_directory):
        super().__init__(executor)
        self.plugin_config_instance = plugin_config_instance
        self.source_repository_provider = source_repository_provider
        self.binary_builder = binary_builder
        self.plugin_source_directory = plugin_source_directory

    def download(self, host):
        self._fetch(host)
        self._prepare(host)

        return {self.plugin_config_instance.name: self._get_zip_path()}

    def _fetch(self, host):
        plugin_remote_url = self.plugin_config_instance.variables["source"]["remote"]["repo"]["url"]
        plugin_revision = self.plugin_config_instance.variables["source"]["revision"]
        self.source_repository_provider.fetch_repository(host, plugin_remote_url, plugin_revision, self.plugin_source_directory)

    def _prepare(self, host):
        if self.binary_builder:
            build_command = self.plugin_config_instance.variables["source"]["build"]["command"]
            self.binary_builder.build(host, [build_command], override_source_directory=self.plugin_source_directory)

    def _get_zip_path(self):
        artifact_path = self.plugin_config_instance.variables["source"]["build"]["artifact"]["subdir"]
        return f"file://{self.plugin_source_directory}/{artifact_path}/*.zip"
