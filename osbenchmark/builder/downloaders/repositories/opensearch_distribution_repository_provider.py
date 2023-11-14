import logging

from osbenchmark.utils import convert


class OpenSearchDistributionRepositoryProvider:
    def __init__(self, cluster_config, repository_url_provider):
        self.logger = logging.getLogger(__name__)
        self.cluster_config = cluster_config
        self.repository_url_provider = repository_url_provider

    def get_download_url(self, host):
        is_runtime_jdk_bundled = self.cluster_config.variables["system"]["runtime"]["jdk"]["bundled"]
        distribution_repository = self.cluster_config.variables["distribution"]["repository"]

        self.logger.info("runtime_jdk_bundled? [%s]", is_runtime_jdk_bundled)
        if is_runtime_jdk_bundled:
            url_key = f"distribution.jdk.bundled.{distribution_repository}_url"
        else:
            url_key = f"distribution.jdk.unbundled.{distribution_repository}_url"

        self.logger.info("key: [%s]", url_key)
        return self.repository_url_provider.render_url_for_key(host, self.cluster_config.variables, url_key)

    def get_file_name_from_download_url(self, download_url):
        return download_url[download_url.rfind("/") + 1:]

    def is_cache_enabled(self):
        distribution_repository = self.cluster_config.variables["distribution"]["repository"]
        is_cache_enabled = self.cluster_config.variables["distribution"][distribution_repository]["cache"]

        return convert.to_bool(is_cache_enabled)
