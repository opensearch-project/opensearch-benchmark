import logging
import os

from osbenchmark.builder.downloaders.downloader import Downloader
from osbenchmark.builder.utils.binary_keys import BinaryKeys


class OpenSearchSourceDownloader(Downloader):
    def __init__(self, provision_config_instance, executor, source_repository_provider, binary_builder, template_renderer,
                 artifact_variables_provider):
        super().__init__(executor)
        self.logger = logging.getLogger(__name__)
        self.provision_config_instance = provision_config_instance
        self.source_repository_provider = source_repository_provider
        self.binary_builder = binary_builder
        self.template_renderer = template_renderer
        self.artifact_variables_provider = artifact_variables_provider

    def download(self, host):
        opensearch_source_path = self._get_opensearch_source_path()
        self._fetch(host, opensearch_source_path)

        artifact_variables = self.artifact_variables_provider.get_artifact_variables(host)
        self._prepare(host, artifact_variables)

        return {BinaryKeys.OPENSEARCH: self._get_zip_path(opensearch_source_path, artifact_variables)}

    def _get_opensearch_source_path(self):
        node_root_dir = self.provision_config_instance.variables["source"]["root"]["dir"]
        opensearch_source_subdir = self.provision_config_instance.variables["source"]["opensearch"]["subdir"]
        return os.path.join(node_root_dir, opensearch_source_subdir)

    def _fetch(self, host, opensearch_source_path):
        plugin_remote_url = self.provision_config_instance.variables["source"]["remote"]["repo"]["url"]
        plugin_revision = self.provision_config_instance.variables["source"]["revision"]

        self.source_repository_provider.fetch_repository(host, plugin_remote_url, plugin_revision, opensearch_source_path)

    def _prepare(self, host, artifact_variables):
        clean_command_template = self.provision_config_instance.variables["source"]["clean"]["command"]
        build_command_template = self.provision_config_instance.variables["source"]["build"]["command"]

        if self.binary_builder:
            self.binary_builder.build(host, [
                self.template_renderer.render_template_string(clean_command_template, artifact_variables),
                self.template_renderer.render_template_string(build_command_template, artifact_variables)
            ])

    def _get_zip_path(self, opensearch_source_path, artifact_variables):
        artifact_path_pattern_template = self.provision_config_instance.variables["source"]["artifact_path_pattern"]
        artifact_path_pattern = self.template_renderer.render_template_string(artifact_path_pattern_template, artifact_variables)

        return os.path.join(opensearch_source_path, artifact_path_pattern)
