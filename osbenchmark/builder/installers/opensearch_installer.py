import logging
import os
import uuid

from osbenchmark.builder.installers.installer import Installer
from osbenchmark.builder.models.node import Node


class OpenSearchInstaller(Installer):
    OPENSEARCH_BINARY_KEY = "opensearch"

    def __init__(self, provision_config_instance, executor):
        super().__init__(executor)
        self.logger = logging.getLogger(__name__)
        self.provision_config_instance = provision_config_instance

    def install(self, host, binaries):
        node = self._create_node()
        self._prepare_node(host, node, binaries[OpenSearchInstaller.OPENSEARCH_BINARY_KEY])

        return node

    def _create_node(self):
        node_name = str(uuid.uuid4())
        node_port = int(self.provision_config_instance.variables["node"]["port"])
        node_root_dir = os.path.join(self.provision_config_instance.variables["test_execution_root"], node_name)
        node_binary_path = os.path.join(node_root_dir, "install")

        return Node(name=node_name,
                    port=node_port,
                    pid=None,
                    root_dir=node_root_dir,
                    binary_path=node_binary_path,
                    data_paths=None,
                    telemetry=None)

    def _prepare_node(self, host, node, binary):
        self._prepare_directories(host, node)
        self._extract_opensearch(host, node, binary)
        self._update_node_binary_path(node)
        self._set_node_data_paths(node)
        self._delete_prebundled_config_files(host, node)

    def _prepare_directories(self, host, node):
        node_log_dir = os.path.join(node.root_dir, "logs", "server")
        node_heap_dump_dir = os.path.join(node.root_dir, "heapdump")

        directories_to_create = [node.binary_path, node_log_dir, node_heap_dump_dir]
        for directory_to_create in directories_to_create:
            self._create_directory(host, directory_to_create)

    def _extract_opensearch(self, host, node, binary):
        self.logger.info("Unzipping %s to %s", binary, node.binary_path)
        self.executor.execute(host, "tar -xzvf {} --directory {}".format(binary, node.binary_path))

    def _update_node_binary_path(self, node):
        node.binary_path = os.path.join(node.binary_path, "opensearch*")

    def _set_node_data_paths(self, node):
        node.data_paths = [os.path.join(node.binary_path, "data")]

    def _delete_prebundled_config_files(self, host, node):
        config_path = os.path.join(node.binary_path, "config")
        self.logger.info("Deleting pre-bundled OpenSearch configuration at [%s]", config_path)
        self._delete_path(host, config_path)

    def cleanup(self, host):
        pass