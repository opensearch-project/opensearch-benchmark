import logging
import os
import uuid

from osbenchmark.builder.installers.preparers.preparer import Preparer
from osbenchmark.builder.models.node import Node
from osbenchmark.builder.utils.binary_keys import BinaryKeys
from osbenchmark.builder.utils.host_cleaner import HostCleaner
from osbenchmark.builder.utils.path_manager import PathManager


class OpenSearchPreparer(Preparer):
    def __init__(self, provision_config_instance, executor, hook_handler_class):
        super().__init__(executor)
        self.logger = logging.getLogger(__name__)
        self.provision_config_instance = provision_config_instance
        self.hook_handler = hook_handler_class(self.provision_config_instance)
        if self.hook_handler.can_load():
            self.hook_handler.load()
        self.path_manager = PathManager(executor)
        self.host_cleaner = HostCleaner(self.path_manager)

    def prepare(self, host, binaries):
        node = self._create_node()
        self._prepare_node(host, node, binaries[BinaryKeys.OPENSEARCH])

        return node

    def _create_node(self):
        node_name = str(uuid.uuid4())
        node_port = int(self.provision_config_instance.variables["node"]["port"])
        node_root_dir = os.path.join(self.provision_config_instance.variables["test_execution_root"], node_name)
        node_binary_path = os.path.join(node_root_dir, "install")
        node_log_dir = os.path.join(node_root_dir, "logs", "server")
        node_heap_dump_dir = os.path.join(node_root_dir, "heapdump")

        return Node(name=node_name,
                    port=node_port,
                    pid=None,
                    root_dir=node_root_dir,
                    binary_path=node_binary_path,
                    log_path=node_log_dir,
                    heap_dump_path=node_heap_dump_dir,
                    data_paths=None,
                    telemetry=None)

    def _prepare_node(self, host, node, binary):
        self._prepare_directories(host, node)
        self._extract_opensearch(host, node, binary)
        self._update_node_binary_path(node)
        self._set_node_data_paths(node)
        # we need to immediately delete the prebundled config files as plugins may copy their configuration during installation.
        self._delete_prebundled_config_files(host, node)

    def _prepare_directories(self, host, node):
        directories_to_create = [node.binary_path, node.log_path, node.heap_dump_path]
        for directory_to_create in directories_to_create:
            self.path_manager.create_path(host, directory_to_create)

    def _extract_opensearch(self, host, node, binary):
        self.logger.info("Unzipping %s to %s", binary, node.binary_path)
        self.executor.execute(host, f"tar -xzvf {binary} --directory {node.binary_path}")

    def _update_node_binary_path(self, node):
        node.binary_path = os.path.join(node.binary_path, "opensearch*")

    def _set_node_data_paths(self, node):
        node.data_paths = [os.path.join(node.binary_path, "data")]

    def _delete_prebundled_config_files(self, host, node):
        config_path = os.path.join(node.binary_path, "config")
        self.logger.info("Deleting pre-bundled OpenSearch configuration at [%s]", config_path)
        self.path_manager.delete_path(host, config_path)

    def get_config_vars(self, host, node, all_node_ips):
        installer_defaults = {
            "cluster_name": self.provision_config_instance.variables["cluster_name"],
            "node_name": node.name,
            "data_paths": node.data_paths[0],
            "log_path": node.log_path,
            "heap_dump_path": node.heap_dump_path,
            # this is the node's IP address as specified by the user when invoking Benchmark
            "node_ip": host.address,
            # this is the IP address that the node will be bound to. Benchmark will bind to the node's IP address (but not to 0.0.0.0). The
            "network_host": host.address,
            "http_port": str(node.port),
            "transport_port": str(node.port + 100),
            "all_node_ips": "[\"%s\"]" % "\",\"".join(all_node_ips),
            # at the moment we are strict and enforce that all nodes are master eligible nodes
            "minimum_master_nodes": len(all_node_ips),
            "install_root_path": node.binary_path
        }
        config_vars = {}
        config_vars.update(self.provision_config_instance.variables)
        config_vars.update(installer_defaults)
        return config_vars

    def get_config_paths(self):
        return self.provision_config_instance.config_paths

    def invoke_install_hook(self, host, phase, variables, env):
        self.hook_handler.invoke(phase.name, variables=variables, env=env)

    def cleanup(self, host):
        self.host_cleaner.cleanup(host, self.provision_config_instance.variables["preserve_install"])
