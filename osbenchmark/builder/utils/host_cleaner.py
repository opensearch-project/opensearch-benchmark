import logging

from osbenchmark.utils import console


class HostCleaner:
    def __init__(self, path_manager):
        self.logger = logging.getLogger(__name__)
        self.path_manager = path_manager

    def cleanup(self, host, preserve_install):
        if preserve_install:
            console.info("Preserving benchmark candidate installation.", logger=self.logger)
            return

        self.logger.info("Wiping benchmark candidate installation at [%s].", host.node.binary_path)

        for data_path in host.node.data_paths:
            self.path_manager.delete_path(host, data_path)

        self.path_manager.delete_path(host, host.node.binary_path)
