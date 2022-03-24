import logging

from osbenchmark.builder.installers.installer import Installer


class OpenSearchInstaller(Installer):
    def __init__(self, provision_config_instance, executor):
        super().__init__(executor)
        self.logger = logging.getLogger(__name__)
        self.provision_config_instance = provision_config_instance

    def install(self, host, binaries):
        pass

    def cleanup(self, host):
        pass