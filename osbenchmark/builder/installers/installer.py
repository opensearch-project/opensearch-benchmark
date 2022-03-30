from abc import ABC, abstractmethod


class Installer(ABC):
    """
    Installers are invoked to prepare the OpenSearch and Plugin data that exists on a host so that an OpenSearch cluster
    can be started.
    """

    def __init__(self, executor):
        self.executor = executor

    @abstractmethod
    def install(self, host, binaries, all_node_ips):
        """
        Executes the necessary logic to prepare and install OpenSearch and any request Plugins on a cluster host

        ;param host: A Host object defining the host on which to install the data
        ;param binaries: A map of components to install to their paths on the host
        ;param all_node_ips: A list of the ips for each node in the cluster. Used for cluster formation
        ;return node: A Node object detailing the installation data of the node on the host
        """
        raise NotImplementedError

    @abstractmethod
    def cleanup(self, host):
        """
        Removes the data that was downloaded, installed, and created on a given host during the test execution

        ;param host: A Host object defining the host on which to remove the data
        ;return None
        """
        raise NotImplementedError
