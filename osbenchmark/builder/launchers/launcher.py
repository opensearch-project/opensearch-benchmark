from abc import ABC, abstractmethod


class Launcher(ABC):
    """
    Launchers are used to start and stop OpenSearch on the nodes in a self-managed cluster.
    """
    def __init__(self, shell_executor):
        self.shell_executor = shell_executor

    @abstractmethod
    def start(self, host, node_configurations):
        """
        Starts the OpenSearch nodes on a given host

        ;param host: A Host object defining the host on which to start the nodes
        ;param node_configurations: A list of NodeConfiguration objects detailing the installation data of the nodes on the host
        ;return nodes: A list of Node objects defining the nodes running on a host
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self, host, nodes):
        """
        Stops the OpenSearch nodes on a given host

        ;param host: A Host object defining the host on which to stop the nodes
        ;param nodes: A list of Node objects defining the nodes running on a host
        ;return nodes: A list of Node objects representing OpenSearch nodes that were successfully stopped on the host
        """
        raise NotImplementedError
