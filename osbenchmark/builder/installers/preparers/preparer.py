from abc import ABC, abstractmethod


class Preparer(ABC):
    """
    A preparer is used for preparing the installation of a node by setting up the filesystem, binaries, and install hooks
    """

    def __init__(self, executor):
        self.executor = executor

    @abstractmethod
    def prepare(self, host, binaries):
        """
        Prepares the filesystem and binaries on a node

        ;param host: A Host object defining the host on which to prepare the data
        ;param binaries: A map of components to download paths on the host
        ;return node: A Node object detailing the installation data of the node on the host. May be None if no Node was generated
        """
        raise NotImplementedError

    @abstractmethod
    def get_config_vars(self, host, node, all_node_ips):
        """
        Gets the config file(s) variables associated with the given preparer

        ;param host: A Host object defining a machine within a cluster
        ;param node: A Node object defining the node on a host
        ;param all_node_ips: A list of the ips for each node in the cluster. Used for cluster formation
        ;return dict: A key value pair of the config variables
        """
        raise NotImplementedError

    @abstractmethod
    def get_config_paths(self):
        """
        Returns the config paths list
        """
        raise NotImplementedError

    @abstractmethod
    def invoke_install_hook(self, host, phase, variables, env):
        """
        Invokes the associated install hook

        ;param host: A Host object defining the host on which to invoke the install hook
        ;param phase: The BoostrapPhase of install hook
        ;param variables: Key value pairs to be passed to the install hook
        ;param env: Key value pairs of environment variables to be passed ot the install hook
        ;return None
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
