from abc import ABC, abstractmethod


class ShellExecutor(ABC):
    """
    Executors are used to run shell commands on the cluster hosts. Implementations of this class will use various
    technologies to interface with the hosts of a cluster.
    """

    @abstractmethod
    def execute(self, host, command, **kwargs):
        """
        Executes a list of commands against the provided host

        ;param host: A Host object defining the host on which to execute the commands
        ;param command: A shell command as a string
        ;return output: The output of the command
        """
        raise NotImplementedError
