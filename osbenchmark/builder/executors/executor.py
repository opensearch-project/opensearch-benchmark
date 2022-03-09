import logging


class Executor:
    """
    Executors are used to run shell commands on the cluster hosts. Implementations of this class will use various
    technologies to interface with the hosts of a cluster.
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def execute(self, host, command, **kwargs):
        """
        Executes a list of commands against the provided host

        ;param host: A Host object defining the host on which to execute the commands
        ;param command: A shell command as a string
        ;return output: The output of the command
        """
        raise NotImplementedError

    def copy(self, host, source, destination):
        """
        Copies the specified source to the destination on the given host

        ;param host: A Host object defining the host on which to execute the commands
        ;param source: The source file to copy
        ;param destination: The destination to copy to
        ;return None
        """
        raise NotImplementedError
