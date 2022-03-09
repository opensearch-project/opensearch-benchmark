import logging

from osbenchmark.exceptions import ExecutorError


class Executor:
    """
    Executors are used to run shell commands on the cluster hosts. Implementations of this class will use various
    technologies to interface with the hosts of a cluster.
    """
    def __init__(self, executor):
        self.logger = logging.getLogger(__name__)
        self.executor = executor

    def execute(self, host, command, **kwargs):
        """
        Executes a list of commands against the provided host

        ;param host: A Host object defining the host on which to execute the commands
        ;param command: A shell command as a string
        ;return output: The output of the command
        """
        try:
            self.executor.execute(host, command, kwargs)
        except Exception as e:
            raise ExecutorError("Command \"{}\" on host \"{}\" failed to execute".format(command, host), e)

    def copy(self, host, source, destination, **kwargs):
        """
        Copies the specified source to the destination on the given host

        ;param host: A Host object defining the host on which to execute the commands
        ;param source: The source file to copy
        ;param destination: The destination to copy to
        ;return None
        """
        try:
            self.executor.copy(host, source, destination, kwargs)
        except Exception as e:
            raise ExecutorError("Copying \"{}\" to \"{}\" failed on host \"{}\"", e)
