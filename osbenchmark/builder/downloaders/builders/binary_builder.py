from abc import ABC, abstractmethod


class BinaryBuilder(ABC):
    """
    A BinaryBuilder is used to wrap the executor calls necessary for constructing binaries from code
    """

    @abstractmethod
    def build(self, host, build_commands, override_source_directory):
        """
        Runs the provided commands on the given host to build binaries

        :param host: A host object representing the machine on which to run the commands
        :param build_commands: A list of strings representing sequential bash commands used to build the binaries
        :param override_source_directory: A string representing the source directory where the pre-binary code is located
        :return None
        """
        raise NotImplementedError
