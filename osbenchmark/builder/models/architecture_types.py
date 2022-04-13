from enum import Enum


class ArchitectureTypes(Enum):
    """
    Represents a machine's architecture type

    :param hardware_name: The value returned by the machine when querying the architecture. Obtained via `uname -m` for unix machines
    ;param opensearch_name: The value used by opensearch artifacts to represent the architecture
    """

    def __init__(self, hardware_name, opensearch_name):
        self.hardware_name = hardware_name
        self.opensearch_name = opensearch_name

    ARM = "aarch64", "arm64"
    x86 = "x86_64", "x64"

    @staticmethod
    def get_from_hardware_name(hardware_name):
        for arch_type in ArchitectureTypes:
            if arch_type.hardware_name == hardware_name:
                return arch_type

        raise ValueError
