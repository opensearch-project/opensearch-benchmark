from enum import Enum


class ConfigInstanceTypes(Enum):
    def __init__(self, config_type, supported_versions, default_version):
        self.config_type = config_type
        self.supported_versions = supported_versions
        self.default_version = default_version

    PLUGIN = "plugins", [1], 1
