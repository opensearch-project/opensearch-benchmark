from enum import Enum


class ConfigInstanceTypes(Enum):
    def __init__(self, config_type, supported_config_format_versions, default_config_format_version):
        self.config_type = config_type
        self.supported_config_format_versions = supported_config_format_versions
        self.default_config_format_version = default_config_format_version

    PLUGIN = "plugins", [1], 1
