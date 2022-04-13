from enum import Enum


class ConfigInstanceTypes(Enum):
    """
    A ConfigInstanceType is a representation of a configuration category.

    :param config_type: The type of configuration. This corresponds to the subdirectory name where the configurations are stored
    :param supported_config_format_versions: Multiple formats can be defined for the same configuration type. These non-equal
                                             formats are correlated with a version number. ``supported_config_format_versions``
                                             defines the supported version numbers, which is used for listing available configs
    :param default_config_format_version: The default config format version to use when parsing a configuration. This version
                                          will be used if no corresponding CLI format version is specified by the user
    """

    def __init__(self, config_type, supported_config_format_versions, default_config_format_version):
        self.config_type = config_type
        self.supported_config_format_versions = supported_config_format_versions
        self.default_config_format_version = default_config_format_version

    PLUGIN = "plugins", [1], 1
