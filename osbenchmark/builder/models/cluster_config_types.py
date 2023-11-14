from enum import Enum


class ClusterConfigType(str, Enum):
    PROVISION_CONFIG_INSTANCE = "cluster-config"
    MIXIN = "mixin"
