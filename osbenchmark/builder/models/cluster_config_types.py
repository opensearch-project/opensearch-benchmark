from enum import Enum


class ClusterConfigType(str, Enum):
    CLUSTER_CONFIG_INSTANCE = "cluster-config"
    MIXIN = "mixin"
