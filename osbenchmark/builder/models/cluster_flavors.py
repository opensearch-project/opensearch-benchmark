from enum import Enum


class ClusterFlavor(str, Enum):
    SELF_MANAGED = "self_managed"
    MANAGED = "managed"
