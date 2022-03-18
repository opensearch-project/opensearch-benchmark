from enum import Enum


class ClusterInfraProvider(str, Enum):
    LOCAL = "local"
    AWS = "aws"
