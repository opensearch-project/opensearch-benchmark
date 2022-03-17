from enum import Enum


class Provider(str, Enum):
    LOCAL = "local"
    AWS = "aws"
