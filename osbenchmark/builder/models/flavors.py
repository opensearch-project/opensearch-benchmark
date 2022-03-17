from enum import Enum


class Flavor(str, Enum):
    SELF_MANAGED = "self_managed"
    MANAGED = "managed"
