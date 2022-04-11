from enum import Enum


class BootstrapPhase(Enum):
    """
    An enum defining the valid phases of bootstrapping. A BootstrapPhase is used to define when a BootstrapHookHandler
    is executed during cluster creation.
    """
    POST_INSTALL = 10

    @classmethod
    def valid(cls, name):
        for n in BootstrapPhase.names():
            if n == name:
                return True
        return False

    @classmethod
    def names(cls):
        return [p.name for p in list(BootstrapPhase)]
