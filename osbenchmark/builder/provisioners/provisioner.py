class Provisioner:
    def __init__(self):
        pass

    def provision_infrastructure(self):
        raise NotImplementedError

    def tear_down_infrastructure(self, cluster):
        raise NotImplementedError
