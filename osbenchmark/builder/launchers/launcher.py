class Launcher:
    def __init__(self, executor):
        self.executor = executor

    def start(self, host, node_configurations):
        raise NotImplementedError

    def stop(self, host):
        raise NotImplementedError
