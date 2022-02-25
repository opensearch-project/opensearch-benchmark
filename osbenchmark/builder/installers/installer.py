class Installer:
    def __init__(self, executor):
        self.executor = executor

    def install(self, host, binaries):
        raise NotImplementedError

    def cleanup(self, host):
        raise NotImplementedError
