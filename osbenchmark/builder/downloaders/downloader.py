class Downloader:
    def __init__(self, executor):
        self.executor = executor

    def download(self, host):
        raise NotImplementedError
