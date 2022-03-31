from abc import ABC, abstractmethod


class Downloader(ABC):
    """
    A downloader is used to supply the necessary components for running self-managed OpenSearch. Implementations of this
    interface will download distributions or fetch from a source repository for both OpenSearch and plugins
    """
    def __init__(self, executor):
        self.executor = executor

    @abstractmethod
    def download(self, host):
        """
        Downloads the relevant data necessary to install and run OpenSearch

        ;param host: A Host object defining the host on which the data should be downloaded
        ;return binaries: A map of component names to installation paths for use by the Installer
        """
        raise NotImplementedError
