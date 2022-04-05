import logging
import os

from osbenchmark.builder.utils.path_manager import PathManager
from osbenchmark.exceptions import ExecutorError


class SourceRepositoryProvider:
    def __init__(self, executor):
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        self.path_manager = PathManager(executor)

    def fetch_repository(self, host, remote_url, target_dir):
        if not self._is_repository_present_on_host(host, target_dir):
            self._initialize_repository(host, remote_url, target_dir)

    def _initialize_repository(self, host, remote_url, target_dir):
        if remote_url is not None:
            self.logger.info("Downloading sources from %s to %s.", remote_url, target_dir)


    def _is_repository_present_on_host(self, host, target_dir):
        try:
            self.executor.execute(host, "test -d {}".format(target_dir))
            self.executor.execute(host, "test -e {}".format(os.path.join(target_dir, ".git")))
            return True
        except ExecutorError:
            return False
