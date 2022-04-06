import logging
import os

from osbenchmark.builder.utils.path_manager import PathManager
from osbenchmark.exceptions import ExecutorError, SystemSetupError


class SourceRepositoryProvider:
    def __init__(self, executor, repository_name):
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        self.path_manager = PathManager(executor)
        self.repository_name = repository_name

    def fetch_repository(self, host, remote_url, revision, target_dir):
        if not self._is_repository_present_on_host(host, target_dir):
            self._initialize_repository(host, remote_url, revision, target_dir)

        self._update_repository(host, remote_url, revision, target_dir)

    def _initialize_repository(self, host, remote_url, revision, target_dir):
        if remote_url is not None:
            self.logger.info(f"Downloading sources for {self.repository_name} from {remote_url} to {target_dir}.")
            self.path_manager.create_path(host, target_dir, create_locally=False)
            self.executor.execute(host, f"git clone {remote_url} {target_dir}")
        elif self._is_directory_present(host, target_dir) and self._is_repository_initialization_skippable(revision):
            self.logger.info(f"Skipping repository initialization for {self.repository_name}.")
        else:
            raise SystemSetupError(f"A remote repository URL is mandatory for {self.repository_name}")

    def _is_repository_present_on_host(self, host, target_dir):
        return self._is_directory_present(host, target_dir) and self._is_directory_a_repository(host, target_dir)

    def _is_directory_a_repository(self, host, directory):
        try:
            self.executor.execute(host, "test -e {}".format(os.path.join(directory, ".git")))
            return True
        except ExecutorError:
            return False

    def _is_directory_present(self, host, directory):
        try:
            self.executor.execute(host, "test -d {}".format(directory))
            return True
        except ExecutorError:
            return False

    def _is_repository_initialization_skippable(self, revision):
        return revision == "current"

    def _update_repository(self, host, remote_url, revision, target_dir):
        # TODO