import logging
import os
from collections import OrderedDict

from osbenchmark.builder.utils.path_manager import PathManager
from osbenchmark.exceptions import SystemSetupError
from osbenchmark.builder.utils.git_manager import GitManager


class SourceRepositoryProvider:
    def __init__(self, executor, repository_name):
        self.logger = logging.getLogger(__name__)
        self.executor = executor
        self.path_manager = PathManager(executor)
        self.git_manager = GitManager(executor)
        self.repository_name = repository_name

        self.update_scenarios = self._generate_update_repository_scenarios()

    def _generate_update_repository_scenarios(self):
        return OrderedDict([
            (
                lambda revision, is_remote_defined: revision == "latest" and is_remote_defined,
                self._update_repository_to_latest
            ),
            (
                lambda revision, is_remote_defined: revision == "current",
                self._update_repository_to_current
            ),
            (
                lambda revision, is_remote_defined: revision.startswith("@") and is_remote_defined,
                self._update_repository_to_timestamp
            ),
            (
                lambda revision, is_remote_defined: is_remote_defined,
                self._update_repository_to_commit_hash
            ),
            (
                lambda revision, is_remote_defined: True,
                self._update_repository_to_local_revision
            ),
        ])

    def fetch_repository(self, host, remote_url, revision, target_dir):
        if not self.path_manager.is_path_present(host, os.path.join(target_dir, ".git")):
            self._initialize_repository(host, remote_url, revision, target_dir)

        self._update_repository(host, remote_url, revision, target_dir)
        return self._get_revision(host, revision, target_dir)

    def _initialize_repository(self, host, remote_url, revision, target_dir):
        if self._is_remote_defined(remote_url):
            self.logger.info("Downloading sources for %s from %s to %s.", self.repository_name, remote_url, target_dir)
            self.path_manager.create_path(host, target_dir, create_locally=False)
            self.git_manager.clone(host, remote_url, target_dir)
        elif self.path_manager.is_path_present(host, target_dir) and self._is_repository_initialization_skippable(revision):
            self.logger.info("Skipping repository initialization for %s.", self.repository_name)
        else:
            raise SystemSetupError(f"A remote repository URL is mandatory for {self.repository_name}")

    def _is_remote_defined(self, remote_url):
        return remote_url is not None

    def _is_repository_initialization_skippable(self, revision):
        return revision == "current"

    def _update_repository(self, host, remote_url, revision, target_dir):
        is_remote_defined = self._is_remote_defined(remote_url)

        for condition, update_function in self.update_scenarios.items():
            if condition(revision, is_remote_defined):
                return update_function(host, revision, target_dir)

    def _update_repository_to_latest(self, host, revision, target_dir):
        self.logger.info("Getting latest sources for %s from origin/main.", self.repository_name)
        self.git_manager.fetch(host, target_dir)
        self.git_manager.checkout(host, target_dir)
        self.git_manager.rebase(host, target_dir)

    def _update_repository_to_current(self, host, revision, target_dir):
        self.logger.info("Skip fetching sources for %s.", self.repository_name)

    def _update_repository_to_timestamp(self, host, revision, target_dir):
        # convert timestamp annotated for Benchmark to something git understands -> we strip leading and trailing " and the @.
        git_timestamp_revision = revision[1:]
        self.logger.info("Fetching from remote and checking out revision with timestamp [%s] for "
                         "%s.", git_timestamp_revision, self.repository_name)
        self.git_manager.fetch(host, target_dir)
        revision_from_timestamp = self.git_manager.get_revision_from_timestamp(host, target_dir, git_timestamp_revision)
        self.git_manager.checkout(host, target_dir, revision_from_timestamp)

    def _update_repository_to_commit_hash(self, host, revision, target_dir):
        self.logger.info("Fetching from remote and checking out revision [%s] for %s.", revision, self.repository_name)
        self.git_manager.fetch(host, target_dir)
        self.git_manager.checkout(host, target_dir, revision)

    def _update_repository_to_local_revision(self, host, revision, target_dir):
        self.logger.info("Checking out local revision [%s] for %s.", revision, self.repository_name)
        self.git_manager.checkout(host, target_dir, revision)

    def _get_revision(self, host, revision, target_dir):
        if self.path_manager.is_path_present(host, os.path.join(target_dir, ".git")):
            git_revision = self.git_manager.get_revision_from_local_repository(host, target_dir)
            self.logger.info("User-specified revision [%s] for [%s] results in git revision [%s]",
                             revision, self.repository_name, git_revision)

            return git_revision

        self.logger.info("Skipping git revision resolution for %s (%s is not a git repository).", self.repository_name, target_dir)
