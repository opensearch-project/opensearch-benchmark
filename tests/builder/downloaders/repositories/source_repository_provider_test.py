from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.repositories.source_repository_provider import SourceRepositoryProvider
from osbenchmark.exceptions import SystemSetupError


class SourceRepositoryProviderTest(TestCase):
    def setUp(self):
        self.host = None
        self.remote_url = "https://git.myrepo.com/repo"
        self.revision = "current"
        self.target_dir = "/fake/path"

        self.executor = Mock()

        self.source_repo_provider = SourceRepositoryProvider(self.executor, "my repo")
        self.source_repo_provider.path_manager = Mock()
        self.source_repo_provider.git_manager = Mock()

        self.source_repo_provider.path_manager.is_path_present.return_value = True

    def test_initialize_repo_with_remote(self):
        self.source_repo_provider.path_manager.is_path_present.return_value = False

        self.source_repo_provider.fetch_repository(self.host, self.remote_url, self.revision, self.target_dir)

        self.source_repo_provider.path_manager.create_path.assert_has_calls([
            mock.call(self.host, self.target_dir, create_locally=False)
        ])
        self.source_repo_provider.git_manager.clone.assert_has_calls([
            mock.call(self.host, self.remote_url, self.target_dir)
        ])

    def test_initialize_repo_skippable(self):
        # Check repo/.git, check repo, check repo/.git
        self.source_repo_provider.path_manager.is_path_present.side_effect = [False, True, False]

        self.source_repo_provider.fetch_repository(self.host, None, self.revision, self.target_dir)

        self.source_repo_provider.path_manager.create_path.assert_has_calls([])
        self.source_repo_provider.git_manager.clone.assert_has_calls([])

    def test_initialize_repo_no_remote_not_skippable(self):
        self.source_repo_provider.path_manager.is_path_present.return_value = False

        with self.assertRaises(SystemSetupError):
            self.source_repo_provider.fetch_repository(self.host, None, "latest", self.target_dir)

    def test_update_repo_to_latest(self):
        self.source_repo_provider.fetch_repository(self.host, self.remote_url, "latest", self.target_dir)

        self.source_repo_provider.git_manager.assert_has_calls([
            mock.call.fetch(self.host, self.target_dir),
            mock.call.checkout(self.host, self.target_dir),
            mock.call.rebase(self.host, self.target_dir),
            mock.call.get_revision_from_local_repository(self.host, self.target_dir)
        ])

    def test_update_repo_to_current(self):
        self.source_repo_provider.fetch_repository(self.host, self.remote_url, self.revision, self.target_dir)

        self.source_repo_provider.git_manager.assert_has_calls([
            mock.call.get_revision_from_local_repository(self.host, self.target_dir)
        ])

    def test_update_repo_to_timestamp(self):
        self.source_repo_provider.git_manager.get_revision_from_timestamp.return_value = "fake rev"

        self.source_repo_provider.fetch_repository(self.host, self.remote_url, "@fake-timestamp", self.target_dir)

        self.source_repo_provider.git_manager.assert_has_calls([
            mock.call.fetch(self.host, self.target_dir),
            mock.call.get_revision_from_timestamp(self.host, self.target_dir, "fake-timestamp"),
            mock.call.checkout(self.host, self.target_dir, "fake rev"),
            mock.call.get_revision_from_local_repository(self.host, self.target_dir)
        ])

    def test_update_repo_to_commit_hash(self):
        self.source_repo_provider.fetch_repository(self.host, self.remote_url, "uuid", self.target_dir)

        self.source_repo_provider.git_manager.assert_has_calls([
            mock.call.fetch(self.host, self.target_dir),
            mock.call.checkout(self.host, self.target_dir, "uuid"),
            mock.call.get_revision_from_local_repository(self.host, self.target_dir)
        ])

    def test_update_repo_to_local_revision(self):
        self.source_repo_provider.fetch_repository(self.host, None, "fake rev", self.target_dir)

        self.source_repo_provider.git_manager.assert_has_calls([
            mock.call.checkout(self.host, self.target_dir, "fake rev"),
            mock.call.get_revision_from_local_repository(self.host, self.target_dir)
        ])

    def test_get_revision_repo_exists(self):
        self.source_repo_provider.git_manager.get_revision_from_local_repository.return_value = "my rev"

        revision = self.source_repo_provider.fetch_repository(self.host, self.remote_url, self.revision, self.target_dir)
        self.assertEqual(revision, "my rev")

    def test_get_revision_repo_does_not_exist(self):
        self.source_repo_provider.path_manager.is_path_present.return_value = False

        revision = self.source_repo_provider.fetch_repository(self.host, self.remote_url, self.revision, self.target_dir)
        self.assertEqual(revision, None)
