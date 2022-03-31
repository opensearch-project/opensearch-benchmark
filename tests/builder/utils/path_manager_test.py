from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.utils.path_manager import PathManager


class PathManagerTest(TestCase):
    def setUp(self):
        self.host = None
        self.path = "fake"

        self.executor = Mock()
        self.path_manager = PathManager(self.executor)

    @mock.patch('osbenchmark.utils.io.ensure_dir')
    def test_create_path(self, ensure_dir):
        self.path_manager.create_path(self.host, self.path)

        ensure_dir.assert_has_calls([
            mock.call(self.path)
        ])
        self.executor.execute.assert_has_calls([
            mock.call(self.host, "mkdir -m 0777 -p {}".format(self.path))
        ])

    @mock.patch('osbenchmark.utils.io.ensure_dir')
    def test_create_path_no_local_copy(self, ensure_dir):
        self.path_manager.create_path(self.host, self.path)

        ensure_dir.assert_has_calls([])
        self.executor.execute.assert_has_calls([
            mock.call(self.host, "mkdir -m 0777 -p {}".format(self.path))
        ])

    def test_delete_valid_path(self):
        self.path_manager.delete_path(self.host, self.path)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "rm -r {}".format(self.path))
        ])

    def test_delete_invalid_path(self):
        self.path_manager.delete_path(self.host, "/")

        self.executor.execute.assert_has_calls([])
