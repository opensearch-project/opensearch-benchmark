from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.utils.path_manager import PathManager
from osbenchmark.exceptions import ExecutorError


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
            mock.call(self.host, f"mkdir -m 0777 -p {self.path}")
        ])

    @mock.patch('osbenchmark.utils.io.ensure_dir')
    def test_create_path_no_local_copy(self, ensure_dir):
        self.path_manager.create_path(self.host, self.path)

        ensure_dir.assert_has_calls([])
        self.executor.execute.assert_has_calls([
            mock.call(self.host, f"mkdir -m 0777 -p {self.path}")
        ])

    def test_delete_valid_path(self):
        self.path_manager.delete_path(self.host, self.path)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, f"rm -r {self.path}")
        ])

    def test_delete_invalid_path(self):
        self.path_manager.delete_path(self.host, "/")

        self.executor.execute.assert_has_calls([])

    def test_path_is_present(self):
        self.executor.execute.return_value = None

        is_path_present = self.path_manager.is_path_present(self.host, self.path)
        self.assertEqual(is_path_present, True)

    def test_path_is_not_present(self):
        self.executor.execute.side_effect = ExecutorError("fake")

        is_path_present = self.path_manager.is_path_present(self.host, self.path)
        self.assertEqual(is_path_present, False)
