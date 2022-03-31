from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.models.host import Host
from osbenchmark.builder.models.node import Node
from osbenchmark.builder.utils.host_cleaner import HostCleaner


class HostCleanerTest(TestCase):
    def setUp(self):
        self.node = Node(binary_path="/fake", data_paths=["/fake1", "/fake2"],
                         name=None, pid=None, telemetry=None, port=None, root_dir=None, log_path=None, heap_dump_path=None)
        self.host = Host(address="fake", name="fake", metadata={}, node=self.node)

        self.path_manager = Mock()
        self.host_cleaner = HostCleaner(self.path_manager)

    def test_cleanup(self):
        self.host_cleaner.cleanup(self.host, False)

        self.path_manager.delete_path.assert_has_calls([
            mock.call(self.host, "/fake1"),
            mock.call(self.host, "/fake2"),
            mock.call(self.host, "/fake")
        ])

    def test_cleanup_preserve_install(self):
        self.host_cleaner.cleanup(self.host, True)

        self.path_manager.delete_path.assert_has_calls([])
