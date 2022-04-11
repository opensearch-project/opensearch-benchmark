from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.builders.source_binary_builder import SourceBinaryBuilder
from osbenchmark.exceptions import BuildError, ExecutorError


class SourceBinaryBuilderTest(TestCase):
    def setUp(self):
        self.host = None
        self.build_commands = ["gradle build"]

        self.executor = Mock()
        self.path_manager = Mock()
        self.jdk_resolver = Mock()

        self.os_src_dir = "/fake/src/dir"
        self.build_jdk = 13
        self.log_dir = "/benchmark/logs"

        self.source_binary_builder = SourceBinaryBuilder(self.executor, self.path_manager, self.jdk_resolver,
                                                         self.os_src_dir, self.build_jdk, self.log_dir)

        self.jdk_resolver.resolve_jdk_path.return_value = (13, "/path/to/jdk")

    def test_build(self):
        self.source_binary_builder.build(self.host, self.build_commands)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "export JAVA_HOME=/path/to/jdk"),
            mock.call(self.host, "/fake/src/dir/gradle build > /benchmark/logs/build.log 2>&1")
        ])

    def test_build_with_src_dir_override(self):
        self.source_binary_builder.build(self.host, self.build_commands, "/override/src")

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "export JAVA_HOME=/path/to/jdk"),
            mock.call(self.host, "/override/src/gradle build > /benchmark/logs/build.log 2>&1")
        ])

    def test_build_failure(self):
        # Set JAVA_HOME, execute build command
        self.executor.execute.side_effect = [None, ExecutorError("fake err")]

        with self.assertRaises(BuildError):
            self.source_binary_builder.build(self.host, self.build_commands)
