from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.downloaders.builders.source_builder import SourceBuilder
from osbenchmark.exceptions import BuildError, ExecutorError


class SourceBuilderTest(TestCase):
    def setUp(self):
        self.host = None
        self.build_command = "gradle build"

        self.executor = Mock()
        self.os_src_dir = "/fake/src/dir"
        self.build_jdk = 13
        self.log_dir = "/benchmark/logs"

        self.source_builder = SourceBuilder(self.executor, self.os_src_dir, self.build_jdk, self.log_dir)
        self.source_builder.path_manager = Mock()
        self.source_builder.jdk_resolver = Mock()
        self.source_builder.jdk_resolver.resolve_jdk_path.return_value = (13, "/path/to/jdk")

    def test_build(self):
        self.source_builder.build(self.host, self.build_command)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "export JAVA_HOME=/path/to/jdk"),
            mock.call(self.host, "/fake/src/dir/gradle build > /benchmark/logs/build.log 2>&1")
        ])

    def test_build_with_src_dir_override(self):
        self.source_builder.build(self.host, self.build_command, "/override/src")

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "export JAVA_HOME=/path/to/jdk"),
            mock.call(self.host, "/override/src/gradle build > /benchmark/logs/build.log 2>&1")
        ])

    def test_build_failure(self):
        # Set JAVA_HOME, execute build command
        self.executor.execute.side_effect = [None, ExecutorError("fake err")]

        with self.assertRaises(BuildError):
            self.source_builder.build(self.host, self.build_command)
