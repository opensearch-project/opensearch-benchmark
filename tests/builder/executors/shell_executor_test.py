import unittest.mock as mock
from unittest import TestCase

from osbenchmark.builder.executors.shell_executor import ShellExecutor
from osbenchmark.exceptions import ExecutorError


class ShellExecutorTests(TestCase):
    def setUp(self):
        self.executor = ShellExecutor()
        self.host = None
        self.command = None

    @mock.patch("osbenchmark.utils.process.run_subprocess_with_output")
    def test_command_with_output(self, run_subprocess_with_output):
        run_subprocess_with_output.return_value = ["test", "output"]

        output = self.executor.execute(self.host, self.command, output=True)
        self.assertEqual(output, ["test", "output"])

    @mock.patch("osbenchmark.utils.process.run_subprocess_with_logging")
    def test_command_with_logging_success(self, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 0

        self.executor.execute(self.host, self.command)

    @mock.patch("osbenchmark.utils.process.run_subprocess_with_logging")
    def test_command_with_logging_failure(self, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 86

        with self.assertRaises(ExecutorError):
            self.executor.execute(self.host, self.command)
