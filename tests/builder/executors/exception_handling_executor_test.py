import unittest.mock as mock
from unittest import TestCase

from osbenchmark.builder.executors.exception_handling_shell_executor import ExceptionHandlingShellExecutor
from osbenchmark.exceptions import ExecutorError


class ExceptionHandlingShellExecutorTests(TestCase):
    def setUp(self):
        self.executor_impl = mock.Mock()
        self.executor_impl.execute.return_value = None
        self.executor_impl.copy.return_value = None

        self.executor = ExceptionHandlingShellExecutor(self.executor_impl)
        self.host = None
        self.command = None
        self.source = "/path/to/source"
        self.destination = "/path/to/dest"

    def test_success_executing_command(self):
        self.executor.execute(self.host, self.command)

    def test_failure_executing_command(self):
        self.executor_impl.execute.side_effect = Exception("error")

        with self.assertRaises(ExecutorError):
            self.executor.execute(self.host, self.command)
