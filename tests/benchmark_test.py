import sys
import unittest
from unittest import mock

from osbenchmark import benchmark


class ArgParserTests(unittest.TestCase):
    @mock.patch.object(sys, "argv", ["opensearch-benchmark", "execute-test"])
    def test_execute_test_supports_no_await(self):
        parser = benchmark.create_arg_parser()

        args = parser.parse_args(["execute-test", "--no-await"])

        self.assertTrue(args.no_await)

    @mock.patch.object(sys, "argv", ["opensearch-benchmark", "execute-test"])
    def test_execute_test_disables_no_await_by_default(self):
        parser = benchmark.create_arg_parser()

        args = parser.parse_args(["execute-test"])

        self.assertFalse(args.no_await)
