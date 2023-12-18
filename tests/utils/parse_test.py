# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
from unittest import TestCase

from osbenchmark.utils.parse import parse_string_parameter, parse_int_parameter, parse_float_parameter


class ParseParamsFunctionalTests(TestCase):
    params = {
        "string-value": "hello-world",
        "int-value": 1000,
        "float-value": 1.234,
    }

    def test_parse_string_parameter_from_params(self):
        self.assertEqual("hello-world", parse_string_parameter("string-value", self.params))

    def test_parse_string_parameter_default(self):
        self.assertEqual("vector-search", parse_string_parameter("default-value", self.params, "vector-search"))

    def test_parse_int_parameter_from_params(self):
        self.assertEqual(1000, parse_int_parameter("int-value", self.params))

    def test_parse_int_parameter_default(self):
        self.assertEqual(1111, parse_int_parameter("default-value", self.params, 1111))

    def test_parse_float_parameter_from_params(self):
        self.assertEqual(1.234, parse_float_parameter("float-value", self.params))

    def test_parse_float_parameter_default(self):
        self.assertEqual(0.1, parse_float_parameter("default-value", self.params, 0.1))
