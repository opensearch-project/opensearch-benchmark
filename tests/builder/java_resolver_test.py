# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest.mock as mock
from unittest import TestCase

from osbenchmark import exceptions
from osbenchmark.builder import java_resolver


class JavaResolverTests(TestCase):
    @mock.patch("osbenchmark.utils.jvm.resolve_path")
    def test_resolves_java_home_for_default_runtime_jdk(self, resolve_jvm_path):
        resolve_jvm_path.return_value = (12, "/opt/jdk12")
        major, java_home = java_resolver.java_home("12,11,10,9,8",
                                                   specified_runtime_jdk=None,
                                                   provides_bundled_jdk=True)

        self.assertEqual(major, 12)
        self.assertEqual(java_home, "/opt/jdk12")

    @mock.patch("osbenchmark.utils.jvm.resolve_path")
    def test_resolves_java_home_for_specific_runtime_jdk(self, resolve_jvm_path):
        resolve_jvm_path.return_value = (8, "/opt/jdk8")
        major, java_home = java_resolver.java_home("12,11,10,9,8",
                                                   specified_runtime_jdk=8,
                                                   provides_bundled_jdk=True)

        self.assertEqual(major, 8)
        self.assertEqual(java_home, "/opt/jdk8")
        resolve_jvm_path.assert_called_with([8])

    # NOTICE: This test is dependent on the operating system you have. Uncomment out and
    # delete the next test when OpenSearch has a distribution for MacOS
    # def test_resolves_java_home_for_bundled_jdk(self):
    #     major, java_home = java_resolver.java_home("12,11,10,9,8",
    #                                                specified_runtime_jdk="bundled",
    #                                                provides_bundled_jdk=True)

    #     # assumes most recent JDK
    #     self.assertEqual(major, 12)
    #     # does not set JAVA_HOME for the bundled JDK
    #     self.assertEqual(java_home, None)

    # Darwin is the operating system for MacOS and since OpenSearch does not
    # currently support MacOS, have to use a hacky method to make sure it gets
    # a proper JDK (this is because OpenSearch comes with a JDK that is not supported on MacOS as well)
    # def test_resolves_java_home_for_bundled_jdk_darwin(self):
    #     major, java_home = java_resolver.java_home("12,11,10,9,8",
    #                                                specified_runtime_jdk="bundled",
    #                                                provides_bundled_jdk=True)

    #     # Make sure you have already set JAVA_HOME to JDK 11 path in your venv or it will throw a key error
    #     java_home_set = os.getenv("JAVA_HOME")
    #     # assumes most recent JDK
    #     self.assertEqual(major, 11)
    #     # sets JAVA_HOME to JAVA_HOME env
    #     self.assertEqual(java_home, java_home_set)

    def test_disallowed_bundled_jdk(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            java_resolver.java_home("12,11,10,9,8", specified_runtime_jdk="bundled")
        self.assertEqual("This OpenSearch version does not contain a bundled JDK. Please specify a different runtime JDK.",
                         ctx.exception.args[0])
