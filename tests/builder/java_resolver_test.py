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

    @mock.patch("osbenchmark.utils.sysstats.os_name", return_value="Windows")
    def test_resolves_java_home_for_bundled_jdk_windows(self, os_name):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            java_resolver.java_home("12,11,10,9,8", specified_runtime_jdk="bundled", provides_bundled_jdk=True)
        self.assertEqual("OpenSearch doesn't provide release artifacts for Windows currently.", ctx.exception.args[0])

    def test_disallowed_bundled_jdk(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            java_resolver.java_home("12,11,10,9,8", specified_runtime_jdk="bundled")
        self.assertEqual("This OpenSearch version does not contain a bundled JDK. Please specify a different runtime JDK.",
                         ctx.exception.args[0])
