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

import os
import pathlib
import subprocess
from unittest import TestCase

class ScriptsTests(TestCase):

    def test_scr(self):
        os.environ["BENCHMARK_HOME"] = "/tmp"
        script = pathlib.Path(__file__).parent.parent / "scripts" / "expand-data-corpus.py"
        p = subprocess.Popen([str(script), "-c", "10"],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stderr = p.communicate()[1].decode('UTF-8')
        self.assertTrue("could not find OSB config file" in stderr)
