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

import re
from importlib import resources

import pkg_resources

from osbenchmark import paths
from osbenchmark.utils import git, io

__version__ = pkg_resources.require("opensearch-benchmark")[0].version

__BENCHMARK_VERSION_PATTERN = re.compile(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:.(.+))?$")


def revision():
    """
    :return: The current git revision if Benchmark is installed in development mode or ``None``.
    """
    # noinspection PyBroadException
    try:
        if git.is_working_copy(io.normalize_path("%s/.." % paths.benchmark_root())):
            raw_revision = git.head_revision(paths.benchmark_root())
            return raw_revision.strip()
    except BaseException:
        pass
    return None


def version():
    """
    :return: The release version string and an optional suffix for the current git revision if Benchmark is installed in development mode.
    """
    release = __version__
    benchmark_revision = revision()
    if benchmark_revision:
        return "%s (git revision: %s)" % (release, benchmark_revision.strip())
    else:
        # cannot determine head revision so user has probably installed Benchmark via pip instead of git clone
        return release


def release_version():
    """
    :return: The release version string split into its components: major, minor, patch and optional suffix.
    """

    matches = __BENCHMARK_VERSION_PATTERN.match(__version__)
    if matches.start(4) > 0:
        return int(matches.group(1)), int(matches.group(2)), int(matches.group(3)), matches.group(4)
    elif matches.start(3) > 0:
        return int(matches.group(1)), int(matches.group(2)), int(matches.group(3)), None


def minimum_os_version():
    """
    :return: A string identifying the minimum version of OpenSearch that is supported by Benchmark.
    """
    return resources.read_text("osbenchmark", "min-os-version.txt").strip()
