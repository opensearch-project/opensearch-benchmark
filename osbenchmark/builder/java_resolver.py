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

import logging

from osbenchmark import exceptions
from osbenchmark.utils import jvm, sysstats


def java_home(provision_config_instance_runtime_jdks, specified_runtime_jdk=None, provides_bundled_jdk=False):
    def determine_runtime_jdks():
        if specified_runtime_jdk:
            return [specified_runtime_jdk]
        else:
            return allowed_runtime_jdks

    def detect_jdk(jdks):
        major, java_home = jvm.resolve_path(jdks)
        logger.info("Detected JDK with major version [%s] in [%s].", major, java_home)
        return major, java_home

    logger = logging.getLogger(__name__)

    try:
        allowed_runtime_jdks = [int(v) for v in provision_config_instance_runtime_jdks.split(",")]

    except ValueError:
        raise exceptions.SystemSetupError(
            "ProvisionConfigInstance config key \"runtime.jdk\" is invalid: \"{}\" (must be int)".format(
                provision_config_instance_runtime_jdks))

    runtime_jdk_versions = determine_runtime_jdks()

    if runtime_jdk_versions[0] == "bundled":
        if not provides_bundled_jdk:
            raise exceptions.SystemSetupError("This OpenSearch version does not contain a bundled JDK. "
                                              "Please specify a different runtime JDK.")
        logger.info("Using JDK bundled with OpenSearch.")

        os_check = sysstats.os_name()
        if os_check == "Windows":
            raise exceptions.SystemSetupError("OpenSearch doesn't provide release artifacts for Windows currently.")
        if os_check == "Darwin":
            # OpenSearch does not provide a Darwin version of OpenSearch or a MacOS JDK version
            logger.info("Using JDK set from JAVA_HOME because OS is MacOS (Darwin).")
            logger.info("NOTICE: OpenSearch doesn't provide release artifacts for MacOS (Darwin) currently."
            " Please set JAVA_HOME to JDK 11 or JDK 8 and set the runtime.jdk.bundled to true in config.ini "
            "in opensearch-benchmark-provisionconfigs directory")
            return detect_jdk(allowed_runtime_jdks)

        # assume that the bundled JDK is the highest available; the path is irrelevant
        return allowed_runtime_jdks[0], None
    else:
        logger.info("Allowed JDK versions are %s.", runtime_jdk_versions)
        return detect_jdk(runtime_jdk_versions)
