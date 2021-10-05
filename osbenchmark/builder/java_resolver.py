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

    logger = logging.getLogger(__name__)
    logger.info("pci runtime jdks: %s", provision_config_instance_runtime_jdks)
    logger.info("specified runtime jdk: %s", specified_runtime_jdk)
    logger.info("provides bundled jdk: %s", provides_bundled_jdk)

    try:
        allowed_runtime_jdks = [int(v) for v in provision_config_instance_runtime_jdks.split(",")]
        logger.info("allowed runtime jdks: %s", allowed_runtime_jdks)

    except ValueError:
        raise exceptions.SystemSetupError(
            "ProvisionConfigInstance config key \"runtime.jdk\" is invalid: \"{}\" (must be int)".format(
                provision_config_instance_runtime_jdks))

    runtime_jdk_versions = determine_runtime_jdks()
    logger.info("bundled in java_resolver?: %s", runtime_jdk_versions[0])

    if runtime_jdk_versions[0] == "bundled":
        if not provides_bundled_jdk:
            raise exceptions.SystemSetupError("This OpenSearch version does not contain a bundled JDK. "
                                              "Please specify a different runtime JDK.")
        logger.info("Using JDK bundled with OpenSearch.")

        os_check = sysstats.os_name()
        if os_check == "Darwin":
            # OpenSearch does not provide a Darwin version of OpenSearch or a MacOS JDK version
            # MIGHT HAVE TO FIX LOGIC OF DETERMINE_RUNTIME_JDKS()
            logger.info("Using JDK set from JAVA_HOME because OS is Darwin.")
            major, java_home = jvm.resolve_path(allowed_runtime_jdks)
            logger.info("Using java major version [%s] in [%s].", major, java_home)
            return major, java_home

        # assume that the bundled JDK is the highest available; the path is irrelevant
        # UPDATE FOR INTEG TESTS: OpenSeaerch doesn't provide the correct build for MacOS.
        # Please set your JAVA_HOME to JDK 11 or 8 and set the flag in config.ini in corresponding
        # provision_config_instance version (for example:
        # OpenSearch/opensearch-benchmark-provisionconfigs/1.0/provision_config_instances/v1/vanilla/config.ini)
        # to runtime.jdk.bundled = true
        return allowed_runtime_jdks[0], None
    else:
        logger.info("Allowed JDK versions are %s.", runtime_jdk_versions)
        major, java_home = jvm.resolve_path(runtime_jdk_versions)
        logger.info("Detected JDK with major version [%s] in [%s].", major, java_home)
        return major, java_home
