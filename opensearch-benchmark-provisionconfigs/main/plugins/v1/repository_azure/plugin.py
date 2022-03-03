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
import os
import subprocess

from osbenchmark.utils import process
from osbenchmark import exceptions

LOGGER_NAME = "osbenchmark.provisioner.repository_azure"


def resolve_binary(install_root, binary_name):
    return os.path.join(install_root, "bin", binary_name)


def resolve_keystore_config(install_root):
    return os.path.join(install_root, "config", "opensearch.keystore")


def create_keystore(install_root, keystore_binary, env):
    logger = logging.getLogger(LOGGER_NAME)

    keystore_create_command = "{keystore} -s create".format(keystore=keystore_binary)

    return_code = process.run_subprocess_with_logging(
        keystore_create_command,
        env=env
    )

    if return_code != 0:
        logger.error("%s has exited with code [%d]", keystore_create_command, return_code)
        raise exceptions.SystemSetupError(
            "Could not initialize a keystore. Please see the log for details.")


def add_property_to_keystore(keystore_binary, client_name, property_name, property_value, env):
    logger = logging.getLogger(LOGGER_NAME)

    p1 = subprocess.Popen(["echo", property_value], stdout=subprocess.PIPE)

    keystore_command = "{keystore} --silent add --stdin azure.client.{client_name}.{key}".format(
        keystore=keystore_binary,
        client_name=client_name,
        key=property_name)

    return_code = process.run_subprocess_with_logging(
        keystore_command,
        stdin=p1.stdout,
        env=env
    )

    if return_code != 0:
        logger.error("%s has exited with code [%d]", keystore_command, return_code)
        raise exceptions.SystemSetupError(
            "Could not add Azure keystore secure setting [{}]. Please see the log for details.".format(property_name))


def configure_keystore(config_names, variables, **kwargs):
    logger = logging.getLogger(LOGGER_NAME)
    keystore_params = ["azure_account", "azure_key"]
    client_name = variables.get("azure_client_name")

    # skip keystore configuration entirely if any of the mandatory params is missing
    if not (client_name and variables.get(keystore_params[0]) and variables.get(keystore_params[1])):
        logger.warning("Skipping keystore configuration for repository-azure as mandatory plugin-params [%s,%s,%s] were not supplied",
                       "azure_client_name",
                       keystore_params[0],
                       keystore_params[1])
        return False

    keystore_binary_filename = "opensearch-keystore"
    install_root = variables["install_root_path"]
    keystore_binary = resolve_binary(install_root, keystore_binary_filename)
    env = kwargs.get("env")

    if not os.path.isfile(resolve_keystore_config(install_root)):
        create_keystore(install_root, keystore_binary, env)

    for property_name in keystore_params:
        # the actual OpenSearch secure settings for the azure plugin don't contain the azure_ prefix
        os_property_name = property_name.replace("azure_", "")
        property_value = variables.get(property_name)
        # skip optional properties like session_token
        if not property_value:
            continue

        add_property_to_keystore(keystore_binary, client_name, os_property_name, property_value, env)

    # Success
    return True


def register(registry):
    registry.register("post_install", configure_keystore)
