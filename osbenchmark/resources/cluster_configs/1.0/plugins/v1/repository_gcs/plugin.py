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

from osbenchmark.utils import process
from osbenchmark import exceptions

LOGGER_NAME = "osbenchmark.provisioner.repository_gcs"


def resolve_binary(install_root, binary_name):
    return os.path.join(install_root, "bin", binary_name)


def resolve_keystore_config(install_root):
    return os.path.join(install_root, "config", "opensearch.keystore")


def create_keystore(install_root, keystore_binary, env):
    logger = logging.getLogger(LOGGER_NAME)

    keystore_create_command = "{keystore} --silent create".format(keystore=keystore_binary)

    return_code = process.run_subprocess_with_logging(
        keystore_create_command,
        env=env
    )

    if return_code != 0:
        logger.error("%s has exited with code [%d]", keystore_create_command, return_code)
        raise exceptions.SystemSetupError(
            "Could not initialize a keystore. Please see the log for details.")


def configure_keystore(config_names, variables, **kwargs):
    logger = logging.getLogger(LOGGER_NAME)
    keystore_params = ["gcs_client_name", "gcs_credentials_file"]
    client_name = variables.get(keystore_params[0])
    credentials_file = variables.get(keystore_params[1])

    if not (credentials_file and client_name):
        logger.warning("Skipping keystore configuration for repository-gcs as plugin-params %s were not supplied", keystore_params)
        return False

    keystore_binary_filename = "opensearch-keystore"
    install_root = variables["install_root_path"]
    keystore_binary = resolve_binary(install_root, keystore_binary_filename)
    env = kwargs.get("env")

    if not os.path.isfile(resolve_keystore_config(install_root)):
        create_keystore(install_root, keystore_binary, env)

    keystore_command = "{keystore} --silent add-file gcs.client.{client_name}.credentials_file {credentials_file}".format(
        keystore=keystore_binary,
        client_name=client_name,
        credentials_file=credentials_file)

    return_code = process.run_subprocess_with_logging(
        keystore_command,
        env=env
    )

    if return_code != 0:
        logger.error("%s has exited with code [%d]", keystore_command, return_code)
        raise exceptions.SystemSetupError(
            "Could not add GCS keystore secure setting. Please see the log for details.")

    # Success
    return True


def register(registry):
    registry.register("post_install", configure_keystore)
