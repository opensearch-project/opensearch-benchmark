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
import tempfile

from esrally.utils import process, io
from esrally import exceptions

LOGGER_NAME="rally.provisioner.security"

# Used for automatically create a certificate for the current Rally node.
instances_yml_template = """
instances:
  - name: "{node_name}"
    ip: 
      - "{node_ip}"  
"""


def resolve_binary(install_root, binary_name):
    return os.path.join(install_root, "bin", binary_name)


def install_certificates(config_names, variables, **kwargs):
    if "x-pack-security" not in config_names:
        return False
    logger = logging.getLogger(LOGGER_NAME)
    cert_binary = "elasticsearch-certutil"
    node_name = variables["node_name"]
    node_ip = variables["node_ip"]
    install_root = variables["install_root_path"]
    bundled_ca_path = os.path.join(os.path.dirname(__file__), "ca")
    x_pack_config_path = os.path.join(install_root, "config", "x-pack")

    logger.info("Installing certificates for node [%s].", node_name)
    instances_yml = os.path.join(tempfile.mkdtemp(), "instances.yml")
    with open(instances_yml, "w") as f:
        f.write(instances_yml_template.format(node_name=node_name, node_ip=node_ip))

    # Generate instance certificates based on a CA that is pre-bundled with Rally
    certutil = resolve_binary(install_root, cert_binary)
    cert_bundle = os.path.join(install_root, "node-cert.zip")

    return_code = process.run_subprocess_with_logging(
        '{certutil} cert --silent --in "{instances_yml}" --out="{cert_bundle}" --ca-cert="{ca_path}/ca.crt" '
        '--ca-key="{ca_path}/ca.key" --pass ""'.format(
            certutil=certutil,
            ca_path=bundled_ca_path,
            instances_yml=instances_yml,
            cert_bundle=cert_bundle), env=kwargs.get("env"))

    if return_code != 0:
        logger.error("%s has exited with code [%d]", cert_binary, return_code)
        raise exceptions.SystemSetupError(
            "Could not create certificate bundle for node [{}]. Please see the log for details.".format(node_name))

    io.decompress(cert_bundle, x_pack_config_path)

    # Success
    return True


def add_rally_user(config_names, variables, **kwargs):
    if "x-pack-security" not in config_names:
        return False
    logger = logging.getLogger(LOGGER_NAME)
    users_binary = "elasticsearch-users"
    user_name = variables.get("xpack_security_user_name", "rally")
    user_password = variables.get("xpack_security_user_password", "rally-password")
    user_role = variables.get("xpack_security_user_role", "superuser")
    install_root = variables["install_root_path"]
    logger.info("Adding user '%s'.",user_name)
    users = resolve_binary(install_root, users_binary)

    return_code = process.run_subprocess_with_logging(
        '{users} useradd {user_name} -p "{user_password}"'.format(
            users=users,
            user_name=user_name,
            user_password=user_password
        ),
        env=kwargs.get("env"))
    if return_code != 0:
        logger.error("%s has exited with code [%d]", users_binary, return_code)
        raise exceptions.SystemSetupError("Could not add user '{}'. Please see the log for details.".format(user_name))

    return_code = process.run_subprocess_with_logging(
        '{users} roles {user_name} -a {user_role}'.format(
            users=users,
            user_name=user_name,
            user_role=user_role
        ),
        env=kwargs.get("env"))
    if return_code != 0:
        logger.error("%s has exited with code [%d]", users_binary, return_code)
        raise exceptions.SystemSetupError(
            "Could not add role '{user_role}' for user '{user_name}'. Please see the log for details.".format(
                user_role=user_role,
                user_name=user_name
            ))

    return True


def register(registry):
    registry.register("post_install", install_certificates)
    registry.register("post_install", add_rally_user)
