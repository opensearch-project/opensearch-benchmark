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

logger = logging.getLogger("rally.provisioner.plugin.xpack")

# Used for automatically create a certificate for the current Rally node.
instances_yml_template = """
instances:
  - name: "{node_name}"
    ip: 
      - "{node_ip}"  
"""


def resolve_binary(install_root, binary_name):
    return os.path.join(install_root, "bin", "elasticsearch-{}".format(binary_name))


def install_certificates(config_names, variables, **kwargs):
    if "x-pack-security" not in config_names:
        return False

    node_name = variables["node_name"]
    node_ip = variables["node_ip"]
    install_root = variables["install_root_path"]
    x_pack_config_path = os.path.join(install_root, "config", "x-pack")

    logger.info("Installing x-pack certificates for node [%s]." % node_name)
    # 0. Create instances.yml for the current ES node.
    instances_yml = os.path.join(tempfile.mkdtemp(), "instances.yml")
    with open(instances_yml, "w") as f:
        f.write(instances_yml_template.format(node_name=node_name, node_ip=node_ip))

    # 1. Create certificate if needed. We will prebundle the CA with Rally and generate instance certificates based on this CA.

    cert_gen = resolve_binary(install_root, "certgen")
    cert_bundle = os.path.join(install_root, "config", "x-pack", "node-cert.zip")

    # ./bin/x-pack/certgen
    #   -cert=/Users/daniel/.rally/benchmarks/distributions/elasticsearch-5.5.0/config/x-pack/ca/ca.crt
    #   -key=/Users/daniel/.rally/benchmarks/distributions/elasticsearch-5.5.0/config/x-pack/ca/ca.key
    #   -in=/Users/daniel/.rally/benchmarks/distributions/elasticsearch-5.5.0/config/instances.yml
    #   -out=/Users/daniel/.rally/benchmarks/distributions/elasticsearch-5.5.0/config/x-pack/node-cert.zip

    return_code = process.run_subprocess_with_logging(
        '{cert_gen} -cert="{config_path}/ca/ca.crt" -key="{config_path}/ca/ca.key" -in="{instances_yml}" -out="{cert_bundle}"'
            .format(cert_gen=cert_gen, config_path=x_pack_config_path, instances_yml=instances_yml, cert_bundle=cert_bundle))
    if return_code != 0:
        logger.error("certgen has exited with code [%d]" % return_code)
        raise exceptions.SystemSetupError("Could not create x-pack certificate bundle for node [%s]. Please see the log for details."
                                          % node_name)

    # 2. Unzip /Users/daniel/.rally/benchmarks/distributions/elasticsearch-5.5.0/config/x-pack/node-cert.zip
    io.decompress(cert_bundle, x_pack_config_path)

    # Success
    return True


def add_rally_user(config_names, variables, **kwargs):
    if "x-pack-security" not in config_names:
        return False

    users_binary = "users"
    user_name = variables.get("xpack_security_user_name", "rally")
    user_password = variables.get("xpack_security_user_password", "rally-password")
    user_role = variables.get("xpack_security_user_role", "superuser")
    install_root = variables["install_root_path"]
    logger.info("Adding user '%s'.", user_name)
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
