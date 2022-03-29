import logging
import os
import uuid

import jinja2
from jinja2 import select_autoescape

from osbenchmark import paths
from osbenchmark.builder.installers.installer import Installer
from osbenchmark.builder.models.node import Node
from osbenchmark.utils import io


class DockerInstaller(Installer):
    def __init__(self, provision_config_instance, executor):
        self.logger = logging.getLogger(__name__)
        super().__init__(executor, self.logger)
        self.provision_config_instance = provision_config_instance

    def install(self, host, binaries, all_node_ips):
        node = self._create_node()
        self._prepare_node(host, node)

        return node

    def _create_node(self):
        node_name = str(uuid.uuid4())
        node_port = int(self.provision_config_instance.variables["node"]["port"])
        node_root_dir = os.path.join(self.provision_config_instance.variables["test_execution_root"], node_name)
        node_data_paths = [os.path.join(node_root_dir, "data", str(uuid.uuid4()))]
        node_binary_path = os.path.join(node_root_dir, "install")

        return Node(name=node_name,
                    port=node_port,
                    pid=None,
                    root_dir=node_root_dir,
                    binary_path=node_binary_path,
                    data_paths=node_data_paths,
                    telemetry=None)

    def _prepare_node(self, host, node):
        node_log_dir = os.path.join(node.root_dir, "logs", "server")
        node_heap_dump_dir = os.path.join(node.root_dir, "heapdump")

        directories_to_create = [node.binary_path, node_log_dir, node_heap_dump_dir, node.data_paths[0]]
        for directory_to_create in directories_to_create:
            self._create_directory(host, directory_to_create)

        mounts = self._prepare_mounts(host, node)
        docker_cfg = self._render_template_from_docker_file(self._get_docker_vars(node, node_log_dir,
                                                                                  node_heap_dump_dir, mounts))
        self.logger.info("Installing Docker container with configuration:\n%s", docker_cfg)

        docker_compose_file = os.path.join(node.binary_path, "docker-compose.yml")
        with open(docker_compose_file, mode="wt", encoding="utf-8") as f:
            f.write(docker_cfg)
        self.executor.execute(host, "cp {0} {0}".format(docker_compose_file))

    def _prepare_mounts(self, host, node):
        config_vars = self._get_config_vars(node)
        return self._apply_configs(host, node, self.provision_config_instance.config_paths, config_vars)

    def _get_config_vars(self, node):
        provisioner_defaults = {
            "cluster_name": self.provision_config_instance.variables["cluster_name"],
            "node_name": node.name,
            # we bind-mount the directories below on the host to these ones.
            "install_root_path": "/usr/share/opensearch",
            "data_paths": ["/usr/share/opensearch/data"],
            "log_path": "/var/log/opensearch",
            "heap_dump_path": "/usr/share/opensearch/heapdump",
            # Docker container needs to expose service on external interfaces
            "network_host": "0.0.0.0",
            "discovery_type": "single-node",
            "http_port": str(node.port),
            "transport_port": str(node.port + 100),
            "cluster_settings": {}
        }

        config_vars = {}
        config_vars.update(self.provision_config_instance.variables["origin"]["docker"])
        config_vars.update(provisioner_defaults)

        return config_vars

    def _get_docker_vars(self, node, log_dir, heap_dump_dir, mounts):
        docker_vars = {
            "os_version": self.provision_config_instance.variables["origin"]["distribution"]["version"],
            "docker_image": self.provision_config_instance.variables["origin"]["docker"]["docker_image"],
            "http_port": node.port,
            "os_data_dir": node.data_paths[0],
            "os_log_dir": log_dir,
            "os_heap_dump_dir": heap_dump_dir,
            "mounts": mounts
        }
        self._add_if_defined_for_provision_config_instance(docker_vars, "docker_mem_limit")
        self._add_if_defined_for_provision_config_instance(docker_vars, "docker_cpu_count")
        return docker_vars

    def _add_if_defined_for_provision_config_instance(self, variables, key):
        if key in self.provision_config_instance.variables["origin"]["docker"]:
            variables[key] = self.provision_config_instance.variables["origin"]["docker"][key]

    def _render_template_from_docker_file(self, variables):
        compose_file = os.path.join(paths.benchmark_root(), "resources", "docker-compose.yml.j2")
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(io.dirname(compose_file)), autoescape=select_autoescape(['html', 'xml']))
        return self._render_template(env, variables, compose_file)

    def cleanup(self, host):
        self._cleanup(host, self.provision_config_instance.variables["preserve_install"])
