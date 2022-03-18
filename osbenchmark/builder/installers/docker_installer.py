import logging
import os
import uuid

import jinja2
from jinja2 import select_autoescape

from osbenchmark.builder.installers.installer import Installer


class DockerInstaller(Installer):
    def __init__(self, pci, executor, node_name, ip, http_port, node_root_dir, distribution_version, benchmark_root, ):
        super().__init__(executor)
        self.logger = logging.getLogger(__name__)
        self.pci = pci

        self.node_name = node_name
        self.node_ip = ip
        self.http_port = http_port
        self.node_root_dir = node_root_dir
        self.node_log_dir = os.path.join(node_root_dir, "logs", "server")
        self.heap_dump_dir = os.path.join(node_root_dir, "heapdump")
        self.distribution_version = distribution_version
        self.benchmark_root = benchmark_root
        self.binary_path = os.path.join(node_root_dir, "install")
        # use a random subdirectory to isolate multiple runs because an external (non-root) user cannot clean it up.
        self.data_paths = [os.path.join(node_root_dir, "data", str(uuid.uuid4()))]


        provisioner_defaults = {
            "cluster_name": "benchmark-provisioned-cluster",
            "node_name": self.node_name,
            # we bind-mount the directories below on the host to these ones.
            "install_root_path": "/usr/share/opensearch",
            "data_paths": ["/usr/share/opensearch/data"],
            "log_path": "/var/log/opensearch",
            "heap_dump_path": "/usr/share/opensearch/heapdump",
            # Docker container needs to expose service on external interfaces
            "network_host": "0.0.0.0",
            "discovery_type": "single-node",
            "http_port": str(self.http_port),
            "transport_port": str(self.http_port + 100),
            "cluster_settings": {}
        }

        self.config_vars = {}
        self.config_vars.update(self.provision_config_instance.variables)
        self.config_vars.update(provisioner_defaults)

    def install(self, host, binaries):
        self._prepare(host)

    def _prepare(self, host):
        # we need to allow other users to write to these directories due to Docker.
        #
        # Although os.mkdir passes 0o777 by default, mkdir(2) uses `mode & ~umask & 0777` to determine the final flags and
        # hence we need to modify the process' umask here. For details see https://linux.die.net/man/2/mkdir.
        previous_umask = os.umask(0)
        try:
            # How to handle directory creation on remote machines?
            io.ensure_dir(self.binary_path)
            io.ensure_dir(self.node_log_dir)
            io.ensure_dir(self.heap_dump_dir)
            io.ensure_dir(self.data_paths[0])
        finally:
            os.umask(previous_umask)

        mounts = {}

        for provision_config_instance_config_path in self.pci.config_paths:
            for root, _, files in os.walk(provision_config_instance_config_path):
                env = jinja2.Environment(loader=jinja2.FileSystemLoader(root), autoescape=select_autoescape(['html', 'xml']))

                relative_root = root[len(provision_config_instance_config_path) + 1:]
                absolute_target_root = os.path.join(self.binary_path, relative_root)
                io.ensure_dir(absolute_target_root)

                for name in files:
                    source_file = os.path.join(root, name)
                    target_file = os.path.join(absolute_target_root, name)
                    mounts[target_file] = os.path.join("/usr/share/opensearch", relative_root, name)
                    if io.is_plain_text(source_file):
                        config_vars = self._config_vars(host.nodes[0].name, host.nodes[0].port)

                        self.logger.info("Reading config template file [%s] and writing to [%s].", source_file, target_file)
                        with open(target_file, mode="a", encoding="utf-8") as f:
                            f.write(self._render_template(env, config_vars, source_file))
                    else:
                        self.logger.info("Treating [%s] as binary and copying as is to [%s].", source_file, target_file)
                        self.executor.copy(source_file, target_file)

        docker_cfg = self._render_template_from_docker_file(self._docker_vars(mounts, host.nodes[0].port))
        self.logger.info("Starting Docker container with configuration:\n%s", docker_cfg)

        with open(os.path.join(self.binary_path, "docker-compose.yml"), mode="wt", encoding="utf-8") as f:
            f.write(docker_cfg)

        # Loop over nodes/host? Docker can only have 1 node per host due to single node discovery?
        return NodeConfiguration("docker", self.pci.variables["system"]["runtime"]["jdk"],
                                 convert.to_bool(self.pci.variables["system"]["runtime"]["jdk"]["bundled"]), host.ip,
                                 host.nodes[0].name, self.node_root_dir, self.binary_path, self.data_paths)

    def _config_vars(self, node_name, port):
        provisioner_defaults = {
            "cluster_name": "benchmark-provisioned-cluster",
            "node_name": node_name,
            # we bind-mount the directories below on the host to these ones.
            "install_root_path": "/usr/share/opensearch",
            "data_paths": ["/usr/share/opensearch/data"],
            "log_path": "/var/log/opensearch",
            "heap_dump_path": "/usr/share/opensearch/heapdump",
            # Docker container needs to expose service on external interfaces
            "network_host": "0.0.0.0",
            "discovery_type": "single-node",
            "http_port": str(port),
            "transport_port": str(port + 100),
            "cluster_settings": {}
        }

        config_vars = {}
        config_vars.update(self.pci.variables)
        config_vars.update(provisioner_defaults)

        return config_vars

    def _docker_vars(self, port, mounts):
        v = {
            "os_version": self.pci.variables["origin"]["distribution"]["version"],
            "docker_image": self.pci.variables["origin"]["docker"]["image"],
            "http_port": port,
            "os_data_dir": self.data_paths[0],
            "os_log_dir": self.node_log_dir,
            "os_heap_dump_dir": self.heap_dump_dir,
            "mounts": mounts
        }
        self._add_if_defined_for_provision_config_instance(v, "mem_limit")
        self._add_if_defined_for_provision_config_instance(v, "cpu_count")
        return v

    def _add_if_defined_for_provision_config_instance(self, variables, key):
        if key in self.pci.variables["origin"]["docker"]:
            variables[key] = self.pci.variables["origin"]["docker"][key]

    def _render_template_from_docker_file(self, variables):
        compose_file = os.path.join(self.benchmark_root, "resources", "docker-compose.yml.j2")
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(io.dirname(compose_file)), autoescape=select_autoescape(['html', 'xml']))
        return self._render_template(env, variables, compose_file)

    def cleanup(self, host, node_configurations):
        pass