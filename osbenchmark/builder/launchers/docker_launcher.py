import logging
import os

from osbenchmark import telemetry, time
from osbenchmark.builder import cluster
from osbenchmark.builder.launchers.launcher import Launcher
from osbenchmark.utils.periodic_waiter import PeriodicWaiter


class DockerLauncher(Launcher):
    # May download a Docker image and that can take some time
    CONTAINER_WAIT_TIMEOUT_SECONDS = 10 * 60
    CONTAINER_WAIT_INTERVAL_SECONDS = 0.5

    def __init__(self, provision_config_instance, shell_executor, metrics_store, clock=time.Clock):
        super().__init__(shell_executor)
        self.logger = logging.getLogger(__name__)
        self.metrics_store = metrics_store
        self.waiter = PeriodicWaiter(DockerLauncher.CONTAINER_WAIT_INTERVAL_SECONDS,
                                     DockerLauncher.CONTAINER_WAIT_TIMEOUT_SECONDS, clock=clock)

    def start(self, host, node_configurations):
        nodes = []
        for node_configuration in node_configurations:
            node_name = node_configuration.node_name
            host_name = node_configuration.ip
            binary_path = node_configuration.binary_path
            self.logger.info("Starting node [%s] in Docker.", node_name)
            self._start_process(host, binary_path)
            node_telemetry = [
                # Don't attach any telemetry devices for now but keep the infrastructure in place
            ]
            t = telemetry.Telemetry(devices=node_telemetry)
            node = cluster.Node(None, binary_path, host_name, node_name, t)
            t.attach_to_node(node)
            nodes.append(node)
        return nodes

    def _start_process(self, host, binary_path):
        compose_cmd = self._docker_compose(binary_path, "up -d")
        self.shell_executor.execute(host, compose_cmd)

        container_id = self._get_container_id(host, binary_path)
        self._wait_for_healthy_running_container(host, container_id)

    def _docker_compose(self, compose_config, cmd):
        docker_compose_file = self._get_docker_compose_file(compose_config)

        return f"docker-compose -f {docker_compose_file} {cmd}"

    def _get_docker_compose_file(self, compose_config):
        return os.path.join(compose_config, "docker-compose.yml")

    def _get_container_id(self, host, compose_config):
        compose_ps_cmd = self._docker_compose(compose_config, "ps -q")
        return self.shell_executor.execute(host, compose_ps_cmd, output=True)[0]

    def _wait_for_healthy_running_container(self, host, container_id):
        self.waiter.wait(self._is_container_healthy, host, container_id)

    def _is_container_healthy(self, host, container_id):
        cmd = f'docker ps -a --filter "id={container_id}" --filter "status=running" --filter "health=healthy" -q'
        containers = self.shell_executor.execute(host, cmd, output=True)
        return len(containers) > 0

    def stop(self, host, nodes):
        self.logger.info("Shutting down [%d] nodes running in Docker on this host.", len(nodes))
        for node in nodes:
            self.logger.info("Stopping node [%s].", node.node_name)
            if self.metrics_store:
                telemetry.add_metadata_for_node(self.metrics_store, node.node_name, node.host_name)
            node.telemetry.detach_from_node(node, running=True)
            self.shell_executor.execute(host, self._docker_compose(node.binary_path, "down"))
            node.telemetry.detach_from_node(node, running=False)
            if self.metrics_store:
                node.telemetry.store_system_metrics(node, self.metrics_store)
