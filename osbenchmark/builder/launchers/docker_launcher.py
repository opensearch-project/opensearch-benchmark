import os

from osbenchmark import time, telemetry
from osbenchmark.builder import cluster
from osbenchmark.builder.launchers.launcher import Launcher
from osbenchmark.exceptions import LaunchError, ExecutorError


class DockerLauncher(Launcher):
    # May download a Docker image and that can take some time
    PROCESS_WAIT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, pci, executor, clock=time.Clock):
        super().__init__(executor)
        self.clock = clock

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
            node = cluster.Node(0, binary_path, host_name, node_name, t)
            t.attach_to_node(node)
            nodes.append(node)
        return nodes

    def _start_process(self, host, binary_path):
        compose_cmd = self._docker_compose(binary_path, "up -d")

        try:
            self.executor.execute(host, compose_cmd)
        except ExecutorError as e:
            raise LaunchError("Exception starting OpenSearch Docker container", e)

        container_id = self._get_container_id(host, binary_path)
        self._wait_for_healthy_running_container(host, container_id, DockerLauncher.PROCESS_WAIT_TIMEOUT_SECONDS)

    def _docker_compose(self, compose_config, cmd):
        return "docker-compose -f {} {}".format(os.path.join(compose_config, "docker-compose.yml"), cmd)

    def _get_container_id(self, host, compose_config):
        compose_ps_cmd = self._docker_compose(compose_config, "ps -q")
        return self.executor.execute(host, compose_ps_cmd, output=True)[0]

    def _wait_for_healthy_running_container(self, host, container_id, timeout):
        cmd = 'docker ps -a --filter "id={}" --filter "status=running" --filter "health=healthy" -q'.format(container_id)
        stop_watch = self.clock.stop_watch()
        stop_watch.start()
        while stop_watch.split_time() < timeout:
            containers = self.executor.execute(host, cmd, output=True)
            if len(containers) > 0:
                return
            time.sleep(0.5)
        msg = "No healthy running container after {} seconds!".format(timeout)
        self.logger.error(msg)
        raise LaunchError(msg)

    def stop(self, host, nodes, metrics_store):
        self.logger.info("Shutting down [%d] nodes running in Docker on this host.", len(nodes))
        for node in nodes:
            self.logger.info("Stopping node [%s].", node.node_name)
            if metrics_store:
                telemetry.add_metadata_for_node(metrics_store, node.node_name, node.host_name)
            node.telemetry.detach_from_node(node, running=True)
            self.executor.execute(host, self._docker_compose(node.binary_path, "down"))
            node.telemetry.detach_from_node(node, running=False)
            if metrics_store:
                node.telemetry.store_system_metrics(node, metrics_store)
