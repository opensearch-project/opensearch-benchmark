import logging
import os
import subprocess

import psutil

from osbenchmark import time, telemetry
from osbenchmark.builder import java_resolver, cluster
from osbenchmark.builder.launchers.launcher import Launcher
from osbenchmark.exceptions import LaunchError
from osbenchmark.utils import io, opts
from osbenchmark.utils.periodic_waiter import PeriodicWaiter


class LocalProcessLauncher(Launcher):
    PROCESS_WAIT_TIMEOUT_SECONDS = 90
    PROCESS_WAIT_INTERVAL_SECONDS = 0.5

    def __init__(self, provision_config_instance, shell_executor, metrics_store, clock=time.Clock):
        super().__init__(shell_executor)
        self.logger = logging.getLogger(__name__)
        self.provision_config_instance = provision_config_instance
        self.metrics_store = metrics_store
        self.waiter = PeriodicWaiter(LocalProcessLauncher.PROCESS_WAIT_INTERVAL_SECONDS,
                                     LocalProcessLauncher.PROCESS_WAIT_TIMEOUT_SECONDS, clock=clock)

    def start(self, host, node_configurations):
        node_count_on_host = len(node_configurations)
        return [self._start_node(host, node_configuration, node_count_on_host) for node_configuration in node_configurations]

    def _start_node(self, host, node_configuration, node_count_on_host):
        host_name = node_configuration.ip
        node_name = node_configuration.node_name
        binary_path = node_configuration.binary_path

        java_major_version, java_home = java_resolver.java_home(node_configuration.provision_config_instance_runtime_jdks,
                                                                self.provision_config_instance.variables["system"]["runtime"]["jdk"],
                                                                node_configuration.provision_config_instance_provides_bundled_jdk)
        self.logger.info("Java major version: %s", java_major_version)
        self.logger.info("Java home: %s", java_home)
        self.logger.info("Starting node [%s].", node_name)

        telemetry = self._prepare_telemetry(node_configuration, node_count_on_host, java_major_version)
        env = self._prepare_env(node_name, java_home, telemetry)
        telemetry.on_pre_node_start(node_name)

        node_pid = self._start_process(host, binary_path, env)
        self.logger.info("Successfully started node [%s] with PID [%s].", node_name, node_pid)
        node = cluster.Node(node_pid, binary_path, host_name, node_name, telemetry)

        self.logger.info("Attaching telemetry devices to node [%s].", node_name)
        telemetry.attach_to_node(node)

        return node

    def _prepare_telemetry(self, node_configuration, node_count_on_host, java_major_version):
        data_paths = node_configuration.data_paths
        node_telemetry_dir = os.path.join(node_configuration.node_root_path, "telemetry")

        enabled_devices = self.provision_config_instance.variables["telemetry"]["devices"]
        telemetry_params = self.provision_config_instance.variables["telemetry"]["params"]

        node_telemetry = [
            telemetry.FlightRecorder(telemetry_params, node_telemetry_dir, java_major_version),
            telemetry.JitCompiler(node_telemetry_dir),
            telemetry.Gc(telemetry_params, node_telemetry_dir, java_major_version),
            telemetry.Heapdump(node_telemetry_dir),
            telemetry.DiskIo(node_count_on_host),
            telemetry.IndexSize(data_paths),
            telemetry.StartupTime(),
        ]

        return telemetry.Telemetry(enabled_devices, devices=node_telemetry)

    def _prepare_env(self, node_name, java_home, telemetry):
        env = {k: v for k, v in os.environ.items() if k in
               opts.csv_to_list(self.provision_config_instance.variables["system"]["env"]["passenv"])}
        if java_home:
            self._set_env(env, "PATH", os.path.join(java_home, "bin"), separator=os.pathsep, prepend=True)
            # This property is the higher priority starting in ES 7.12.0, and is the only supported java home in >=8.0
            env["OPENSEARCH_JAVA_HOME"] = java_home
            # TODO remove this when ES <8.0 becomes unsupported by Benchmark
            env["JAVA_HOME"] = java_home
            self.logger.info("JAVA HOME: %s", env["JAVA_HOME"])
        if not env.get("OPENSEARCH_JAVA_OPTS"):
            env["OPENSEARCH_JAVA_OPTS"] = "-XX:+ExitOnOutOfMemoryError"

        # we just blindly trust telemetry here...
        for jvm_option in telemetry.instrument_candidate_java_opts():
            self._set_env(env, "OPENSEARCH_JAVA_OPTS", jvm_option)

        self.logger.debug("env for [%s]: %s", node_name, str(env))
        return env

    def _set_env(self, env, key, value, separator=' ', prepend=False):
        if value is not None:
            if key not in env:
                env[key] = value
            elif prepend:
                env[key] = value + separator + env[key]
            else:
                env[key] = env[key] + separator + value

    def _start_process(self, host, binary_path, env):
        if os.name == "posix" and os.geteuid() == 0:
            raise LaunchError("Cannot launch OpenSearch as root. Please run Benchmark as a non-root user.")

        cmd = [io.escape_path(os.path.join(binary_path, "bin", "opensearch"))]
        cmd.extend(["-d", "-p", "pid"])

        self.shell_executor.execute(host, " ".join(cmd), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, detach=True)

        pid_file_name = io.escape_path(os.path.join(binary_path, "pid"))
        self._wait_for_pid_file(pid_file_name)

        return self._get_pid_from_file(pid_file_name)

    def _wait_for_pid_file(self, pid_file_name):
        self.waiter.wait(self._is_pid_file_available, pid_file_name)

    def _is_pid_file_available(self, pid_file_name):
        try:
            pid = self._get_pid_from_file(pid_file_name)
            return pid != 0
        except (FileNotFoundError, EOFError):
            self.logger.info("PID file %s is not ready", pid_file_name)
            return False

    def _get_pid_from_file(self, pid_file_name):
        with open(pid_file_name, "rb") as f:
            buf = f.read()
            if not buf:
                raise EOFError
            return int(buf)

    def stop(self, host, nodes):
        self.logger.info("Shutting down [%d] nodes on this host.", len(nodes))
        stopped_nodes = []
        for node in nodes:
            node_stopped = self._stop_node(node)
            if node_stopped:
                stopped_nodes.append(node)

        return stopped_nodes

    def _stop_node(self, node):
        node_stopped = False

        if self.metrics_store:
            telemetry.add_metadata_for_node(self.metrics_store, node.node_name, node.host_name)

        opensearch_process = self._get_opensearch_process(node)
        if opensearch_process:
            node.telemetry.detach_from_node(node, running=True)
            node_stopped = self._stop_process(opensearch_process, node)
            node.telemetry.detach_from_node(node, running=False)
        # store system metrics in any case (telemetry devices may derive system metrics while the node is running)
        if self.metrics_store:
            node.telemetry.store_system_metrics(node, self.metrics_store)

        return node_stopped

    def _get_opensearch_process(self, node):
        try:
            return psutil.Process(pid=node.pid)
        except psutil.NoSuchProcess:
            self.logger.warning("No process found with PID [%s] for node [%s].", node.pid, node.node_name)

    def _stop_process(self, opensearch_process, node):
        process_stopped = False

        try:
            opensearch_process.terminate()
            opensearch_process.wait(10.0)
            process_stopped = True
        except psutil.NoSuchProcess:
            self.logger.warning("No process found with PID [%s] for node [%s].", opensearch_process.pid, node.node_name)
        except psutil.TimeoutExpired:
            self.logger.info("kill -KILL node [%s]", node.node_name)
            try:
                # kill -9
                opensearch_process.kill()
                process_stopped = True
            except psutil.NoSuchProcess:
                self.logger.warning("No process found with PID [%s] for node [%s].", opensearch_process.pid, node.node_name)
        self.logger.info("Done shutting down node [%s].", node.node_name)

        return process_stopped
