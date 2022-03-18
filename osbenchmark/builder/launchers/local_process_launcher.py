import logging
import os

import psutil

from osbenchmark import time, telemetry
from osbenchmark.builder import java_resolver, cluster
from osbenchmark.builder.launchers.launcher import Launcher
from osbenchmark.utils import io, opts
from osbenchmark.utils.periodic_waiter import PeriodicWaiter


class LocalProcessLauncher(Launcher):
    PROCESS_WAIT_TIMEOUT_SECONDS = 90
    PROCESS_WAIT_INTERVAL_SECONDS = 0.5

    def __init__(self, pci, shell_executor, metrics_store, clock=time.Clock):
        super().__init__(shell_executor)
        self.logger = logging.getLogger(__name__)
        self.pci = pci
        self.metrics_store = metrics_store
        self.waiter = PeriodicWaiter(LocalProcessLauncher.PROCESS_WAIT_INTERVAL_SECONDS,
                                     LocalProcessLauncher.PROCESS_WAIT_TIMEOUT_SECONDS, clock=clock)

    def start(self, host, node_configurations):
        node_count_on_host = len(node_configurations)
        return [self._start_node(node_configuration, node_count_on_host) for node_configuration in node_configurations]

    def _start_node(self, node_configuration, node_count_on_host):
        host_name = node_configuration.ip
        node_name = node_configuration.node_name
        binary_path = node_configuration.binary_path
        data_paths = node_configuration.data_paths
        node_telemetry_dir = os.path.join(node_configuration.node_root_path, "telemetry")

        java_major_version, java_home = java_resolver.java_home(node_configuration.provision_config_instance_runtime_jdks,
                                                                self.pci.variables["builder"]["runtime"]["jdk"],
                                                                node_configuration.provision_config_instance_provides_bundled_jdk)
        self.logger.info("Java major version: %s", java_major_version)
        self.logger.info("Java home: %s", java_home)

        self.logger.info("Starting node [%s].", node_name)

        enabled_devices = self.pci.variables["telemetry"]["devices"]
        telemetry_params = self.pci.variables["telemetry"]["params"]
        node_telemetry = [
            telemetry.FlightRecorder(telemetry_params, node_telemetry_dir, java_major_version),
            telemetry.JitCompiler(node_telemetry_dir),
            telemetry.Gc(telemetry_params, node_telemetry_dir, java_major_version),
            telemetry.Heapdump(node_telemetry_dir),
            telemetry.DiskIo(node_count_on_host),
            telemetry.IndexSize(data_paths),
            telemetry.StartupTime(),
        ]

        t = telemetry.Telemetry(enabled_devices, devices=node_telemetry)
        env = self._prepare_env(node_name, java_home, t)
        t.on_pre_node_start(node_name)
        node_pid = self._start_process(binary_path, env)
        self.logger.info("Successfully started node [%s] with PID [%s].", node_name, node_pid)
        node = cluster.Node(node_pid, binary_path, host_name, node_name, t)

        self.logger.info("Attaching telemetry devices to node [%s].", node_name)
        t.attach_to_node(node)

        return node

    def _prepare_env(self, node_name, java_home, t):
        env = {k: v for k, v in os.environ.items() if k in opts.csv_to_list(self.pci.variables["system"]["env"]["passenv"])}
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
        for v in t.instrument_candidate_java_opts():
            self._set_env(env, "OPENSEARCH_JAVA_OPTS", v)

        self.logger.debug("env for [%s]: %s", node_name, str(env))
        return env

    def _set_env(self, env, k, v, separator=' ', prepend=False):
        if v is not None:
            if k not in env:
                env[k] = v
            elif prepend:
                env[k] = v + separator + env[k]
            else:
                env[k] = env[k] + separator + v

    @staticmethod
    def _run_subprocess(command_line, env):
        command_line_args = shlex.split(command_line)

        with subprocess.Popen(command_line_args,
                              stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL,
                              env=env,
                              start_new_session=True) as command_line_process:
            # wait for it to finish
            command_line_process.wait()

        return command_line_process.returncode

    def _start_process(self, binary_path, env):
        if os.name == "posix" and os.geteuid() == 0:
            raise exceptions.LaunchError("Cannot launch OpenSearch as root. Please run Benchmark as a non-root user.")
        os.chdir(binary_path)
        cmd = [io.escape_path(os.path.join(".", "bin", "opensearch"))]
        cmd.extend(["-d", "-p", "pid"])
        ret = ProcessLauncher._run_subprocess(command_line=" ".join(cmd), env=env)
        if ret != 0:
            msg = "Daemon startup failed with exit code [{}]".format(ret)
            logging.error(msg)
            raise exceptions.LaunchError(msg)

        return self._wait_for_pidfile(io.escape_path(os.path.join(".", "pid")))

    def _wait_for_pidfile(self, pidfilename, timeout=60, clock=time.Clock):
        stop_watch = clock.stop_watch()
        stop_watch.start()
        while stop_watch.split_time() < timeout:
            try:
                with open(pidfilename, "rb") as f:
                    buf = f.read()
                    if not buf:
                        raise EOFError
                    return int(buf)
            except (FileNotFoundError, EOFError):
                time.sleep(0.5)

        msg = "pid file not available after {} seconds!".format(timeout)
        logging.error(msg)
        raise exceptions.LaunchError(msg)

    def stop(self, host, nodes):
        self.logger.info("Shutting down [%d] nodes on this host.", len(nodes))
        stopped_nodes = []
        for node in nodes:
            node_name = node.node_name
            if self.metrics_store:
                telemetry.add_metadata_for_node(self.metrics_store, node_name, node.host_name)
            try:
                opensearch = psutil.Process(pid=node.pid)
                node.telemetry.detach_from_node(node, running=True)
            except psutil.NoSuchProcess:
                self.logger.warning("No process found with PID [%s] for node [%s].", node.pid, node_name)
                opensearch = None

            if opensearch:
                try:
                    opensearch.terminate()
                    opensearch.wait(10.0)
                    stopped_nodes.append(node)
                except psutil.NoSuchProcess:
                    self.logger.warning("No process found with PID [%s] for node [%s].", opensearch.pid, node_name)
                except psutil.TimeoutExpired:
                    self.logger.info("kill -KILL node [%s]", node_name)
                    try:
                        # kill -9
                        opensearch.kill()
                        stopped_nodes.append(node)
                    except psutil.NoSuchProcess:
                        self.logger.warning("No process found with PID [%s] for node [%s].", opensearch.pid, node_name)
                self.logger.info("Done shutting down node [%s].", node_name)

                node.telemetry.detach_from_node(node, running=False)
            # store system metrics in any case (telemetry devices may derive system metrics while the node is running)
            if self.metrics_store:
                node.telemetry.store_system_metrics(node, self.metrics_store)
        return stopped_nodes