# pylint: disable=protected-access

import os
from unittest import TestCase, mock
from unittest.mock import Mock, mock_open

from psutil import NoSuchProcess

from osbenchmark import telemetry
from osbenchmark.builder import cluster
from osbenchmark.builder.launchers.local_process_launcher import LocalProcessLauncher
from osbenchmark.builder.provision_config import ProvisionConfigInstance
from osbenchmark.builder.provisioner import NodeConfiguration


class LocalProcessLauncherTests(TestCase):
    def setUp(self):
        self.shell_executor = Mock()
        self.metrics_store = Mock()

        self.variables = {
            "system": {
                "runtime": {
                    "jdk": None
                },
                "env": {
                    "passenv": "PATH"
                }
            },
            "telemetry": {
                "devices": [],
                "params": None
            }
        }
        self.cluster_config = ProvisionConfigInstance("fake_cluster_config", "/path/to/root",
                                                                 ["/path/to/config"], variables=self.variables)

        self.launcher = LocalProcessLauncher(self.cluster_config, self.shell_executor, self.metrics_store)
        self.launcher.waiter = Mock()
        self.host = None
        self.path = "fake"

    @mock.patch('osbenchmark.builder.java_resolver.java_home', return_value=(12, "/java_home/"))
    @mock.patch('osbenchmark.utils.jvm.supports_option', return_value=True)
    @mock.patch('osbenchmark.utils.io.get_size')
    @mock.patch('osbenchmark.telemetry')
    @mock.patch('psutil.Process')
    def test_daemon_start_stop(self, process, telemetry, get_size, supports, java_home):
        mo = mock_open(read_data="1234")

        node_configs = []
        for node in range(2):
            node_configs.append(NodeConfiguration(build_type="tar",
                                                  cluster_config_runtime_jdks="12,11",
                                                  cluster_config_provides_bundled_jdk=True,
                                                  ip="127.0.0.1",
                                                  node_name=f"testnode-{node}",
                                                  node_root_path="/tmp",
                                                  binary_path="/tmp",
                                                  data_paths="/tmp"))

        with mock.patch("builtins.open", mo):
            nodes = self.launcher.start(self.host, node_configs)

        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0].pid, 1234)

        stopped_nodes = self.launcher.stop(self.host, nodes)
        # all nodes should be stopped
        self.assertEqual(nodes, stopped_nodes)

    @mock.patch('psutil.Process')
    def test_daemon_stop_with_already_terminated_process(self, process):
        process.side_effect = NoSuchProcess(123)

        nodes = [
            cluster.Node(pid=-1,
                         binary_path="/bin",
                         host_name="localhost",
                         node_name="benchmark-0",
                         telemetry=telemetry.Telemetry())
        ]

        stopped_nodes = self.launcher.stop(self.host, nodes)
        # no nodes should have been stopped (they were already stopped)
        self.assertEqual([], stopped_nodes)

    # flight recorder shows a warning for several seconds before continuing
    @mock.patch("osbenchmark.time.sleep")
    def test_env_options_order(self, sleep):
        node_telemetry = [
            telemetry.FlightRecorder(telemetry_params={}, log_root="/tmp/telemetry", java_major_version=8)
        ]
        telem = telemetry.Telemetry(["jfr"], devices=node_telemetry)
        env = self.launcher._prepare_env(node_name="node0", java_home="/java_home", telemetry=telem)

        self.assertEqual("/java_home/bin" + os.pathsep + os.environ["PATH"], env["PATH"])
        self.assertEqual("-XX:+ExitOnOutOfMemoryError -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints "
                         "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder "
                         "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,dumponexitpath=/tmp/telemetry/profile.jfr "  # pylint: disable=line-too-long
                         "-XX:StartFlightRecording=defaultrecording=true", env["OPENSEARCH_JAVA_OPTS"])

    def test_bundled_jdk_not_in_path(self):
        os.environ["JAVA_HOME"] = "/path/to/java"

        telem = telemetry.Telemetry()
        # no JAVA_HOME -> use the bundled JDK
        env = self.launcher._prepare_env(node_name="node0", java_home=None, telemetry=telem)

        # unmodified
        self.assertEqual(os.environ["PATH"], env["PATH"])
        self.assertIsNone(env.get("JAVA_HOME"))

    def test_pass_env_vars(self):
        self.cluster_config.variables["system"]["env"]["passenv"] = "JAVA_HOME,FOO1"

        os.environ["JAVA_HOME"] = "/path/to/java"
        os.environ["FOO1"] = "BAR1"

        telem = telemetry.Telemetry()
        # no JAVA_HOME -> use the bundled JDK
        env = self.launcher._prepare_env(node_name="node0", java_home=None, telemetry=telem)

        # unmodified
        self.assertEqual(os.environ["JAVA_HOME"], env["JAVA_HOME"])
        self.assertEqual(os.environ["FOO1"], env["FOO1"])
        self.assertEqual(env["OPENSEARCH_JAVA_OPTS"], "-XX:+ExitOnOutOfMemoryError")

    def test_pass_java_opts(self):
        self.cluster_config.variables["system"]["env"]["passenv"] = "OPENSEARCH_JAVA_OPTS"

        os.environ["OPENSEARCH_JAVA_OPTS"] = "-XX:-someJunk"

        telem = telemetry.Telemetry()
        # no JAVA_HOME -> use the bundled JDK
        env = self.launcher._prepare_env(node_name="node0", java_home=None, telemetry=telem)

        # unmodified
        self.assertEqual(os.environ["OPENSEARCH_JAVA_OPTS"], env["OPENSEARCH_JAVA_OPTS"])

    def test_pid_file_not_created(self):
        mo = mock_open()

        with mock.patch("builtins.open", mo):
            mo.side_effect = FileNotFoundError

            is_pid_file_ready = self.launcher._is_pid_file_available("fake")
            self.assertEqual(is_pid_file_ready, False)

    def test_pid_file_empty(self):
        mo = mock_open(read_data="")

        with mock.patch("builtins.open", mo):
            is_pid_file_ready = self.launcher._is_pid_file_available("fake")
            self.assertEqual(is_pid_file_ready, False)

    def test_pid_file_ready(self):
        mo = mock_open(read_data="1234")

        with mock.patch("builtins.open", mo):
            is_pid_file_ready = self.launcher._is_pid_file_available("fake")
            self.assertEqual(is_pid_file_ready, True)
