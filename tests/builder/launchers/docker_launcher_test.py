# pylint: disable=protected-access

from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark import telemetry
from osbenchmark.builder import cluster
from osbenchmark.builder.launchers.docker_launcher import DockerLauncher
from osbenchmark.builder.provisioner import NodeConfiguration


class DockerLauncherTests(TestCase):
    def setUp(self):
        self.shell_executor = Mock()
        self.metrics_store = Mock()

        self.launcher = DockerLauncher(None, self.shell_executor, self.metrics_store)
        self.launcher.waiter = Mock()

        self.host = None
        self.node_config = NodeConfiguration(build_type="docker",
                                        cluster_config_runtime_jdks="12,11",
                                        cluster_config_provides_bundled_jdk=True,
                                        ip="127.0.0.1", node_name="testnode",
                                        node_root_path="/tmp", binary_path="/bin",
                                        data_paths="/tmp")

    def test_starts_container_successfully(self):
        # [Start container (from docker-compose up), Docker container id (from docker-compose ps),
        self.shell_executor.execute.side_effect = [None, ["de604d0d"]]
        self.launcher.waiter.wait.return_value = None

        nodes = self.launcher.start(self.host, [self.node_config])
        self.assertEqual(1, len(nodes))
        node = nodes[0]

        self.assertEqual(None, node.pid)
        self.assertEqual("/bin", node.binary_path)
        self.assertEqual("127.0.0.1", node.host_name)
        self.assertEqual("testnode", node.node_name)
        self.assertIsNotNone(node.telemetry)

        self.shell_executor.execute.assert_has_calls([
            mock.call(self.host, "docker-compose -f /bin/docker-compose.yml up -d"),
            mock.call(self.host, "docker-compose -f /bin/docker-compose.yml ps -q", output=True),
        ])

    def test_container_not_started(self):
        # [Start container (from docker-compose up), Docker container id (from docker-compose ps),
        self.shell_executor.execute.side_effect = [None, ["de604d0d"]]
        self.launcher.waiter.wait.side_effect = TimeoutError

        with self.assertRaises(TimeoutError):
            self.launcher.start(self.host, [self.node_config])

    @mock.patch("osbenchmark.telemetry.add_metadata_for_node")
    def test_stops_container_successfully_with_metrics_store(self, add_metadata_for_node):
        nodes = [cluster.Node(0, "/bin", "127.0.0.1", "testnode", telemetry.Telemetry())]
        self.launcher.stop(self.host, nodes)

        add_metadata_for_node.assert_called_once_with(self.metrics_store, "testnode", "127.0.0.1")
        self.shell_executor.execute.assert_called_once_with(self.host, "docker-compose -f /bin/docker-compose.yml down")

    @mock.patch("osbenchmark.telemetry.add_metadata_for_node")
    def test_stops_container_when_no_metrics_store_is_provided(self, add_metadata_for_node):
        self.launcher.metrics_store = None

        nodes = [cluster.Node(0, "/bin", "127.0.0.1", "testnode", telemetry.Telemetry())]
        self.launcher.stop(self.host, nodes)

        self.assertEqual(0, add_metadata_for_node.call_count)
        self.shell_executor.execute.assert_called_once_with(self.host, "docker-compose -f /bin/docker-compose.yml down")

    def test_container_not_healthy(self):
        self.shell_executor.execute.return_value = []
        output = self.launcher._is_container_healthy(self.host, "de604d0d")

        self.assertEqual(output, False)
        self.shell_executor.execute.assert_has_calls([
            mock.call(self.host, 'docker ps -a --filter "id=de604d0d" --filter "status=running" --filter "health=healthy" -q', output=True)
        ])

    def test_container_healthy(self):
        self.shell_executor.execute.return_value = ["We have a container"]
        output = self.launcher._is_container_healthy(self.host, "de604d0d")

        self.assertEqual(output, True)
        self.shell_executor.execute.assert_has_calls([
            mock.call(self.host, 'docker ps -a --filter "id=de604d0d" --filter "status=running" --filter "health=healthy" -q', output=True)
        ])
