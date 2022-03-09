import sys
import uuid
from datetime import datetime
from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark import telemetry
from osbenchmark.builder import cluster
from osbenchmark.builder.launchers.docker_launcher import DockerLauncher
from osbenchmark.builder.provisioner import NodeConfiguration
from osbenchmark.exceptions import LaunchError
from osbenchmark.metrics import InMemoryMetricsStore


class DockerLauncherTests(TestCase):
    def setUp(self):
        self.executor = Mock()
        stop_watch = IterationBasedStopWatch(max_iterations=2)
        clock = TestClock(stop_watch=stop_watch)
        self.launcher = DockerLauncher(None, self.executor, clock=clock)

        self.host = None
        self.node_config = NodeConfiguration(build_type="docker",
                                        provision_config_instance_runtime_jdks="12,11",
                                        provision_config_instance_provides_bundled_jdk=True,
                                        ip="127.0.0.1", node_name="testnode",
                                        node_root_path="/tmp", binary_path="/bin",
                                        data_paths="/tmp")

    def test_starts_container_successfully(self):
        # [Start container (from docker-compose up), Docker container id (from docker-compose ps),
        # Docker container id (from docker ps --filter ...)]
        self.executor.execute.side_effect = [None, ["de604d0d"], ["de604d0d"]]

        nodes = self.launcher.start(self.host, [self.node_config])
        self.assertEqual(1, len(nodes))
        node = nodes[0]

        self.assertEqual(0, node.pid)
        self.assertEqual("/bin", node.binary_path)
        self.assertEqual("127.0.0.1", node.host_name)
        self.assertEqual("testnode", node.node_name)
        self.assertIsNotNone(node.telemetry)

        self.executor.execute.assert_has_calls([
            mock.call(self.host, "docker-compose -f /bin/docker-compose.yml up -d"),
            mock.call(self.host, "docker-compose -f /bin/docker-compose.yml ps -q", output=True),
            mock.call(self.host, 'docker ps -a --filter "id=de604d0d" --filter "status=running" --filter "health=healthy" -q', output=True)
        ])

    @mock.patch("osbenchmark.time.sleep")
    def test_container_not_started(self, sleep):
        # [Start container (from docker-compose up), Docker container id (from docker-compose ps),
        # but NO Docker container id (from docker ps --filter...) twice
        self.executor.execute.side_effect = [None, ["de604d0d"], [], []]

        with self.assertRaisesRegex(LaunchError, "No healthy running container after 600 seconds!"):
            self.launcher.start(self.host, [self.node_config])

    @mock.patch("osbenchmark.telemetry.add_metadata_for_node")
    def test_stops_container_successfully_with_metrics_store(self, add_metadata_for_node):
        metrics_store = Mock()

        nodes = [cluster.Node(0, "/bin", "127.0.0.1", "testnode", telemetry.Telemetry())]
        self.launcher.stop(self.host, nodes, metrics_store=metrics_store)

        add_metadata_for_node.assert_called_once_with(metrics_store, "testnode", "127.0.0.1")
        self.executor.execute.assert_called_once_with(self.host, "docker-compose -f /bin/docker-compose.yml down")

    @mock.patch("osbenchmark.telemetry.add_metadata_for_node")
    def test_stops_container_when_no_metrics_store_is_provided(self, add_metadata_for_node):
        metrics_store = None

        nodes = [cluster.Node(0, "/bin", "127.0.0.1", "testnode", telemetry.Telemetry())]
        self.launcher.stop(self.host, nodes, metrics_store=metrics_store)

        self.assertEqual(0, add_metadata_for_node.call_count)
        self.executor.execute.assert_called_once_with(self.host, "docker-compose -f /bin/docker-compose.yml down")


class IterationBasedStopWatch:
    __test__ = False

    def __init__(self, max_iterations):
        self.iterations = 0
        self.max_iterations = max_iterations

    def start(self):
        self.iterations = 0

    def split_time(self):
        if self.iterations < self.max_iterations:
            self.iterations += 1
            return 0
        else:
            return sys.maxsize


class TestClock:
    __test__ = False

    def __init__(self, stop_watch):
        self._stop_watch = stop_watch

    def stop_watch(self):
        return self._stop_watch


def get_metrics_store(cfg):
    ms = InMemoryMetricsStore(cfg)
    ms.open(test_ex_id=str(uuid.uuid4()),
            test_ex_timestamp=datetime.now(),
            workload_name="test",
            test_procedure_name="test",
            provision_config_instance_name="test")
    return ms
