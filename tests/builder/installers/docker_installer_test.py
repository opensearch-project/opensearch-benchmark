# pylint: disable=protected-access

import os
import tempfile
from unittest import TestCase, mock
from unittest.mock import Mock

from osbenchmark.builder.installers.docker_installer import DockerInstaller
from osbenchmark.builder.provision_config import ProvisionConfigInstance


class DockerProvisionerTests(TestCase):
    def setUp(self):
        self.host = None
        self.binaries = None
        self.node_name = "9dbc682e-d32a-4669-8fbe-56fb77120dd4"
        self.cluster_name = "my-cluster"
        self.port = "39200"
        self.test_execution_root = tempfile.gettempdir()
        self.node_root_dir = os.path.join(self.test_execution_root, self.node_name)
        self.node_data_dir = os.path.join(self.node_root_dir, "data", self.node_name)
        self.node_log_dir = os.path.join(self.node_root_dir, "logs", "server")
        self.node_heap_dump_dir = os.path.join(self.node_root_dir, "heapdump")

        self.executor = Mock()
        self.cluster_config = ProvisionConfigInstance(
            names="fake",
            root_path=None,
            config_paths="/tmp",
            variables={
                "cluster_name": self.cluster_name,
                "test_execution_root": self.test_execution_root,
                "node": {
                    "port": self.port
                },
                "origin": {
                    "distribution": {
                        "version": "1.1.0"
                    },
                    "docker": {
                        "docker_image": "opensearchproject/opensearch"
                    }
                }
            }
        )

        self.installer = DockerInstaller(self.cluster_config, self.executor)

    maxDiff = None
    @mock.patch("uuid.uuid4")
    @mock.patch("osbenchmark.paths.benchmark_root")
    def test_provisioning_with_defaults(self, benchmark_root, uuid4):
        uuid4.return_value = self.node_name
        benchmark_root.return_value = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                                    os.pardir, os.pardir, os.pardir, "osbenchmark"))

        node = self.installer._create_node()

        self.assertDictEqual({
            "cluster_name": self.cluster_name,
            "node_name": self.node_name,
            "install_root_path": "/usr/share/opensearch",
            "data_paths": ["/usr/share/opensearch/data"],
            "log_path": "/var/log/opensearch",
            "heap_dump_path": "/usr/share/opensearch/heapdump",
            "discovery_type": "single-node",
            "network_host": "0.0.0.0",
            "http_port": self.port,
            "transport_port": str(int(self.port) + 100),
            "cluster_settings": {
            },
            "docker_image": "opensearchproject/opensearch"
        }, self.installer._get_config_vars(node))

        docker_vars = self.installer._get_docker_vars(node, mounts={})
        self.assertDictEqual({
            "os_data_dir": self.node_data_dir,
            "os_log_dir": self.node_log_dir,
            "os_heap_dump_dir": self.node_heap_dump_dir,
            "os_version": "1.1.0",
            "docker_image": "opensearchproject/opensearch",
            "http_port": 39200,
            "mounts": {}
        }, docker_vars)

        docker_cfg = self.installer._render_template_from_docker_file(docker_vars)

        self.assertEqual(
"""version: '3'
services:
  opensearch-node1:
    image: opensearchproject/opensearch:1.1.0
    container_name: opensearch-node1
    labels:
      io.benchmark.description: "opensearch-benchmark"
    environment:
      - cluster.name=opensearch-cluster
      - node.name=opensearch-node1
      - discovery.seed_hosts=opensearch-node1
      - DISABLE_INSTALL_DEMO_CONFIG=true
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    volumes:
      - %s:/usr/share/opensearch/data
      - %s:/var/log/opensearch
      - %s:/usr/share/opensearch/heapdump
    ports:
      - 39200:39200
      - 9200:9200
      - 9600:9600
    networks:
      - opensearch-net
    healthcheck:
          test: curl -f http://localhost:39200 -u admin:admin --insecure
          interval: 5s
          timeout: 2s
          retries: 10

volumes:
  opensearch-data1:
networks:
  opensearch-net:
""" % (self.node_data_dir, self.node_log_dir, self.node_heap_dump_dir), docker_cfg)

    @mock.patch("uuid.uuid4")
    @mock.patch("osbenchmark.paths.benchmark_root")
    def test_provisioning_with_variables(self, benchmark_root, uuid4):
        uuid4.return_value = self.node_name
        benchmark_root.return_value = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                                    os.pardir, os.pardir, os.pardir, "osbenchmark"))

        self.cluster_config.variables["origin"]["docker"]["docker_mem_limit"] = "256m"
        self.cluster_config.variables["origin"]["docker"]["docker_cpu_count"] = 2

        node = self.installer._create_node()

        docker_vars = self.installer._get_docker_vars(node, mounts={})
        docker_cfg = self.installer._render_template_from_docker_file(docker_vars)

        self.assertEqual(
"""version: '3'
services:
  opensearch-node1:
    image: opensearchproject/opensearch:1.1.0
    container_name: opensearch-node1
    labels:
      io.benchmark.description: "opensearch-benchmark"
    cpu_count: 2
    mem_limit: 256m
    environment:
      - cluster.name=opensearch-cluster
      - node.name=opensearch-node1
      - discovery.seed_hosts=opensearch-node1
      - DISABLE_INSTALL_DEMO_CONFIG=true
      - bootstrap.memory_lock=true
      - "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m"
    ulimits:
      memlock:
        soft: -1
        hard: -1
      nofile:
        soft: 65536
        hard: 65536
    volumes:
      - %s:/usr/share/opensearch/data
      - %s:/var/log/opensearch
      - %s:/usr/share/opensearch/heapdump
    ports:
      - 39200:39200
      - 9200:9200
      - 9600:9600
    networks:
      - opensearch-net
    healthcheck:
          test: curl -f http://localhost:39200 -u admin:admin --insecure
          interval: 5s
          timeout: 2s
          retries: 10

volumes:
  opensearch-data1:
networks:
  opensearch-net:
""" % (self.node_data_dir, self.node_log_dir, self.node_heap_dump_dir), docker_cfg)
