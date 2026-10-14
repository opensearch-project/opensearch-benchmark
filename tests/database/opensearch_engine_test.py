# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import asyncio
from unittest import mock

import pytest

from osbenchmark import config, exceptions, telemetry
from osbenchmark.database.clients.opensearch import OpenSearchEngine
from osbenchmark.database.clients.opensearch import engine as opensearch_engine
from osbenchmark.utils import opts

DISTRIBUTION_VERSION = "osbenchmark.builder.cluster_distribution_version"


def distribution_version(cfg):
    return cfg.opts("builder", "distribution.version", mandatory=False)


@mock.patch(DISTRIBUTION_VERSION)
def test_detects_distribution_version_from_cluster(cluster_distribution_version):
    cluster_distribution_version.return_value = "2.19.1"
    cfg = config.Config()

    OpenSearchEngine().configure_distribution_version(cfg)

    cluster_distribution_version.assert_called_once_with(cfg)
    assert distribution_version(cfg) == "2.19.1"
    assert not cfg.exists("worker_coordinator", "serverless.mode")


@mock.patch(DISTRIBUTION_VERSION)
def test_keeps_explicit_distribution_version(cluster_distribution_version):
    cfg = config.Config()
    cfg.add(config.Scope.applicationOverride, "builder", "distribution.version", "3.1.0")

    OpenSearchEngine().configure_distribution_version(cfg)

    cluster_distribution_version.assert_not_called()
    assert distribution_version(cfg) == "3.1.0"


@mock.patch(DISTRIBUTION_VERSION)
def test_skips_detection_for_source_builds(cluster_distribution_version):
    cfg = config.Config()

    OpenSearchEngine().configure_distribution_version(cfg, sources=True)

    cluster_distribution_version.assert_not_called()
    assert distribution_version(cfg) is None


@mock.patch(DISTRIBUTION_VERSION, return_value="oss")
def test_serverless_collection_enables_serverless_mode(_):
    cfg = config.Config()

    OpenSearchEngine().configure_distribution_version(cfg)

    assert distribution_version(cfg) == opensearch_engine.SERVERLESS_DISTRIBUTION_VERSION
    assert cfg.opts("worker_coordinator", "serverless.mode") is True
    assert cfg.opts("worker_coordinator", "serverless.operator") is True


@mock.patch(DISTRIBUTION_VERSION, return_value="oss")
def test_serverless_collection_keeps_explicit_serverless_settings(_):
    cfg = config.Config()
    cfg.add(config.Scope.applicationOverride, "worker_coordinator", "serverless.mode", False)
    cfg.add(config.Scope.applicationOverride, "worker_coordinator", "serverless.operator", False)

    OpenSearchEngine().configure_distribution_version(cfg)

    assert cfg.opts("worker_coordinator", "serverless.mode") is False
    assert cfg.opts("worker_coordinator", "serverless.operator") is False


@mock.patch(DISTRIBUTION_VERSION, return_value="0.9.0")
@mock.patch("osbenchmark.version.minimum_os_version", return_value="1.0.0")
def test_rejects_cluster_older_than_minimum_version(*_):
    with pytest.raises(exceptions.SystemSetupError, match=r"Cluster version must be at least \[1.0.0\] but was \[0.9.0\]"):
        OpenSearchEngine().configure_distribution_version(config.Config())


@mock.patch("osbenchmark.client.OsClientFactory")
def test_creates_sync_client_with_os_client_factory(os_client_factory):
    hosts = [{"host": "localhost", "port": 9200}]
    client_options = {"timeout": 60}

    created = OpenSearchEngine().create_client(hosts, client_options)

    os_client_factory.assert_called_once_with(hosts, client_options)
    assert created is os_client_factory.return_value.create.return_value


@mock.patch("osbenchmark.client.UnifiedClientFactory")
@mock.patch("osbenchmark.client.OsClientFactory")
def test_async_client_uses_default_grpc_hosts(os_client_factory, unified_client_factory):
    hosts = [{"host": "localhost", "port": 9200}]
    cfg = config.Config()

    created = OpenSearchEngine().create_async_client(hosts, {}, cfg)

    os_client_factory.assert_called_once_with(hosts, {})
    rest_client_factory, grpc_hosts = unified_client_factory.call_args.args
    assert rest_client_factory is os_client_factory.return_value
    assert grpc_hosts.default == opts.TargetHosts(opensearch_engine.DEFAULT_GRPC_HOSTS).default
    assert created is unified_client_factory.return_value.create_async.return_value


@mock.patch("osbenchmark.client.UnifiedClientFactory")
@mock.patch("osbenchmark.client.OsClientFactory")
def test_async_client_uses_configured_grpc_hosts(_, unified_client_factory):
    configured = opts.TargetHosts("grpc-host:9400")
    cfg = config.Config()
    cfg.add(config.Scope.applicationOverride, "client", "grpc_hosts", configured)

    OpenSearchEngine().create_async_client([{"host": "localhost", "port": 9200}], {}, cfg)

    assert unified_client_factory.call_args.args[1] is configured


def test_closes_async_client_transport():
    client_instance = mock.Mock()
    client_instance.transport.close = mock.AsyncMock()

    asyncio.run(OpenSearchEngine().close_async_client(client_instance))

    client_instance.transport.close.assert_awaited_once_with()


@mock.patch("osbenchmark.client.wait_for_rest_layer", return_value=True)
def test_waits_for_rest_layer(wait_for_rest_layer):
    client_instance = mock.Mock()

    assert OpenSearchEngine().wait_for_rest_layer(client_instance, max_attempts=3) is True
    wait_for_rest_layer.assert_called_once_with(client_instance, max_attempts=3)


def test_cluster_info_comes_from_info_api():
    client_instance = mock.Mock()

    assert OpenSearchEngine().cluster_info(client_instance) is client_instance.info.return_value


@mock.patch("osbenchmark.paths.test_execution_root", return_value="/tmp/test-execution")
def test_provides_opensearch_telemetry_devices(_):
    cfg = config.Config()
    cfg.add(config.Scope.applicationOverride, "telemetry", "params", {})
    clients = {"default": mock.Mock()}
    index = mock.Mock()
    index.name = "logs"
    workload = mock.Mock(indices=[index])

    devices = OpenSearchEngine().telemetry_devices(cfg, clients, mock.Mock(), workload)

    assert [type(device) for device in devices] == [
        telemetry.NodeStats,
        telemetry.ExternalEnvironmentInfo,
        telemetry.ClusterEnvironmentInfo,
        telemetry.JvmStatsSummary,
        telemetry.IndexStats,
        telemetry.MlBucketProcessingTime,
        telemetry.SegmentStats,
        telemetry.CcrStats,
        telemetry.CcrStatsV2,
        telemetry.RecoveryStats,
        telemetry.TransformStats,
        telemetry.SearchableSnapshotsStats,
        telemetry.SegmentReplicationStats,
        telemetry.ShardStats,
    ]
