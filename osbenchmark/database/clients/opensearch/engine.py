# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

from osbenchmark import client as osb_client
from osbenchmark import config, exceptions, paths, telemetry, version
from osbenchmark.database.engine import Engine
from osbenchmark.utils import opts, versions

# Distribution version assumed for serverless collections, which do not report one.
SERVERLESS_DISTRIBUTION_VERSION = "2.11.0"

# gRPC endpoint used for gRPC operations when no gRPC hosts are configured.
DEFAULT_GRPC_HOSTS = "localhost:9400"


class OpenSearchEngine(Engine):
    """
    OpenSearch, and the default engine. Uses the same clients, readiness check, version detection and
    telemetry devices OSB has always used for OpenSearch.
    """

    name = "opensearch"
    display_name = "OpenSearch"

    def create_client(self, hosts, client_options):
        return osb_client.OsClientFactory(hosts, client_options).create()

    def create_async_client(self, hosts, client_options, cfg):
        grpc_hosts = cfg.opts("client", "grpc_hosts", mandatory=False)
        if not grpc_hosts or not grpc_hosts.all_hosts:
            grpc_hosts = opts.TargetHosts(DEFAULT_GRPC_HOSTS)
        rest_client_factory = osb_client.OsClientFactory(hosts, client_options)
        return osb_client.UnifiedClientFactory(rest_client_factory, grpc_hosts).create_async()

    async def close_async_client(self, client):
        await client.transport.close()

    def wait_for_rest_layer(self, client, max_attempts=40):
        return osb_client.wait_for_rest_layer(client, max_attempts=max_attempts)

    def cluster_info(self, client):
        return client.info()

    def configure_distribution_version(self, cfg, sources=False):
        # For source builds and explicitly specified versions there is nothing to detect.
        if sources or cfg.exists("builder", "distribution.version"):
            return
        # imported here because the builder package imports most of OSB
        from osbenchmark import builder  # pylint: disable=import-outside-toplevel
        distribution_version = builder.cluster_distribution_version(cfg)
        if distribution_version == "oss":
            self.logger.info("Automatically derived serverless collection, setting distribution version to %s",
                             SERVERLESS_DISTRIBUTION_VERSION)
            distribution_version = SERVERLESS_DISTRIBUTION_VERSION
            if not cfg.exists("worker_coordinator", "serverless.mode"):
                cfg.add(config.Scope.benchmark, "worker_coordinator", "serverless.mode", True)

            if not cfg.exists("worker_coordinator", "serverless.operator"):
                cfg.add(config.Scope.benchmark, "worker_coordinator", "serverless.operator", True)
        else:
            self.logger.info("Automatically derived distribution version [%s]", distribution_version)
        cfg.add(config.Scope.benchmark, "builder", "distribution.version", distribution_version)
        min_os_version = versions.Version.from_string(version.minimum_os_version())
        specified_version = versions.Version.from_string(distribution_version)
        if specified_version < min_os_version:
            raise exceptions.SystemSetupError(f"Cluster version must be at least [{min_os_version}] but was [{distribution_version}]")

    def telemetry_devices(self, cfg, clients, metrics_store, workload):
        telemetry_params = cfg.opts("telemetry", "params")
        log_root = paths.test_execution_root(cfg)
        default_client = clients["default"]
        index_names = [idx.name for idx in workload.indices] if workload.indices else None
        return [
            telemetry.NodeStats(telemetry_params, clients, metrics_store),
            telemetry.ExternalEnvironmentInfo(default_client, metrics_store),
            telemetry.ClusterEnvironmentInfo(default_client, metrics_store),
            telemetry.JvmStatsSummary(default_client, metrics_store),
            telemetry.IndexStats(default_client, metrics_store, index_names=index_names),
            telemetry.MlBucketProcessingTime(default_client, metrics_store),
            telemetry.SegmentStats(log_root, default_client),
            telemetry.CcrStats(telemetry_params, clients, metrics_store),
            telemetry.CcrStatsV2(telemetry_params, clients, metrics_store),
            telemetry.RecoveryStats(telemetry_params, clients, metrics_store),
            telemetry.TransformStats(telemetry_params, clients, metrics_store),
            telemetry.SearchableSnapshotsStats(telemetry_params, clients, metrics_store),
            telemetry.SegmentReplicationStats(telemetry_params, clients, metrics_store),
            telemetry.ShardStats(telemetry_params, clients, metrics_store)
        ]
