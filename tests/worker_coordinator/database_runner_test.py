# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

from unittest import mock

from osbenchmark.database.registry import DatabaseType
from osbenchmark.worker_coordinator import worker_coordinator


@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.get_client_factory")
def test_registers_database_specific_runners(get_client_factory):
    client_factory_class = mock.Mock()
    get_client_factory.return_value = client_factory_class

    worker_coordinator.register_database_runners("vespa")

    get_client_factory.assert_called_once_with(DatabaseType.VESPA)
    client_factory_class.register_runners.assert_called_once_with()


@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.get_client_factory")
def test_skips_runner_registration_when_factory_has_no_hook(get_client_factory):
    get_client_factory.return_value = mock.Mock(spec=[])

    worker_coordinator.register_database_runners("opensearch")

    get_client_factory.assert_called_once_with(DatabaseType.OPENSEARCH)


@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.get_client_factory")
def test_defers_unknown_database_validation_to_client_creation(get_client_factory):
    worker_coordinator.register_database_runners("unknown")

    get_client_factory.assert_not_called()


@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.register_database_runners")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.workload.load_workload_plugins")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.runner.register_default_runners")
def test_database_runners_override_workload_plugin_runners(
    register_default_runners,
    load_workload_plugins,
    register_database_runners,
):
    calls = []
    register_default_runners.side_effect = lambda: calls.append("defaults")
    load_workload_plugins.side_effect = lambda *args: calls.append("workload")
    register_database_runners.side_effect = lambda database_type: calls.append(database_type)

    cfg = mock.Mock()
    cfg.opts.return_value = "milvus"
    benchmark_workload = mock.Mock(has_plugins=True)
    benchmark_workload.name = "vectorsearch"

    worker_coordinator.register_workload_runners(cfg, benchmark_workload)

    assert calls == ["defaults", "workload", "milvus"]
    load_workload_plugins.assert_called_once_with(
        cfg,
        "vectorsearch",
        worker_coordinator.runner.register_runner,
        worker_coordinator.scheduler.register_scheduler,
    )
