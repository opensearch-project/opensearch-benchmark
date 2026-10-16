# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

from unittest import mock

from osbenchmark.worker_coordinator import worker_coordinator


@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.engine_for")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.workload.load_workload_plugins")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.runner.register_default_runners")
def test_database_runners_override_workload_plugin_runners(
    register_default_runners,
    load_workload_plugins,
    engine_for,
):
    calls = []
    register_default_runners.side_effect = lambda: calls.append("defaults")
    load_workload_plugins.side_effect = lambda *args: calls.append("workload")
    engine_for.return_value.register_runners.side_effect = lambda: calls.append("engine")

    cfg = mock.Mock()
    benchmark_workload = mock.Mock(has_plugins=True)
    benchmark_workload.name = "vectorsearch"

    worker_coordinator.register_workload_runners(cfg, benchmark_workload)

    assert calls == ["defaults", "workload", "engine"]
    engine_for.assert_called_once_with(cfg)
    load_workload_plugins.assert_called_once_with(
        cfg,
        "vectorsearch",
        worker_coordinator.runner.register_runner,
        worker_coordinator.scheduler.register_scheduler,
    )


@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.engine_for")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.workload.load_workload_plugins")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.runner.register_default_runners")
def test_skips_workload_plugins_when_workload_has_none(register_default_runners, load_workload_plugins, engine_for):
    benchmark_workload = mock.Mock(has_plugins=False)

    worker_coordinator.register_workload_runners(mock.Mock(), benchmark_workload)

    register_default_runners.assert_called_once_with()
    load_workload_plugins.assert_not_called()
    engine_for.return_value.register_runners.assert_called_once_with()
