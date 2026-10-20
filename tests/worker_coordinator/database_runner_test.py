# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import asyncio
from unittest import mock

from osbenchmark.worker_coordinator import runner, worker_coordinator

REGISTER_ENGINE_RUNNERS = "osbenchmark.worker_coordinator.worker_coordinator.runner.register_engine_runners"


@mock.patch(REGISTER_ENGINE_RUNNERS)
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.engine_for")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.workload.load_workload_plugins")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.runner.register_default_runners")
def test_database_runners_override_workload_plugin_runners(
    register_default_runners,
    load_workload_plugins,
    engine_for,
    register_engine_runners,
):
    calls = []
    register_default_runners.side_effect = lambda: calls.append("defaults")
    load_workload_plugins.side_effect = lambda *args: calls.append("workload")
    register_engine_runners.side_effect = lambda engine: calls.append("engine")

    cfg = mock.Mock()
    benchmark_workload = mock.Mock(has_plugins=True)
    benchmark_workload.name = "vectorsearch"

    worker_coordinator.register_workload_runners(cfg, benchmark_workload)

    assert calls == ["defaults", "workload", "engine"]
    engine_for.assert_called_once_with(cfg)
    register_engine_runners.assert_called_once_with(engine_for.return_value)
    load_workload_plugins.assert_called_once_with(
        cfg,
        "vectorsearch",
        worker_coordinator.runner.register_runner,
        worker_coordinator.scheduler.register_scheduler,
    )


@mock.patch(REGISTER_ENGINE_RUNNERS)
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.engine_for")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.workload.load_workload_plugins")
@mock.patch("osbenchmark.worker_coordinator.worker_coordinator.runner.register_default_runners")
def test_skips_workload_plugins_when_workload_has_none(register_default_runners, load_workload_plugins, engine_for,
                                                      register_engine_runners):
    benchmark_workload = mock.Mock(has_plugins=False)

    worker_coordinator.register_workload_runners(mock.Mock(), benchmark_workload)

    register_default_runners.assert_called_once_with()
    load_workload_plugins.assert_not_called()
    register_engine_runners.assert_called_once_with(engine_for.return_value)


class EngineSearch(runner.Runner):
    async def __call__(self, client, params):
        return {"weight": 1, "unit": "ops", "engine": True}

    def __repr__(self):
        return "engine-search"


def test_engine_runners_replace_registered_runners():
    engine = mock.Mock()
    engine.runners.return_value = {"unit-test-search": EngineSearch()}
    runner.register_runner("unit-test-search", runner.Query(), async_runner=True)
    try:
        runner.register_engine_runners(engine)

        result = asyncio.run(runner.runner_for("unit-test-search")({"default": mock.Mock()}, {}))
        assert result["engine"] is True
    finally:
        runner.remove_runner("unit-test-search")


def test_engine_without_runners_registers_nothing():
    engine = mock.Mock()
    engine.runners.return_value = {}

    with mock.patch("osbenchmark.worker_coordinator.runner.register_runner") as register_runner:
        runner.register_engine_runners(engine)

    register_runner.assert_not_called()
