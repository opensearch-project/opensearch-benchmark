# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

from unittest import mock

import pytest

from osbenchmark import config, exceptions
from osbenchmark.database import registry
from osbenchmark.database.clients.opensearch import OpenSearchEngine
from osbenchmark.database.engine import Engine


class FakeEngine(Engine):
    name = "Fake"
    display_name = "Fake"

    def create_client(self, hosts, client_options):
        return None

    def create_async_client(self, hosts, client_options, cfg):
        return None

    async def close_async_client(self, client):
        pass

    def wait_for_rest_layer(self, client, max_attempts=40):
        return True


@pytest.fixture
def isolated_registry():
    with mock.patch.dict(registry._ENGINES, clear=True):  # pylint: disable=protected-access
        yield


def test_opensearch_is_registered_by_default():
    assert registry.DEFAULT_ENGINE in registry.registered_engines()
    assert isinstance(registry.get_engine("opensearch"), OpenSearchEngine)


def test_engines_are_looked_up_case_insensitively(isolated_registry):  # pylint: disable=unused-argument
    registry.register_engine(FakeEngine)

    assert registry.registered_engines() == ["fake"]
    assert isinstance(registry.get_engine("FAKE"), FakeEngine)


def test_get_engine_returns_a_new_instance(isolated_registry):  # pylint: disable=unused-argument
    registry.register_engine(FakeEngine)

    assert registry.get_engine("fake") is not registry.get_engine("fake")


def test_unknown_engine_lists_registered_engines(isolated_registry):  # pylint: disable=unused-argument
    registry.register_engine(FakeEngine)

    with pytest.raises(exceptions.SystemSetupError, match=r"Unknown database type \[vespa\]. Registered database types: fake."):
        registry.get_engine("vespa")


def test_engine_without_name_is_rejected(isolated_registry):  # pylint: disable=unused-argument
    class NamelessEngine(FakeEngine):
        name = None

    with pytest.raises(ValueError, match="must define a name"):
        registry.register_engine(NamelessEngine)


def test_engine_for_defaults_to_opensearch():
    assert isinstance(registry.engine_for(config.Config()), OpenSearchEngine)


def test_engine_for_uses_configured_database_type(isolated_registry):  # pylint: disable=unused-argument
    registry.register_engine(FakeEngine)
    cfg = config.Config()
    cfg.add(config.Scope.applicationOverride, "database", "type", "fake")

    assert isinstance(registry.engine_for(cfg), FakeEngine)


def test_incomplete_engine_cannot_be_created():
    class IncompleteEngine(Engine):
        name = "incomplete"

    with pytest.raises(TypeError):
        IncompleteEngine()  # pylint: disable=abstract-class-instantiated


def test_default_distribution_version_is_only_set_when_missing():
    engine = FakeEngine()
    cfg = config.Config()

    engine.configure_distribution_version(cfg)
    assert cfg.opts("builder", "distribution.version") == Engine.default_distribution_version

    cfg.add(config.Scope.benchmark, "builder", "distribution.version", "8.1.0")
    engine.configure_distribution_version(cfg)
    assert cfg.opts("builder", "distribution.version") == "8.1.0"
