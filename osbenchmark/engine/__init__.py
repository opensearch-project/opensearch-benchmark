# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Engine registry for multi-engine support.

Each engine under osbenchmark.engine is a module that exports:

    def create_client_factory(hosts, client_options) -> FactoryInstance:
        '''Factory whose .create() produces a sync client for setup/telemetry.'''

    def create_async_client(hosts, client_options, cfg=None) -> AsyncClient:
        '''Async client the worker coordinators use for benchmark traffic.'''

    def register_runners() -> None:
        '''Register engine-specific runners with osbenchmark.worker_coordinator.runner.'''

    def wait_for_client(async_client, max_attempts: int = 40) -> bool:
        '''Return True when the engine is reachable and ready.'''

    def on_execute_error(e: Exception):  # optional
        '''Translate engine-native exception to OSB's (ops, unit, meta, fatal) tuple.
        Return None if the exception is not engine-specific.'''

The CLI flag --database-type=<name> and config key [database] type = <name> are
preserved for backwards compatibility. Internally the name passed to get_engine()
is the same string; there is no separate rename.
"""

import logging

from osbenchmark import exceptions

_ENGINE_REGISTRY = {}

logger = logging.getLogger(__name__)


def register_engine(name, engine_module):
    """Register an engine module by name.

    :param name: Engine name (e.g. "opensearch", "vespa", "milvus")
    :param engine_module: Module providing the engine interface described above.
    """
    _ENGINE_REGISTRY[name] = engine_module
    logger.debug("Registered engine [%s].", name)


def _ensure_builtin_engines():
    """Lazily register built-in engines on first access.

    Lazy registration avoids importing pyvespa / pymilvus at osbenchmark startup,
    which matters both for users who never use those engines and for import
    isolation in the coordinator process (grpc initialization must happen in
    worker children, not the pre-fork coordinator).
    """
    if "opensearch" not in _ENGINE_REGISTRY:
        from osbenchmark.engine import opensearch as opensearch_engine  # pylint: disable=import-outside-toplevel
        register_engine("opensearch", opensearch_engine)
    if "vespa" not in _ENGINE_REGISTRY:
        try:
            from osbenchmark.engine import vespa as vespa_engine  # pylint: disable=import-outside-toplevel
            register_engine("vespa", vespa_engine)
        except ImportError as e:
            logger.debug("Vespa engine not available: %s", e)


def get_engine(name):
    """Retrieve a registered engine module by name.

    :raises SystemSetupError: if the engine is not registered.
    """
    _ensure_builtin_engines()
    if name not in _ENGINE_REGISTRY:
        available = ", ".join(sorted(_ENGINE_REGISTRY.keys())) or "(none)"
        raise exceptions.SystemSetupError(
            f"Unknown engine type [{name}]. Available engines: [{available}]."
        )
    return _ENGINE_REGISTRY[name]


def available_engines():
    """Return the list of registered engine names."""
    _ensure_builtin_engines()
    return sorted(_ENGINE_REGISTRY.keys())
