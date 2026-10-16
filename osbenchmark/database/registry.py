# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Registry of database engines, keyed by the name users pass to ``--database-type``.
"""

from osbenchmark import exceptions

DEFAULT_ENGINE = "opensearch"

_ENGINES = {}


def register_engine(engine_class):
    """
    Register a database engine under its ``name``.

    :param engine_class: A subclass of :class:`osbenchmark.database.engine.Engine`.
    """
    if not engine_class.name:
        raise ValueError(f"Engine class [{engine_class.__name__}] must define a name.")
    _ENGINES[engine_class.name.lower()] = engine_class


def registered_engines():
    """
    :return: The names of all registered engines, in registration order.
    """
    return list(_ENGINES.keys())


def get_engine(name):
    """
    :param name: An engine name, as passed to ``--database-type``.
    :return: A new instance of the engine registered under that name.
    """
    engine_class = _ENGINES.get(name.lower())
    if engine_class is None:
        raise exceptions.SystemSetupError(
            f"Unknown database type [{name}]. Registered database types: {', '.join(registered_engines())}.")
    return engine_class()


def engine_for(cfg):
    """
    :param cfg: The current config object.
    :return: The engine selected by ``database.type``, defaulting to OpenSearch.
    """
    return get_engine(cfg.opts("database", "type", default_value=DEFAULT_ENGINE, mandatory=False))
