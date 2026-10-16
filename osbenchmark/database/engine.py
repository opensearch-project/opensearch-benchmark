# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
The contract every database engine implements.

An engine tells OSB how to talk to one kind of database: how to create its
clients, how to tell whether it is reachable, which runners execute workload
operations against it, and which telemetry devices apply to it. The worker
coordinator and test execution orchestrator only call these methods, so they
never need to know which engine is selected.
"""

import logging
from abc import ABC, abstractmethod

from osbenchmark import config


class Engine(ABC):
    """
    Base class for database engines.

    Subclasses must set ``name`` and implement the abstract client and readiness methods.
    The remaining methods have defaults suitable for an engine with no
    engine-specific runners or telemetry.
    """

    #: The value users pass to ``--database-type``.
    name = None

    #: The engine's name as shown in messages.
    display_name = None

    #: Distribution version used to pick the workload branch when the engine
    #: cannot report a version of its own.
    default_distribution_version = "3.0.0"

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def create_client(self, hosts, client_options):
        """
        Create a synchronous client. Used before the benchmark starts and by telemetry.

        :param hosts: The hosts of a single cluster.
        :param client_options: Client options for that cluster.
        """

    @abstractmethod
    def create_async_client(self, hosts, client_options, cfg):
        """
        Create an asynchronous client. Used by workers to run workload operations.

        :param hosts: The hosts of a single cluster.
        :param client_options: Client options for that cluster.
        :param cfg: The current config object.
        """

    @abstractmethod
    async def close_async_client(self, client):
        """
        Close a client created by ``create_async_client`` once all workload operations have finished.

        :param client: The client to close.
        """

    @abstractmethod
    def wait_for_rest_layer(self, client, max_attempts=40):
        """
        Wait until the engine accepts requests.

        :param client: A client created by ``create_client``.
        :param max_attempts: How many times to probe before giving up.
        :return: ``True`` if the engine became available, ``False`` otherwise.
        """

    def cluster_info(self, client):
        """
        :param client: A client created by ``create_client``.
        :return: Cluster details reported at benchmark start, or ``None`` if the engine has none.
        """
        return None

    def configure_distribution_version(self, cfg, sources=False):
        """
        Make sure ``builder.distribution.version`` is set before the workload is loaded. It selects the
        workload branch and is available to workload templates.

        :param cfg: The current config object.
        :param sources: Whether OSB builds the engine from sources.
        """
        if not cfg.exists("builder", "distribution.version"):
            self.logger.info("Using default distribution version [%s] for database type [%s].",
                             self.default_distribution_version, self.name)
            cfg.add(config.Scope.benchmark, "builder", "distribution.version", self.default_distribution_version)

    def register_runners(self):
        """
        Register engine-specific runners. Called after the default runners and workload plugin runners are
        registered, so engine runners take precedence.
        """

    def telemetry_devices(self, cfg, clients, metrics_store, workload):
        """
        :param cfg: The current config object.
        :param clients: Synchronous clients keyed by cluster name.
        :param metrics_store: The metrics store telemetry devices write to.
        :param workload: The workload being run.
        :return: The telemetry devices this engine supports.
        """
        return []
