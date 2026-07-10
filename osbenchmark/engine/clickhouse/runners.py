# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""ClickHouse-native runners for OSB operations."""

import logging

from osbenchmark import exceptions
from osbenchmark.workload import workload
from osbenchmark.worker_coordinator import runner
from osbenchmark.worker_coordinator.runner import Runner, Retry, register_runner, request_context_holder

logger = logging.getLogger(__name__)

# Runner classes are filled in P4. Registration lives in
# osbenchmark/engine/clickhouse/__init__.py:register_runners() — do NOT define a
# module-level register_clickhouse_runners here (see the engine module docstring).
