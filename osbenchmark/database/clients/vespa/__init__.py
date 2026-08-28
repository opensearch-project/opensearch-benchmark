# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""Vespa client implementation."""

from osbenchmark.database.clients.vespa.client import VespaClientFactory
from osbenchmark.database.clients.vespa.runners import register_vespa_runners

__all__ = ["VespaClientFactory", "register_vespa_runners"]
