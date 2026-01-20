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
Database abstraction layer for OpenSearch Benchmark.

This package provides a unified interface for multiple database backends,
allowing OSB to benchmark different search engines and databases while
maintaining compatibility with existing workloads and runners.

Architecture:
- interface.py: Abstract base classes defining the contract
- registry.py: Database type registry and enumeration
- factory.py: Factory for creating database clients
- clients/: Database-specific implementations

Usage:
    from osbenchmark.database.factory import DatabaseClientFactory

    # Create a client factory
    factory = DatabaseClientFactory.create_client_factory(
        "opensearch",
        [{"host": "localhost", "port": 9200}],
        {"use_ssl": True}
    )

    # Create the async client
    client = factory.create_async()

    # Use the client (same API for all databases)
    await client.bulk(body=documents)
    results = await client.search(index="test", body=query)
"""

# Register database implementations on module import
from osbenchmark.database.registry import register_database, DatabaseType
from osbenchmark.database.factory import DatabaseClientFactory
from osbenchmark.database.clients.opensearch.opensearch import OpenSearchClientFactory

# Register OpenSearch as the default database type
register_database(DatabaseType.OPENSEARCH, OpenSearchClientFactory)

# Note: Other database types (Vespa, Milvus, etc.) will be registered
# when their implementations are added in future PRs.

# Public API exports
__all__ = [
    'DatabaseType',
    'DatabaseClientFactory',
]
