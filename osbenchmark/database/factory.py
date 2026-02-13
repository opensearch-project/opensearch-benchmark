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
Database client factory for creating database-specific clients.

This module provides the main entry point for creating database clients
based on configuration. It uses the registry to look up the appropriate
client factory for each database type.
"""
from typing import Any, Dict, List
from osbenchmark.database.registry import DatabaseType, get_client_factory
from osbenchmark import exceptions


class DatabaseClientFactory:
    """
    Factory for creating database clients based on configuration.

    This is the main entry point used by OSB to create database clients.
    It determines which database type to use and delegates to the
    appropriate database-specific factory.
    """

    @staticmethod
    def create_client_factory(database_type: str, hosts: List[Dict], client_options: Dict) -> Any:
        """
        Create the appropriate database client factory.

        Args:
            database_type: String identifier for database type (e.g., "opensearch")
            hosts: List of host dictionaries with "host" and "port" keys
            client_options: Dictionary of client-specific options

        Returns:
            A database-specific client factory instance

        Raises:
            ValueError: If database_type is not supported
            exceptions.SystemSetupError: If no factory is registered for the database type

        Example:
            factory = DatabaseClientFactory.create_client_factory(
                "opensearch",
                [{"host": "localhost", "port": 9200}],
                {"use_ssl": True}
            )
            client = factory.create_async()
        """
        try:
            db_enum = DatabaseType(database_type.lower())
        except ValueError:
            valid_types = [db.value for db in DatabaseType]
            raise ValueError(
                f"Unsupported database type: '{database_type}'. "
                f"Valid types are: {', '.join(valid_types)}"
            )

        factory_class = get_client_factory(db_enum)

        if not factory_class:
            raise exceptions.SystemSetupError(
                f"No client factory registered for database type: {database_type}. "
                f"This database type may not be fully implemented yet."
            )

        return factory_class(hosts, client_options)

    @staticmethod
    def detect_database_type(client_options: Dict) -> str:
        """
        Detect database type from client options.

        Args:
            client_options: Dictionary of client options

        Returns:
            String identifier for database type (defaults to "opensearch")

        Example:
            # Explicit database type in options
            db_type = DatabaseClientFactory.detect_database_type(
                {"database_type": "vespa"}
            )  # Returns "vespa"

            # Default to OpenSearch
            db_type = DatabaseClientFactory.detect_database_type({})
            # Returns "opensearch"
        """
        # Check for explicit database type in client options
        if "database_type" in client_options:
            return client_options["database_type"]

        # Default to OpenSearch for backward compatibility
        return DatabaseType.OPENSEARCH.value
