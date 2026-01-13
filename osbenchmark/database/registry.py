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
Database client registry for managing multiple database backends.

This module provides:
- DatabaseType enum for supported database types
- Registry for mapping database types to client factory classes
- Functions to register and retrieve database implementations
"""
from enum import Enum
from typing import Dict, Type, Optional


class DatabaseType(Enum):
    """Supported database types for benchmarking"""
    OPENSEARCH = "opensearch"
    ELASTICSEARCH = "elasticsearch"
    MILVUS = "milvus"
    VESPA = "vespa"


# Global registry mapping database types to their factory classes
_DATABASE_REGISTRY: Dict[DatabaseType, Type] = {}


def register_database(db_type: DatabaseType, client_factory_class: Type) -> None:
    """
    Register a database client factory.

    Args:
        db_type: The database type enum value
        client_factory_class: The factory class for creating clients

    Example:
        register_database(DatabaseType.OPENSEARCH, OpenSearchClientFactory)
    """
    _DATABASE_REGISTRY[db_type] = client_factory_class


def get_client_factory(db_type: DatabaseType) -> Optional[Type]:
    """
    Retrieve the registered client factory for a database type.

    Args:
        db_type: The database type enum value

    Returns:
        The factory class, or None if not registered
    """
    return _DATABASE_REGISTRY.get(db_type)


def list_registered_databases() -> list:
    """
    Get list of all registered database types.

    Returns:
        List of DatabaseType enum values
    """
    return list(_DATABASE_REGISTRY.keys())
