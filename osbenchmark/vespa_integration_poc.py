# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Integration module for Vespa support in OpenSearch Benchmark.

This module provides the glue code to register Vespa as a supported
backend engine for OSB benchmarking.
"""

import logging
from typing import Dict, Any

from osbenchmark import workload
from osbenchmark.vespa_client import VespaClientFactory, wait_for_vespa
from osbenchmark.vespa_runners import VESPA_RUNNERS


logger = logging.getLogger(__name__)


def is_vespa_backend(client_options: Dict[str, Any]) -> bool:
    """
    Determine if Vespa backend is being used based on client options.

    :param client_options: Client configuration options
    :return: True if Vespa backend should be used
    """
    return client_options.get("backend", "").lower() == "vespa" or \
           client_options.get("engine", "").lower() == "vespa"


def create_vespa_client_factory(hosts, client_options):
    """
    Create a Vespa client factory.

    :param hosts: Host configurations
    :param client_options: Client options
    :return: VespaClientFactory instance
    """
    logger.info("Creating Vespa client factory")
    return VespaClientFactory(hosts, client_options)


def register_vespa_runners(runner_registry):
    """
    Register Vespa-specific runners with the OSB runner registry.

    This allows Vespa operations to be used in workloads.

    :param runner_registry: The OSB runner registry
    """
    logger.info("Registering Vespa runners")

    # Map OSB operation types to Vespa runners
    operation_mapping = {
        workload.OperationType.Bulk: VESPA_RUNNERS["bulk"],
        workload.OperationType.Search: VESPA_RUNNERS["search"],
        workload.OperationType.VectorSearch: VESPA_RUNNERS["vector-search"],
        workload.OperationType.BulkVectorDataSet: VESPA_RUNNERS["bulk-vector-data-set"],
        workload.OperationType.CreateIndex: VESPA_RUNNERS["create-index"],
        workload.OperationType.DeleteIndex: VESPA_RUNNERS["delete-index"],
        workload.OperationType.IndexStats: VESPA_RUNNERS["indices-stats"],
        workload.OperationType.ClusterHealth: VESPA_RUNNERS["cluster-health"],
        workload.OperationType.Refresh: VESPA_RUNNERS["refresh"],
        workload.OperationType.ForceMerge: VESPA_RUNNERS["force-merge"],
    }

    for operation_type, runner in operation_mapping.items():
        try:
            runner_registry.register_runner(operation_type, runner, async_runner=True)
            logger.debug(f"Registered Vespa runner for {operation_type}")
        except Exception as e:
            logger.warning(f"Failed to register Vespa runner for {operation_type}: {e}")


class VespaBackendAdapter:
    """
    Adapter that allows OSB to use Vespa as a backend engine.

    This class provides the interface OSB expects while delegating
    to Vespa-specific implementations.
    """

    def __init__(self, hosts, client_options):
        """
        Initialize the Vespa backend adapter.

        :param hosts: Vespa host configurations
        :param client_options: Client options
        """
        self.hosts = hosts
        self.client_options = client_options
        self.factory = VespaClientFactory(hosts, client_options)
        self.logger = logging.getLogger(__name__)

    def create_client(self):
        """Create a synchronous Vespa client (not supported)."""
        raise NotImplementedError("Synchronous Vespa client not supported")

    def create_async_client(self):
        """
        Create an async Vespa client.

        :return: Async Vespa client instance
        """
        return self.factory.create_async()

    def wait_for_client(self, client, max_attempts=40):
        """
        Wait for Vespa to be ready.

        :param client: Vespa client instance
        :param max_attempts: Maximum health check attempts
        :return: True if ready, False otherwise
        """
        return wait_for_vespa(client, max_attempts)


def get_vespa_operation_params(operation_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert OSB operation parameters to Vespa-compatible format.

    :param operation_name: Name of the operation
    :param params: Original operation parameters
    :return: Vespa-compatible parameters
    """
    vespa_params = dict(params)

    # Handle index/schema name mapping
    if "index" in params:
        vespa_params["schema"] = params["index"]

    # Handle bulk operation parameters
    if operation_name in ["bulk", "bulk-vector-data-set"]:
        # Ensure body is present
        if "body" not in vespa_params:
            raise ValueError("Bulk operations require 'body' parameter")

    # Handle search operation parameters
    if operation_name in ["search", "vector-search"]:
        # Convert OpenSearch query DSL to Vespa YQL if needed
        if "body" in vespa_params and isinstance(vespa_params["body"], dict):
            # The VespaAsyncClient will handle conversion
            pass

    return vespa_params


class VespaWorkloadAdapter:
    """
    Adapts OSB workload definitions to work with Vespa.

    This class translates workload configurations and operations
    to be compatible with Vespa's data model and APIs.
    """

    def __init__(self, workload_spec: Dict[str, Any]):
        """
        Initialize the workload adapter.

        :param workload_spec: OSB workload specification
        """
        self.workload_spec = workload_spec
        self.logger = logging.getLogger(__name__)

    def adapt_indices(self, indices: list) -> list:
        """
        Adapt index definitions to Vespa schemas.

        :param indices: List of index definitions
        :return: Adapted index/schema definitions
        """
        adapted = []

        for index in indices:
            schema_def = {
                "name": index.get("name"),
                "body": self._convert_mappings_to_schema(index.get("body", {}))
            }
            adapted.append(schema_def)

        return adapted

    def _convert_mappings_to_schema(self, mappings: Dict) -> Dict:
        """
        Convert OpenSearch mappings to Vespa schema definition.

        :param mappings: OpenSearch index mappings
        :return: Vespa schema definition
        """
        # This is a simplified conversion
        # In practice, you'd need more sophisticated mapping
        schema = {
            "fields": []
        }

        properties = mappings.get("mappings", {}).get("properties", {})

        for field_name, field_def in properties.items():
            field_type = field_def.get("type", "string")

            # Map OpenSearch types to Vespa types
            type_mapping = {
                "text": "string",
                "keyword": "string",
                "long": "long",
                "integer": "int",
                "float": "float",
                "double": "double",
                "boolean": "bool",
                "date": "long",
                "knn_vector": "tensor<float>(x[{}])".format(field_def.get("dimension", 768))
            }

            vespa_type = type_mapping.get(field_type, "string")
            schema["fields"].append({
                "name": field_name,
                "type": vespa_type
            })

        return schema

    def adapt_operations(self, operations: list) -> list:
        """
        Adapt operation definitions for Vespa.

        :param operations: List of operation definitions
        :return: Adapted operations
        """
        adapted = []

        for op in operations:
            op_type = op.get("operation-type")

            # Map operation types if needed
            if op_type in ["bulk", "search", "vector-search"]:
                # These operations are already supported
                adapted.append(op)
            else:
                # Log unsupported operations
                self.logger.warning(f"Operation type {op_type} may not be fully supported by Vespa")
                adapted.append(op)

        return adapted


def initialize_vespa_backend():
    """
    Initialize Vespa backend support in OSB.

    This should be called during OSB startup to register Vespa
    as a supported backend.
    """
    logger.info("Initializing Vespa backend support")

    # Register Vespa-specific components
    try:
        from osbenchmark.worker_coordinator import runner
        register_vespa_runners(runner)
        logger.info("Vespa backend initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Vespa backend: {e}")
        raise


# Utility functions for Vespa-specific conversions

def convert_opensearch_query_to_yql(query: Dict[str, Any], document_type: str) -> str:
    """
    Convert an OpenSearch query DSL to Vespa YQL.

    :param query: OpenSearch query dict
    :param document_type: Vespa document type
    :return: YQL query string
    """
    if not query:
        return f"select * from {document_type} where true"

    query_body = query.get("query", {})

    # Handle KNN/vector search
    if "knn" in query_body:
        knn = query_body["knn"]
        field = knn.get("field", "vector")
        k = knn.get("k", 10)
        return (
            f"select * from {document_type} where "
            f"{{targetHits:{k}}}nearestNeighbor({field}, query_vector)"
        )

    # Handle match_all
    elif "match_all" in query_body:
        return f"select * from {document_type} where true"

    # Handle match query
    elif "match" in query_body:
        for field, value in query_body["match"].items():
            search_value = value if isinstance(value, str) else value.get("query", "")
            return f"select * from {document_type} where {field} contains '{search_value}'"

    # Handle term query
    elif "term" in query_body:
        for field, value in query_body["term"].items():
            term_value = value if isinstance(value, (str, int, float)) else value.get("value", "")
            return f"select * from {document_type} where {field} contains '{term_value}'"

    # Default fallback
    return f"select * from {document_type} where true"


def convert_bulk_to_vespa_format(bulk_body: str, index_name: str) -> list:
    """
    Convert OpenSearch bulk format to Vespa document feed format.

    :param bulk_body: Bulk request body (newline-delimited JSON)
    :param index_name: Target index/schema name
    :return: List of Vespa document dicts
    """
    import json

    documents = []
    lines = bulk_body.strip().split('\n') if isinstance(bulk_body, str) else []

    i = 0
    while i < len(lines):
        if not lines[i].strip():
            i += 1
            continue

        try:
            action = json.loads(lines[i])
            if i + 1 < len(lines):
                doc = json.loads(lines[i + 1])

                # Extract document ID
                doc_id = action.get("index", {}).get("_id") or \
                         action.get("create", {}).get("_id") or \
                         f"doc_{len(documents)}"

                # Build Vespa document
                vespa_doc = {
                    "id": f"id:{index_name}:{index_name}::{doc_id}",
                    "fields": doc.get("_source", doc)
                }

                documents.append(vespa_doc)
                i += 2
            else:
                i += 1
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse bulk line: {e}")
            i += 1

    return documents
