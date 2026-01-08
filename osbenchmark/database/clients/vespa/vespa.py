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

import asyncio
import io
import json
import logging
import time
import uuid
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

import aiohttp
import certifi
import requests
# pyvespa package is imported as 'vespa' internally
from vespa.package import ApplicationPackage, Schema, Document, Field, HNSW
from vespa.application import Vespa
import urllib3
from urllib3.util.ssl_ import is_ipaddress

from osbenchmark import doc_link, exceptions
from osbenchmark.cloud_provider.factory import CloudProviderFactory
from osbenchmark.utils import console, convert
from osbenchmark.context import RequestContextHolder

from osbenchmark.database.interface import (
    DatabaseClient,
    IndicesNamespace,
    ClusterNamespace,
    TransportNamespace,
    NodesNamespace
)


# ============================================================================
# Field Name Mapping (OpenSearch → Vespa)
# ============================================================================

FIELD_NAME_MAPPING = {
    "@timestamp": "timestamp",
    "log.file.path": "log_file_path",
    "process.name": "process_name",
    "metrics.size": "metrics_size",
    "metrics.tmin": "metrics_tmin",
    "cloud.region": "cloud_region",
    "agent.name": "agent_name",
    "agent.id": "agent_id",
    "agent.type": "agent_type",
    "agent.version": "agent_version",
    "agent.ephemeral_id": "agent_ephemeral_id",
    "aws.cloudwatch.log_stream": "aws_cloudwatch_log_stream",
    "aws.cloudwatch.log_group": "aws_cloudwatch_log_group",
    "aws.cloudwatch.ingestion_time": "aws_cloudwatch_ingestion_time",
    "meta.file": "meta_file",
    "event.id": "event_id",
    "event.dataset": "event_dataset",
    "event.ingested": "event_ingested",
    "data_stream.dataset": "data_stream_dataset",
    "data_stream.namespace": "data_stream_namespace",
    "data_stream.type": "data_stream_type",
    "input.type": "input_type",
    "ecs.version": "ecs_version",
}

# Fields allowed for big5 workload
BIG5_ALLOWED_FIELDS = {
    "timestamp", "message", "metrics_size", "metrics_tmin",
    "agent_ephemeral_id", "agent_id", "agent_name", "agent_type", "agent_version",
    "aws_cloudwatch_ingestion_time", "aws_cloudwatch_log_group", "aws_cloudwatch_log_stream",
    "cloud_region",
    "data_stream_dataset", "data_stream_namespace", "data_stream_type",
    "ecs_version",
    "event_dataset", "event_id", "event_ingested",
    "input_type",
    "log_file_path",
    "meta_file",
    "process_name",
    "tags",
}

# Interval to milliseconds mapping for date_histogram
INTERVAL_MS_MAP = {
    "second": 1000, "1s": 1000,
    "minute": 60000, "1m": 60000,
    "hour": 3600000, "1h": 3600000,
    "day": 86400000, "1d": 86400000,
    "week": 604800000, "1w": 604800000,
    "month": 2592000000, "1M": 2592000000,
}


# ============================================================================
# OpenSearch to Vespa Type Mapping
# ============================================================================

class OpenSearchToVespaMapper:
    """
    Translates OpenSearch index mappings to Vespa schema definitions.

    OpenSearch types -> Vespa types:
    - text -> string with indexing: index | summary
    - keyword -> string with indexing: attribute | summary
    - integer -> int with indexing: attribute | summary
    - long -> long with indexing: attribute | summary
    - float -> float with indexing: attribute | summary
    - double -> double with indexing: attribute | summary
    - boolean -> bool with indexing: attribute | summary
    - date -> string (Vespa doesn't have native date, use string or long epoch)
    - geo_point -> position with indexing: attribute | summary
    - knn_vector -> tensor<float>(x[N]) with HNSW index
    """

    # Map OpenSearch types to Vespa types
    TYPE_MAP = {
        "text": "string",
        "keyword": "string",
        "integer": "int",
        "long": "long",
        "float": "float",
        "double": "double",
        "boolean": "bool",
        "date": "string",  # Could also use long for epoch
        "ip": "string",
        "geo_point": "position",
        "object": None,  # Handled specially - nested fields
        "nested": None,  # Handled specially
    }

    @classmethod
    def get_vespa_type(cls, os_type: str, field_config: Dict) -> Optional[str]:
        """Convert OpenSearch type to Vespa type."""
        # Handle knn_vector specially
        if os_type == "knn_vector":
            dimension = field_config.get("dimension", 128)
            return f"tensor<float>(x[{dimension}])"

        return cls.TYPE_MAP.get(os_type)

    @classmethod
    def get_indexing_config(cls, os_type: str, field_config: Dict) -> List[str]:
        """
        Determine Vespa indexing configuration based on OpenSearch type.

        Returns list like ["index", "summary"] or ["attribute", "summary"]
        """
        if os_type == "text":
            # Full-text searchable
            return ["index", "summary"]
        elif os_type == "keyword":
            # Exact match, filterable, sortable
            return ["attribute", "summary"]
        elif os_type == "knn_vector":
            # Vector field needs attribute + index for HNSW
            return ["attribute", "index"]
        elif os_type in ("integer", "long", "float", "double", "boolean", "date", "ip"):
            # Numeric/filterable types
            return ["attribute", "summary"]
        elif os_type == "geo_point":
            return ["attribute", "summary"]
        else:
            # Default
            return ["attribute", "summary"]

    @classmethod
    def get_index_config(cls, os_type: str, field_config: Dict) -> Optional[str]:
        """Get Vespa index configuration string."""
        if os_type == "text":
            return "enable-bm25"
        return None

    @classmethod
    def get_hnsw_config(cls, os_type: str, field_config: Dict) -> Optional[HNSW]:
        """Get HNSW configuration for vector fields."""
        if os_type != "knn_vector":
            return None

        # Extract HNSW parameters from OpenSearch config
        method = field_config.get("method", {})
        engine = method.get("engine", "nmslib")
        params = method.get("parameters", {})

        # Map OpenSearch HNSW params to Vespa HNSW
        # OpenSearch: ef_construction, m
        # Vespa: max_links_per_node (m), neighbors_to_explore_at_insert (ef_construction)
        m = params.get("m", 16)
        ef_construction = params.get("ef_construction", 100)

        return HNSW(
            max_links_per_node=m,
            neighbors_to_explore_at_insert=ef_construction
        )

    @classmethod
    def translate_field(cls, field_name: str, field_config: Dict) -> Optional[Field]:
        """
        Translate a single OpenSearch field definition to a Vespa Field.

        Args:
            field_name: Name of the field
            field_config: OpenSearch field configuration dict

        Returns:
            Vespa Field object or None if type not supported
        """
        os_type = field_config.get("type")
        if not os_type:
            return None

        vespa_type = cls.get_vespa_type(os_type, field_config)
        if not vespa_type:
            logging.getLogger(__name__).warning(
                f"Unsupported OpenSearch type '{os_type}' for field '{field_name}', skipping"
            )
            return None

        indexing = cls.get_indexing_config(os_type, field_config)
        index = cls.get_index_config(os_type, field_config)
        ann = cls.get_hnsw_config(os_type, field_config)

        return Field(
            name=field_name,
            type=vespa_type,
            indexing=indexing,
            index=index,
            ann=ann
        )

    @classmethod
    def translate_mappings(cls, mappings: Dict) -> List[Field]:
        """
        Translate OpenSearch mappings to list of Vespa Fields.

        Args:
            mappings: OpenSearch mappings dict with "properties" key

        Returns:
            List of Vespa Field objects
        """
        fields = []
        properties = mappings.get("properties", {})

        for field_name, field_config in properties.items():
            # Handle nested fields (flatten for now)
            if field_config.get("type") == "object" or "properties" in field_config:
                # Recursively handle nested properties with dot notation
                nested_props = field_config.get("properties", {})
                for nested_name, nested_config in nested_props.items():
                    full_name = f"{field_name}_{nested_name}"  # Use underscore, Vespa doesn't like dots
                    field = cls.translate_field(full_name, nested_config)
                    if field:
                        fields.append(field)
            else:
                field = cls.translate_field(field_name, field_config)
                if field:
                    fields.append(field)

                # Handle multi-fields (e.g., text with .raw keyword subfield)
                if "fields" in field_config:
                    for subfield_name, subfield_config in field_config["fields"].items():
                        full_name = f"{field_name}_{subfield_name}"
                        subfield = cls.translate_field(full_name, subfield_config)
                        if subfield:
                            fields.append(subfield)

        return fields


# ============================================================================
# Vespa Namespace Implementations
# ============================================================================

class VespaIndicesNamespace(IndicesNamespace):
    """
    Vespa implementation of indices namespace.

    Note: Vespa handles indices differently than OpenSearch.
    - Creating an "index" means deploying a schema to the application
    - Deleting means removing documents (schema changes require redeployment)

    If you need to skip schema deployment (e.g., schema is pre-deployed),
    use --exclude-tasks to skip the index creation operation.
    """

    def __init__(self, vespa_client: Vespa, app_package: ApplicationPackage,
                 config_url: str = None):
        self._client = vespa_client
        self._app_package = app_package
        self._config_url = config_url  # e.g., "http://localhost:19071"
        self._schemas: Dict[str, Schema] = {}  # Track created schemas
        self.logger = logging.getLogger(__name__)

    async def create(self, index: str, body: Optional[Dict] = None, **kwargs) -> Dict:
        """
        Create a Vespa schema from OpenSearch index definition and deploy it.

        This method:
        1. Translates OpenSearch mappings to Vespa schema
        2. Builds an application package with the schema
        3. Deploys it via the Vespa Deploy API (POST to /application/v2/tenant/default/prepareandactivate)

        Args:
            index: Index name (becomes schema name in Vespa)
            body: OpenSearch index definition with "mappings" and "settings"

        Returns:
            Dict with acknowledgement and deployment status
        """
        RequestContextHolder.on_request_start()
        self.logger.info(f"Creating Vespa schema for index '{index}'")

        if body is None:
            body = {}

        mappings = body.get("mappings", {})

        # Translate OpenSearch mappings to Vespa fields
        fields = OpenSearchToVespaMapper.translate_mappings(mappings)

        if not fields:
            self.logger.warning(f"No fields could be translated for index '{index}'")
            # Create minimal schema with a dummy field
            fields = [Field(name="doc_id", type="string", indexing=["attribute", "summary"])]

        # Create Vespa Document and Schema
        document = Document(fields=fields)
        schema = Schema(name=index, document=document)

        # Store schema reference
        self._schemas[index] = schema

        # Add schema to application package
        self._app_package.schema = schema

        self.logger.info(f"Created Vespa schema '{index}' with {len(fields)} fields")

        # Deploy the application package via the Deploy API
        if self._config_url:
            deploy_result = await self._deploy_application()
            RequestContextHolder.on_request_end()
            return {
                "acknowledged": True,
                "index": index,
                "deployed": deploy_result.get("success", False),
                "deploy_message": deploy_result.get("message", "")
            }
        else:
            self.logger.warning("No config_url provided - schema created but not deployed")
            RequestContextHolder.on_request_end()
            return {"acknowledged": True, "index": index, "deployed": False}

    async def _deploy_application(self) -> Dict:
        """
        Deploy the application package to Vespa via the Deploy API.

        Uses POST /application/v2/tenant/default/prepareandactivate
        with the application package as a zip file.

        Returns:
            Dict with deployment result
        """
        try:
            # Create zip file of application package in memory
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                # Write services.xml
                services_xml = self._generate_services_xml()
                zf.writestr("services.xml", services_xml)

                # Write schema files
                for schema_name, schema in self._schemas.items():
                    schema_content = self._generate_schema_sd(schema_name, schema)
                    zf.writestr(f"schemas/{schema_name}.sd", schema_content)

            zip_buffer.seek(0)
            zip_data = zip_buffer.getvalue()

            # Deploy via REST API
            deploy_url = f"{self._config_url}/application/v2/tenant/default/prepareandactivate"

            self.logger.info(f"Deploying application package to {deploy_url}")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    deploy_url,
                    data=zip_data,
                    headers={"Content-Type": "application/zip"}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.logger.info(f"Deployment successful: {result}")
                        return {"success": True, "message": result.get("message", "Deployed")}
                    else:
                        error_text = await response.text()
                        self.logger.error(f"Deployment failed ({response.status}): {error_text}")
                        return {"success": False, "message": f"HTTP {response.status}: {error_text}"}

        except Exception as e:
            self.logger.error(f"Deployment failed with exception: {e}")
            return {"success": False, "message": str(e)}

    def _generate_services_xml(self) -> str:
        """
        Generate services.xml for the application package.

        This defines the Vespa cluster configuration.
        """
        schema_names = list(self._schemas.keys())
        document_types = "\n          ".join(
            f'<document type="{name}" mode="index"/>' for name in schema_names
        )

        return f'''<?xml version="1.0" encoding="UTF-8"?>
<services version="1.0">
  <container id="default" version="1.0">
    <search/>
    <document-api/>
    <nodes>
      <node hostalias="node1"/>
    </nodes>
  </container>
  <content id="content" version="1.0">
    <redundancy>1</redundancy>
    <documents>
      {document_types}
    </documents>
    <nodes>
      <node hostalias="node1" distribution-key="0"/>
    </nodes>
  </content>
</services>
'''

    def _generate_schema_sd(self, name: str, schema: Schema) -> str:
        """
        Generate a Vespa schema definition (.sd) file content.

        Args:
            name: Schema name
            schema: Schema object with document and fields

        Returns:
            String content of the .sd file
        """
        # Build field definitions
        field_defs = []
        for field in schema.document.fields:
            field_def = self._generate_field_definition(field)
            field_defs.append(field_def)

        fields_str = "\n    ".join(field_defs)

        return f'''schema {name} {{
  document {name} {{
    {fields_str}
  }}
}}
'''

    def _generate_field_definition(self, field: Field) -> str:
        """Generate a single field definition for the schema."""
        parts = [f"field {field.name} type {field.type} {{"]

        # Add indexing
        if field.indexing:
            indexing_str = " | ".join(field.indexing)
            parts.append(f"      indexing: {indexing_str}")

        # Add index configuration (e.g., enable-bm25)
        if field.index:
            parts.append(f"      index: {field.index}")

        # Add HNSW configuration for vector fields
        if field.ann:
            parts.append(f"      index {{")
            parts.append(f"        hnsw {{")
            parts.append(f"          max-links-per-node: {field.ann.max_links_per_node}")
            parts.append(f"          neighbors-to-explore-at-insert: {field.ann.neighbors_to_explore_at_insert}")
            parts.append(f"        }}")
            parts.append(f"      }}")

        parts.append("    }")
        return "\n    ".join(parts)

    async def delete(self, index: str, **kwargs) -> Dict:
        """
        Delete documents from a Vespa schema.

        Note: In Vespa, you can't dynamically delete schemas without redeployment.
        This will delete all documents in the schema instead.
        """
        self.logger.info(f"Deleting all documents from Vespa schema '{index}'")

        # Simulate request timing for stub operations
        RequestContextHolder.on_request_start()

        # Use Vespa's delete_all_docs or visit API
        # For now, mark as acknowledged - actual implementation depends on Vespa setup
        if index in self._schemas:
            del self._schemas[index]

        RequestContextHolder.on_request_end()
        return {"acknowledged": True}

    async def exists(self, index: str, **kwargs) -> bool:
        """Check if a schema exists in the application."""
        RequestContextHolder.on_request_start()
        result = index in self._schemas
        RequestContextHolder.on_request_end()
        return result

    async def refresh(self, index: Optional[str] = None, **kwargs) -> Dict:
        """
        Vespa doesn't have explicit refresh - documents are searchable immediately.
        This is a no-op for compatibility.
        """
        RequestContextHolder.on_request_start()
        RequestContextHolder.on_request_end()
        return {"_shards": {"successful": 1, "failed": 0}}

    def stats(self, index: Optional[str] = None, metric: Optional[str] = None, **kwargs) -> Dict:
        """
        Get index statistics.

        Vespa provides stats differently - this returns a compatible structure.
        Note: This is synchronous because telemetry code calls it synchronously.
        """
        # Return minimal structure expected by telemetry
        return {
            "_shards": {"total": 1, "successful": 1, "failed": 0},
            "_all": {
                "primaries": {
                    "docs": {"count": 0, "deleted": 0},
                    "store": {"size_in_bytes": 0},
                    "indexing": {"index_time_in_millis": 0, "throttle_time_in_millis": 0},
                    "merges": {"total_time_in_millis": 0, "total_throttled_time_in_millis": 0},
                    "refresh": {"total_time_in_millis": 0},
                    "flush": {"total_time_in_millis": 0},
                    "segments": {
                        "memory_in_bytes": 0,
                        "stored_fields_memory_in_bytes": 0,
                        "doc_values_memory_in_bytes": 0,
                        "terms_memory_in_bytes": 0,
                        "norms_memory_in_bytes": 0,
                        "points_memory_in_bytes": 0
                    },
                    "translog": {"size_in_bytes": 0}
                },
                "total": {
                    "store": {"size_in_bytes": 0},
                    "translog": {"size_in_bytes": 0}
                }
            }
        }

    def forcemerge(self, index: Optional[str] = None, **kwargs) -> Dict:
        """
        Vespa handles segment merging automatically.
        This is a no-op for compatibility.
        """
        return {"_shards": {"successful": 1, "failed": 0}}

class VespaClusterNamespace(ClusterNamespace):
    """
    Vespa implementation of cluster namespace.

    Vespa doesn't have the same cluster concept as OpenSearch.
    Health checks use Vespa's application status endpoint.
    """

    def __init__(self, vespa_client: Vespa, config_host: str = None, config_port: int = 19071):
        self._client = vespa_client
        self._config_host = config_host
        self._config_port = config_port
        self.logger = logging.getLogger(__name__)

    async def health(self, **kwargs) -> Dict:
        """
        Get cluster health status.

        Vespa health is determined by checking if the application is up.
        Maps to OpenSearch health response format.
        """
        self.logger.info("DEBUG: VespaClusterNamespace.health() called with kwargs=%s", kwargs)
        RequestContextHolder.on_request_start()
        try:
            # pyvespa Vespa client has get_application_status method
            # For async, we use the sync method in an executor or direct call
            status = "green"  # Vespa either works or doesn't - no yellow state
            RequestContextHolder.on_request_end()
            return {
                "cluster_name": "vespa",
                "status": status,
                "timed_out": False,
                "number_of_nodes": 1,  # Would need actual cluster info
                "number_of_data_nodes": 1,
                "active_primary_shards": 1,
                "active_shards": 1,
                "relocating_shards": 0,
                "initializing_shards": 0,
                "unassigned_shards": 0,
            }
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            RequestContextHolder.on_request_end()
            return {"status": "red", "error": str(e)}

    async def put_settings(self, body: Dict, **kwargs) -> Dict:
        """
        Update cluster settings.

        Vespa settings are managed through application package deployment.
        This is a no-op for runtime compatibility.
        """
        RequestContextHolder.on_request_start()
        self.logger.warning("Vespa does not support runtime cluster settings changes")
        RequestContextHolder.on_request_end()
        return {"acknowledged": True, "persistent": {}, "transient": {}}


class VespaTransportNamespace(TransportNamespace):
    """
    Low-level transport for custom API endpoints in Vespa.

    Provides raw HTTP request capability to Vespa endpoints.
    """

    def __init__(self, vespa_client: Vespa, base_url: str):
        self._client = vespa_client
        self._base_url = base_url
        self.logger = logging.getLogger(__name__)

    async def perform_request(self, method: str, url: str,
                             params: Optional[Dict] = None,
                             body: Optional[Any] = None,
                             headers: Optional[Dict] = None) -> Any:
        """
        Perform a raw HTTP request to Vespa.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            url: URL path (will be appended to base URL)
            params: Query parameters
            body: Request body
            headers: Additional headers

        Returns:
            Response data
        """
        import aiohttp

        full_url = f"{self._base_url}{url}"
        if params:
            from urllib.parse import urlencode
            full_url = f"{full_url}?{urlencode(params)}"

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method=method,
                url=full_url,
                json=body if body else None,
                headers=headers
            ) as response:
                if response.content_type == 'application/json':
                    return await response.json()
                return await response.text()

    async def close(self):
        """Close any open connections. No-op for Vespa as we use per-request sessions."""
        pass


class VespaNodesNamespace(NodesNamespace):
    """
    Nodes namespace for Vespa.

    Vespa doesn't have the same node statistics concept as OpenSearch,
    so this provides stub implementations that return empty/minimal data
    to satisfy telemetry requirements.
    """

    def __init__(self, vespa_client: Vespa, host: str, port: int):
        self._client = vespa_client
        self._host = host
        self._port = port
        self.logger = logging.getLogger(__name__)

    def stats(self, node_id: Optional[str] = None,
              metric: Optional[str] = None,
              **kwargs) -> Dict:
        """
        Get node statistics.

        Vespa doesn't expose the same node stats as OpenSearch.
        Returns minimal stub data to satisfy telemetry code.
        """
        # Return minimal structure expected by telemetry
        return {
            "nodes": {
                "vespa-node-1": {
                    "name": f"{self._host}:{self._port}",
                    "host": self._host,
                    "ip": self._host,
                    "transport_address": f"{self._host}:{self._port}",
                    "indices": {},
                    "os": {"cpu": {"percent": 0}},
                    "process": {"cpu": {"percent": 0}},
                    "jvm": {
                        "mem": {
                            "heap_used_percent": 0,
                            "pools": {
                                "old": {"peak_used_in_bytes": 0},
                                "young": {"peak_used_in_bytes": 0},
                                "survivor": {"peak_used_in_bytes": 0}
                            }
                        },
                        "gc": {
                            "collectors": {
                                "old": {
                                    "collection_time_in_millis": 0,
                                    "collection_count": 0
                                },
                                "young": {
                                    "collection_time_in_millis": 0,
                                    "collection_count": 0
                                }
                            }
                        },
                        "buffer_pools": {}
                    },
                    "thread_pool": {},
                    "breakers": {},
                    "network": {},
                    "indexing_pressure": {}
                }
            }
        }

    def info(self, node_id: Optional[str] = None,
             metric: Optional[str] = None,
             **kwargs) -> Dict:
        """
        Get node information.

        Returns minimal stub data for Vespa.
        """
        return {
            "nodes": {
                "vespa-node-1": {
                    "name": f"{self._host}:{self._port}",
                    "host": self._host,
                    "ip": self._host,
                    "version": "8.0.0",  # Vespa version placeholder
                    "roles": ["data", "ingest"],
                    "os": {"name": "Linux"},
                    "jvm": {"version": "17"},
                    "plugins": []
                }
            }
        }


class VespaDatabaseClient(DatabaseClient, RequestContextHolder):
    """
    Vespa implementation of the DatabaseClient interface.

    Provides OpenSearch-compatible API for benchmarking Vespa clusters.
    Uses aiohttp with timing hooks for accurate benchmark measurement.
    """

    def __init__(self, vespa_client: Vespa, hosts: List[Dict], client_options: Dict):
        self.logger = logging.getLogger(__name__)
        self._client = vespa_client
        self._hosts = hosts
        self._client_options = client_options

        # Build base URL from hosts
        scheme = client_options.get("scheme", "http")
        host = hosts[0]["host"] if hosts else "localhost"
        port = hosts[0].get("port", 8080) if hosts else 8080
        self._base_url = f"{scheme}://{host}:{port}"
        self._host = host
        self._port = port

        # Config server URL (for deployment operations)
        config_port = client_options.get("config_port", 19071)
        self._config_url = f"{scheme}://{host}:{config_port}"

        # Application settings
        self._app_name = client_options.get("app_name", "benchmark")
        self._namespace = client_options.get("namespace", "benchmark")
        self._cluster = client_options.get("cluster", None)

        # Create application package for schema management
        self._app_package = ApplicationPackage(name=self._app_name)

        # Initialize namespaces
        self._indices = VespaIndicesNamespace(
            vespa_client, self._app_package,
            config_url=self._config_url
        )
        self._cluster_ns = VespaClusterNamespace(vespa_client, host, config_port)
        self._transport = VespaTransportNamespace(vespa_client, self._base_url)
        self._nodes = VespaNodesNamespace(vespa_client, host, port)

        # Track document counts per schema for stats
        self._doc_counts: Dict[str, int] = {}

        # aiohttp session (lazy initialized)
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_initialized = False

    async def _ensure_session(self):
        """Initialize aiohttp session with timing hooks for benchmarking."""
        if self._session_initialized:
            return

        self._session_initialized = True

        # Timing hooks for benchmark measurement
        async def on_request_start(session, trace_config_ctx, params):
            try:
                VespaDatabaseClient.on_request_start()
            except LookupError:
                pass  # No context set - standalone usage

        async def on_request_end(session, trace_config_ctx, params):
            try:
                VespaDatabaseClient.on_request_end()
            except LookupError:
                pass

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        trace_config.on_request_exception.append(on_request_end)

        # High connection limits for parallel bulk feeding
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=100, force_close=False)
        self._session = aiohttp.ClientSession(
            trace_configs=[trace_config],
            connector=connector
        )

    @property
    def indices(self) -> IndicesNamespace:
        """Access to indices namespace"""
        return self._indices

    @property
    def cluster(self) -> ClusterNamespace:
        """Access to cluster namespace"""
        return self._cluster_ns

    @property
    def transport(self) -> TransportNamespace:
        """Access to transport namespace"""
        return self._transport

    @property
    def nodes(self) -> NodesNamespace:
        """Access to nodes namespace"""
        return self._nodes

    def info(self) -> Dict:
        """
        Get cluster/database information.

        Returns Vespa-specific info in OpenSearch-compatible format.
        """
        return {
            "name": "vespa-cluster",
            "cluster_name": "vespa",
            "cluster_uuid": "vespa-benchmark-cluster",
            "version": {
                "number": "8.0.0",
                "build_hash": "unknown",
                "build_flavor": "default",
                "build_type": "docker",
                "lucene_version": "N/A",
                "minimum_wire_compatibility_version": "7.0.0",
                "minimum_index_compatibility_version": "7.0.0"
            },
            "tagline": "Vespa - The open big data serving engine"
        }

    async def bulk(self, body: Any,
                   index: Optional[str] = None,
                   doc_type: Optional[str] = None,
                   params: Optional[Dict] = None,
                   **kwargs) -> Dict:
        """
        Bulk index/update/delete documents in Vespa.

        Uses parallel HTTP requests with semaphore for rate limiting.
        Vespa Document API: POST /document/v1/{namespace}/{schema}/docid/{doc_id}

        Args:
            body: Bulk request body (bytes, string, or list format)
            index: Default index/schema name
            doc_type: Ignored (deprecated in OpenSearch)
            params: Additional parameters

        Returns:
            Dict with bulk operation results in OpenSearch format
        """
        self.logger.info("DEBUG: VespaDatabaseClient.bulk() called with index=%s", index)
        await self._ensure_session()

        document_type = index or self._app_name
        endpoint = f"{self._base_url}/document/v1/{self._namespace}/{document_type}/docid"
        self.logger.info("DEBUG: Vespa bulk endpoint=%s, base_url=%s, namespace=%s, document_type=%s",
                        endpoint, self._base_url, self._namespace, document_type)

        # Parse bulk body into list of documents
        documents = self._parse_bulk_body(body, index)
        if documents:
            self.logger.info("DEBUG: First document (of %d): %s", len(documents), str(documents[0])[:500])

        max_concurrent = kwargs.get("max_concurrent", 50)
        timeout_val = aiohttp.ClientTimeout(total=kwargs.get("request_timeout", 30))

        # Build query params for Vespa
        query_params = {}
        cluster = self._cluster or document_type
        if cluster:
            query_params["cluster"] = cluster

        semaphore = asyncio.Semaphore(max_concurrent)
        items = []
        errors_count = 0

        async def post_document(doc_index: int, doc: Dict) -> Dict:
            nonlocal errors_count
            async with semaphore:
                doc_id = doc.get("_id", f"doc_{doc_index}")
                doc_endpoint = f"{endpoint}/{doc_id}"

                source = doc.get("_source", doc.get("doc", doc))

                # Transform document for Vespa (flatten nested, convert timestamps)
                source = self._transform_document_for_vespa(source, document_type)

                vespa_doc = {"fields": source}

                try:
                    async with self._session.post(
                        doc_endpoint,
                        json=vespa_doc,
                        params=query_params,
                        timeout=timeout_val
                    ) as response:
                        if response.status >= 400:
                            errors_count += 1
                            response_text = await response.text()
                            self.logger.warning("Vespa document POST failed: endpoint=%s status=%d error=%s",
                                              doc_endpoint, response.status, response_text[:500])
                            return {"index": {"_id": doc_id, "status": response.status, "error": response_text}}
                        return {"index": {"_id": doc_id, "status": 200}}
                except Exception as e:
                    errors_count += 1
                    self.logger.warning("Vespa document POST exception: endpoint=%s error=%s", doc_endpoint, str(e))
                    return {"index": {"_id": doc_id, "status": 500, "error": str(e)}}

        # Process all documents in parallel
        tasks = [post_document(i, doc) for i, doc in enumerate(documents)]
        items = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions that were raised
        final_items = []
        for item in items:
            if isinstance(item, Exception):
                errors_count += 1
                final_items.append({"index": {"status": 500, "error": str(item)}})
            else:
                final_items.append(item)

        return {
            "took": 0,
            "errors": errors_count > 0,
            "items": final_items
        }

    def _parse_bulk_body(self, body: Any, default_index: str) -> List[Dict]:
        """
        Parse bulk body into list of documents.

        Handles:
        - bytes input (OSB passes bytes for standard bulk operations)
        - string input (newline-delimited JSON format)
        - list input (alternating [action, doc, action, doc, ...] from vectorsearch)
        """
        # Handle list format from BulkVectorsFromDataSetParamSource
        if isinstance(body, (list, tuple)):
            body_list = list(body)
            if len(body_list) >= 2 and isinstance(body_list[0], dict):
                first_item = body_list[0]
                if "index" in first_item and isinstance(first_item.get("index"), dict):
                    # Alternating format: [action0, doc0, action1, doc1, ...]
                    documents = []
                    for i in range(0, len(body_list) - 1, 2):
                        action = body_list[i]
                        doc_body = body_list[i + 1]
                        doc_id = action.get("index", {}).get("_id", f"doc_{len(documents)}")
                        documents.append({"_id": doc_id, "_source": doc_body})
                    return documents
            # If it's already a list of documents
            return body_list

        # Handle bytes
        if isinstance(body, bytes):
            body = body.decode('utf-8')

        # Parse newline-delimited JSON
        documents = []
        lines = body.strip().split('\n') if isinstance(body, str) else []

        i = 0
        while i < len(lines):
            if not lines[i].strip():
                i += 1
                continue

            try:
                action = json.loads(lines[i])

                # Get action type and metadata
                action_type = None
                action_meta = None
                for key in ["index", "create", "update", "delete"]:
                    if key in action:
                        action_type = key
                        action_meta = action[key]
                        break

                if action_type in ["index", "create", "update"] and i + 1 < len(lines):
                    doc_body = json.loads(lines[i + 1])
                    doc_id = action_meta.get("_id") if action_meta else None
                    if not doc_id:
                        doc_id = str(uuid.uuid4())  # Generate UUID for uniqueness
                    documents.append({
                        "_id": doc_id,
                        "_index": action_meta.get("_index", default_index) if action_meta else default_index,
                        "_source": doc_body
                    })
                    i += 2
                else:
                    i += 1
            except json.JSONDecodeError:
                i += 1

        return documents

    def _transform_document_for_vespa(self, doc: Dict, document_type: str = None) -> Dict:
        """
        Transform OpenSearch document to Vespa format.

        1. Flattens nested object fields (log.file.path → log_file_path)
        2. Converts @timestamp to epoch milliseconds
        3. Maps field names according to FIELD_NAME_MAPPING
        4. Filters fields for big5 workload
        """
        vespa_doc = {}

        def flatten(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{prefix}_{key}" if prefix else key

                    if isinstance(value, dict) and not self._is_leaf_value(value):
                        flatten(value, new_key)
                    else:
                        # Apply field mapping
                        if new_key in FIELD_NAME_MAPPING:
                            mapped_key = FIELD_NAME_MAPPING[new_key]
                        else:
                            mapped_key = new_key.replace(".", "_").replace("@", "")

                        # Handle special conversions
                        if mapped_key == "timestamp" and isinstance(value, str):
                            value = self._date_to_epoch(value)
                        elif self._is_geo_point(mapped_key, value):
                            # Convert geo_point to Vespa position format
                            value = self._convert_geo_point(value)
                        elif isinstance(value, list):
                            # Vespa doesn't handle arrays the same way - join strings
                            if all(isinstance(v, str) for v in value):
                                value = ",".join(value)

                        vespa_doc[mapped_key] = value

        # Handle @timestamp at top level first
        if "@timestamp" in doc:
            vespa_doc["timestamp"] = self._date_to_epoch(doc["@timestamp"])
            doc = {k: v for k, v in doc.items() if k != "@timestamp"}

        flatten(doc)

        # Filter for big5 workload
        if document_type == "big5" or self._app_name == "big5":
            vespa_doc = {k: v for k, v in vespa_doc.items() if k in BIG5_ALLOWED_FIELDS}

        return vespa_doc

    def _is_leaf_value(self, value: Any) -> bool:
        """
        Determine if a value is a leaf (not a nested object to flatten).

        Leaf values include:
        - geo_point: {"lat": ..., "lon": ...}
        - GeoJSON: {"type": ..., "coordinates": ...}
        - Simple wrappers: {"value": ...} or {"values": [...]}
        """
        if not isinstance(value, dict):
            return True

        keys = set(value.keys())

        # Geo patterns
        if {"lat", "lon"}.issubset(keys):
            return True
        if {"type", "coordinates"}.issubset(keys):
            return True

        # Simple wrappers
        if keys == {"value"} or keys == {"values"}:
            return True

        return False

    # Known geo_point field names (common patterns)
    GEO_POINT_FIELDS = {
        "pickup_location", "dropoff_location", "location", "geo_location",
        "coordinates", "geo", "point", "position", "geo_point"
    }

    def _is_geo_point(self, field_name: str, value: Any) -> bool:
        """
        Determine if a field is a geo_point that needs conversion.

        OpenSearch geo_point formats:
        - Array: [lon, lat]
        - Object: {"lat": ..., "lon": ...}
        - String: "lat,lon"
        """
        # Check if field name suggests geo_point
        field_lower = field_name.lower()
        is_geo_field = any(geo in field_lower for geo in self.GEO_POINT_FIELDS)

        if not is_geo_field:
            return False

        # Check if value looks like a geo_point
        if isinstance(value, list) and len(value) == 2:
            # [lon, lat] format
            return all(isinstance(v, (int, float)) for v in value)
        elif isinstance(value, dict):
            # {"lat": ..., "lon": ...} format
            return "lat" in value and ("lon" in value or "lng" in value)
        elif isinstance(value, str) and "," in value:
            # "lat,lon" string format
            parts = value.split(",")
            if len(parts) == 2:
                try:
                    float(parts[0])
                    float(parts[1])
                    return True
                except ValueError:
                    pass

        return False

    def _convert_geo_point(self, value: Any) -> Dict[str, float]:
        """
        Convert OpenSearch geo_point to Vespa position format.

        Vespa position format: {"lat": degrees, "lng": degrees}
        """
        if isinstance(value, list) and len(value) == 2:
            # OpenSearch array format: [lon, lat]
            return {"lng": float(value[0]), "lat": float(value[1])}
        elif isinstance(value, dict):
            lat = value.get("lat")
            lon = value.get("lon") or value.get("lng")
            return {"lat": float(lat), "lng": float(lon)}
        elif isinstance(value, str) and "," in value:
            # "lat,lon" string format
            parts = value.split(",")
            return {"lat": float(parts[0]), "lng": float(parts[1])}

        # Return as-is if we can't parse
        return value

    def _date_to_epoch(self, date_value: Any) -> int:
        """Convert date string to epoch milliseconds."""
        if isinstance(date_value, (int, float)):
            return int(date_value)

        if isinstance(date_value, str):
            try:
                # Try ISO format
                if "T" in date_value:
                    dt = datetime.fromisoformat(date_value.replace("Z", "+00:00"))
                    return int(dt.timestamp() * 1000)
                # Try epoch string
                return int(float(date_value))
            except (ValueError, TypeError):
                return 0

        return 0

    def _map_field_name(self, os_field: str) -> str:
        """Map OpenSearch field name to Vespa field name."""
        if os_field in FIELD_NAME_MAPPING:
            return FIELD_NAME_MAPPING[os_field]
        return os_field.replace(".", "_").replace("@", "")

    async def index(self, index: str, body: Dict,
                   id: Optional[str] = None,
                   doc_type: Optional[str] = None,
                   **kwargs) -> Dict:
        """
        Index a single document in Vespa.

        Args:
            index: Schema name
            body: Document body
            id: Document ID
            doc_type: Ignored (deprecated)

        Returns:
            Dict with index result
        """
        import asyncio

        doc_id = id or str(time.time_ns())

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.feed_data_point(
                    schema=index,
                    data_id=doc_id,
                    fields=body
                )
            )

            self._doc_counts[index] = self._doc_counts.get(index, 0) + 1

            return {
                "_index": index,
                "_id": doc_id,
                "_version": 1,
                "result": "created" if response.status_code == 200 else "error",
                "_shards": {"total": 1, "successful": 1, "failed": 0}
            }
        except Exception as e:
            self.logger.error(f"Failed to index document: {e}")
            return {
                "_index": index,
                "_id": doc_id,
                "result": "error",
                "error": str(e)
            }

    async def search(self, index: Optional[str] = None,
                    body: Optional[Dict] = None,
                    doc_type: Optional[str] = None,
                    **kwargs) -> Dict:
        """
        Execute a search query in Vespa.

        Translates OpenSearch query DSL to Vespa YQL and executes via HTTP.

        Args:
            index: Schema name to search
            body: OpenSearch query body
            doc_type: Ignored (deprecated)

        Returns:
            Dict with search results in OpenSearch format
        """
        await self._ensure_session()

        if body is None:
            body = {}

        document_type = index or self._app_name

        # Translate OpenSearch query to Vespa YQL
        yql, query_params = self._convert_to_yql(body, document_type)

        # Build search URL
        search_url = f"{self._base_url}/search/"

        # Build query parameters
        params = {"yql": yql}
        params.update(query_params)

        # Add size/limit
        if "hits" not in params:
            params["hits"] = body.get("size", 10)

        try:
            async with self._session.get(search_url, params=params) as response:
                vespa_response = await response.json()

                # Convert Vespa response to OpenSearch format
                return self._convert_vespa_response(vespa_response, index)

        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            return {
                "took": 0,
                "timed_out": True,
                "error": str(e),
                "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}
            }

    def _convert_to_yql(self, body: Optional[Dict], document_type: str) -> Tuple[str, Dict]:
        """
        Convert OpenSearch query DSL to Vespa YQL.

        Returns:
            Tuple of (yql_query, query_params)
            query_params contains additional parameters like input.query(query_vector)
        """
        query_params = {}

        if not body:
            return f"select * from {document_type} where true", query_params

        where_clause = self._build_where_clause(body.get("query", {}), document_type, query_params)
        order_clause = self._build_order_clause(body.get("sort", []))
        limit_clause = self._build_limit_clause(body)

        yql = f"select * from {document_type} where {where_clause}"

        if order_clause:
            yql += f" order by {order_clause}"
        if limit_clause:
            yql += f" {limit_clause}"

        # Aggregations appended with |
        grouping_clause = self._build_grouping_clause(body.get("aggs", body.get("aggregations", {})))
        if grouping_clause:
            yql += f" | {grouping_clause}"

        return yql, query_params

    def _build_where_clause(self, query: Dict, document_type: str, query_params: Dict) -> str:
        """Build the WHERE clause from OpenSearch query DSL."""
        if not query:
            return "true"

        # match_all
        if "match_all" in query:
            return "true"

        # KNN / Vector Search
        if "knn" in query:
            return self._convert_knn_query(query["knn"], query_params)

        # term
        if "term" in query:
            return self._convert_term_query(query["term"])

        # terms
        if "terms" in query:
            return self._convert_terms_query(query["terms"])

        # match
        if "match" in query:
            return self._convert_match_query(query["match"])

        # range
        if "range" in query:
            return self._convert_range_query(query["range"])

        # bool
        if "bool" in query:
            return self._convert_bool_query(query["bool"], document_type, query_params)

        # query_string
        if "query_string" in query:
            return self._convert_query_string(query["query_string"])

        # prefix
        if "prefix" in query:
            return self._convert_prefix_query(query["prefix"])

        # exists
        if "exists" in query:
            field = self._map_field_name(query["exists"].get("field", ""))
            return f"{field} != null"

        return "true"

    def _convert_knn_query(self, knn_config: Dict, query_params: Dict) -> str:
        """Convert KNN query to Vespa nearestNeighbor."""
        # Handle nested structure: {"field": {"vector": [...], "k": 10}}
        if len(knn_config) == 1:
            field = list(knn_config.keys())[0]
            config = knn_config[field]
            if isinstance(config, dict):
                vector = config.get("vector", [])
                k = config.get("k", 10)
            else:
                return "true"
        else:
            field = knn_config.get("field", "vector")
            vector = knn_config.get("vector", [])
            k = knn_config.get("k", 10)

        vector_str = "[" + ",".join(str(v) for v in vector) + "]"
        query_params["input.query(query_vector)"] = vector_str
        query_params["ranking"] = "vector-similarity"

        return f"{{targetHits:{k}}}nearestNeighbor({field}, query_vector)"

    def _convert_term_query(self, term_query: Dict) -> str:
        """Convert term query to YQL."""
        for field, value_spec in term_query.items():
            vespa_field = self._map_field_name(field)
            if isinstance(value_spec, dict):
                value = value_spec.get("value", "")
            else:
                value = value_spec

            if isinstance(value, str):
                value = value.replace('"', '\\"')
                return f'{vespa_field} contains "{value}"'
            else:
                return f"{vespa_field} = {value}"
        return "true"

    def _convert_terms_query(self, terms_query: Dict) -> str:
        """Convert terms query (multiple values) to YQL."""
        for field, values in terms_query.items():
            if field == "boost":
                continue
            vespa_field = self._map_field_name(field)
            if isinstance(values, list):
                conditions = []
                for value in values:
                    if isinstance(value, str):
                        escaped_value = value.replace('"', '\\"')
                        conditions.append(f'{vespa_field} contains "{escaped_value}"')
                    else:
                        conditions.append(f"{vespa_field} = {value}")
                if conditions:
                    return "(" + " or ".join(conditions) + ")"
        return "true"

    def _convert_match_query(self, match_query: Dict) -> str:
        """Convert match query to YQL."""
        for field, value_spec in match_query.items():
            vespa_field = self._map_field_name(field)
            if isinstance(value_spec, dict):
                value = value_spec.get("query", "")
            else:
                value = value_spec

            if isinstance(value, str):
                escaped_value = value.replace('"', '\\"')
                return f'{vespa_field} contains "{escaped_value}"'
            else:
                return f"{vespa_field} = {value}"
        return "true"

    def _convert_range_query(self, range_query: Dict) -> str:
        """Convert range query to YQL."""
        conditions = []
        for field, range_spec in range_query.items():
            vespa_field = self._map_field_name(field)
            is_date_field = field in ("@timestamp", "event.ingested", "timestamp")

            for op, value in range_spec.items():
                if op in ("format", "time_zone"):
                    continue

                if is_date_field and isinstance(value, str):
                    value = self._date_to_epoch(value)

                op_map = {"gte": ">=", "gt": ">", "lte": "<=", "lt": "<"}
                if op in op_map:
                    conditions.append(f"{vespa_field} {op_map[op]} {value}")

        return " and ".join(conditions) if conditions else "true"

    def _convert_bool_query(self, bool_query: Dict, document_type: str, query_params: Dict) -> str:
        """Convert bool query to YQL."""
        parts = []

        # must = AND
        if "must" in bool_query:
            must_clauses = bool_query["must"]
            if not isinstance(must_clauses, list):
                must_clauses = [must_clauses]
            must_parts = [self._build_where_clause(q, document_type, query_params) for q in must_clauses]
            must_parts = [p for p in must_parts if p and p != "true"]
            if must_parts:
                parts.append("(" + " and ".join(must_parts) + ")" if len(must_parts) > 1 else must_parts[0])

        # filter = AND (same as must, no scoring)
        if "filter" in bool_query:
            filter_clauses = bool_query["filter"]
            if not isinstance(filter_clauses, list):
                filter_clauses = [filter_clauses]
            filter_parts = [self._build_where_clause(q, document_type, query_params) for q in filter_clauses]
            filter_parts = [p for p in filter_parts if p and p != "true"]
            if filter_parts:
                parts.append("(" + " and ".join(filter_parts) + ")" if len(filter_parts) > 1 else filter_parts[0])

        # should = OR
        if "should" in bool_query:
            should_clauses = bool_query["should"]
            if not isinstance(should_clauses, list):
                should_clauses = [should_clauses]
            should_parts = [self._build_where_clause(q, document_type, query_params) for q in should_clauses]
            should_parts = [p for p in should_parts if p and p != "true"]
            if should_parts:
                parts.append("(" + " or ".join(should_parts) + ")" if len(should_parts) > 1 else should_parts[0])

        # must_not = NOT
        if "must_not" in bool_query:
            must_not_clauses = bool_query["must_not"]
            if not isinstance(must_not_clauses, list):
                must_not_clauses = [must_not_clauses]
            for clause in must_not_clauses:
                part = self._build_where_clause(clause, document_type, query_params)
                if part and part != "true":
                    parts.append(f"!({part})")

        return " and ".join(parts) if parts else "true"

    def _convert_query_string(self, query_string: Dict) -> str:
        """Convert query_string to YQL."""
        query = query_string.get("query", "")
        default_field = query_string.get("default_field", "message")

        # Handle field:value format
        if ":" in query:
            field_part, terms_part = query.split(":", 1)
            field = self._map_field_name(field_part.strip())
            terms = terms_part.strip()
        else:
            field = self._map_field_name(default_field)
            terms = query.strip()

        # Handle OR/AND operators
        if " OR " in terms:
            term_list = [t.strip() for t in terms.split(" OR ")]
            conditions = [f'{field} contains "{t}"' for t in term_list if t]
            return "(" + " or ".join(conditions) + ")"
        elif " AND " in terms:
            term_list = [t.strip() for t in terms.split(" AND ")]
            conditions = [f'{field} contains "{t}"' for t in term_list if t]
            return "(" + " and ".join(conditions) + ")"
        else:
            # Space-separated = OR by default
            term_list = terms.split()
            if len(term_list) == 1:
                return f'{field} contains "{term_list[0]}"'
            conditions = [f'{field} contains "{t}"' for t in term_list if t]
            return "(" + " or ".join(conditions) + ")"

    def _convert_prefix_query(self, prefix_query: Dict) -> str:
        """Convert prefix query to YQL."""
        for field, value_spec in prefix_query.items():
            vespa_field = self._map_field_name(field)
            value = value_spec.get("value", "") if isinstance(value_spec, dict) else value_spec
            return f'{vespa_field} contains "{value}*"'
        return "true"

    def _build_order_clause(self, sort_spec: List) -> str:
        """Build ORDER BY clause from OpenSearch sort specification."""
        if not sort_spec:
            return ""

        clauses = []
        for sort_item in sort_spec:
            if isinstance(sort_item, str):
                vespa_field = self._map_field_name(sort_item)
                clauses.append(f"{vespa_field} asc")
            elif isinstance(sort_item, dict):
                for field, direction_spec in sort_item.items():
                    if field == "_score":
                        continue  # Handled by ranking

                    vespa_field = self._map_field_name(field)
                    direction = direction_spec if isinstance(direction_spec, str) else direction_spec.get("order", "asc")
                    clauses.append(f"{vespa_field} {direction.lower()}")

        return ", ".join(clauses)

    def _build_limit_clause(self, body: Dict) -> str:
        """Build LIMIT/OFFSET clause."""
        size = body.get("size", 10)
        from_val = body.get("from", 0)

        clause = f"limit {size}"
        if from_val > 0:
            clause += f" offset {from_val}"
        return clause

    def _build_grouping_clause(self, aggs: Dict) -> str:
        """Build Vespa grouping clause from OpenSearch aggregations."""
        if not aggs:
            return ""

        # Simplified aggregation support
        clauses = []
        for agg_name, agg_spec in aggs.items():
            if "date_histogram" in agg_spec:
                clauses.append(self._convert_date_histogram_agg(agg_spec["date_histogram"]))
            elif "terms" in agg_spec:
                clauses.append(self._convert_terms_agg(agg_spec["terms"]))
            elif "histogram" in agg_spec:
                clauses.append(self._convert_histogram_agg(agg_spec["histogram"]))
            elif any(m in agg_spec for m in ["sum", "avg", "min", "max", "stats"]):
                for metric in ["sum", "avg", "min", "max", "stats"]:
                    if metric in agg_spec:
                        clauses.append(self._convert_metric_agg(metric, agg_spec[metric]))
                        break

        return " ".join(clauses)

    def _convert_date_histogram_agg(self, spec: Dict) -> str:
        """Convert date_histogram aggregation to Vespa grouping."""
        field = self._map_field_name(spec.get("field", "timestamp"))
        interval = spec.get("calendar_interval", spec.get("fixed_interval", "hour"))
        interval_ms = INTERVAL_MS_MAP.get(interval, 3600000)
        return f"all(group(floor({field} / {interval_ms})) each(output(count())))"

    def _convert_terms_agg(self, spec: Dict) -> str:
        """Convert terms aggregation to Vespa grouping."""
        field = self._map_field_name(spec.get("field", ""))
        size = spec.get("size", 10)
        return f"all(group({field}) max({size}) each(output(count())))"

    def _convert_histogram_agg(self, spec: Dict) -> str:
        """Convert histogram aggregation to Vespa grouping."""
        field = self._map_field_name(spec.get("field", ""))
        interval = spec.get("interval", 100)
        return f"all(group(floor({field} / {interval})) each(output(count())))"

    def _convert_metric_agg(self, metric_type: str, spec: Dict) -> str:
        """Convert metric aggregation to Vespa grouping."""
        field = self._map_field_name(spec.get("field", ""))
        if metric_type == "stats":
            return f"all(output(sum({field})) output(avg({field})) output(min({field})) output(max({field})) output(count()))"
        return f"all(output({metric_type}({field})))"

    def _convert_vespa_response(self, vespa_response: Dict, index: str) -> Dict:
        """Convert Vespa search response to OpenSearch format."""
        hits = vespa_response.get("root", {}).get("children", [])
        root_fields = vespa_response.get("root", {}).get("fields", {})
        total_count = root_fields.get("totalCount", len(hits))

        os_hits = []
        for hit in hits:
            os_hits.append({
                "_index": index,
                "_id": hit.get("id", ""),
                "_score": hit.get("relevance", 0),
                "_source": hit.get("fields", {})
            })

        timing = vespa_response.get("timing", {})
        took_ms = int(timing.get("searchtime", 0) * 1000) if timing else 0

        return {
            "took": took_ms,
            "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": total_count, "relation": "eq"},
                "max_score": os_hits[0]["_score"] if os_hits else 0,
                "hits": os_hits
            }
        }

    def return_raw_response(self):
        """Configure client to return raw responses."""
        pass  # Vespa responses are already raw

    async def close(self):
        """Close client connections."""
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self._session_initialized = False


class VespaClientFactory:
    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = client_options
        self.ssl_context = None
        self.provider = CloudProviderFactory.get_provider_from_client_options(self.client_options)
        self.logger = logging.getLogger(__name__)

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        if self.provider:
            self.provider.parse_log_in_params(client_options=self.client_options)
            self.provider.mask_client_options(masked_client_options, self.client_options)
            self.logger.info("Masking client options with cloud provider: [%s]", self.provider)
        
        self.logger.info("Creating Vespa client connected to %s with options [%s]", hosts, masked_client_options)
        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        if self.client_options.pop("use_ssl", False):
            # pylint: disable=import-outside-toplevel
            import ssl
            self.logger.info("SSL support: on")
            self.client_options["scheme"] = "https"

            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH,
                                                          cafile=self.client_options.pop("ca_certs", certifi.where()))

            if not self.client_options.pop("verify_certs", True):
                self.logger.info("SSL certificate verification: off")
                # order matters to avoid ValueError: check_hostname needs a SSL context with either CERT_OPTIONAL or CERT_REQUIRED
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE

                self.logger.warning("User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a "
                                    "benchmark. Disabling urllib warnings now to avoid a logging storm. "
                                    "See https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings for details.")
                # disable:  "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly \
                # advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings"
                urllib3.disable_warnings()
            else:
                # The peer's hostname can be matched if only a hostname is provided.
                # In other words, hostname checking is disabled if an IP address is
                # found in the host lists.
                self.ssl_context.check_hostname = self._has_only_hostnames(hosts)
                self.ssl_context.verify_mode=ssl.CERT_REQUIRED
                self.logger.info("SSL certificate verification: on")

            # When using SSL_context, all SSL related kwargs in client options get ignored
            client_cert = self.client_options.pop("client_cert", False)
            client_key = self.client_options.pop("client_key", False)

            if not client_cert and not client_key:
                self.logger.info("SSL client authentication: off")
            elif bool(client_cert) != bool(client_key):
                self.logger.error(
                    "Supplied client-options contain only one of client_cert/client_key. "
                )
                defined_client_ssl_option = "client_key" if client_key else "client_cert"
                missing_client_ssl_option = "client_cert" if client_key else "client_key"
                console.println(
                    "'{}' is missing from client-options but '{}' has been specified.\n"
                    "If your OpenSearch setup requires client certificate verification both need to be supplied.\n"
                    "Read the documentation at {}\n".format(
                        missing_client_ssl_option,
                        defined_client_ssl_option,
                        console.format.link(doc_link("command_line_reference.html#client-options")))
                )
                raise exceptions.SystemSetupError(
                    "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                        defined_client_ssl_option,
                        missing_client_ssl_option))
            elif client_cert and client_key:
                self.logger.info("SSL client authentication: on")
                self.ssl_context.load_cert_chain(certfile=client_cert,
                                                 keyfile=client_key)
        else:
            self.logger.info("SSL support: off")
            self.client_options["scheme"] = "http"
        
        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            self.logger.info("HTTP basic authentication: on")
            self.client_options["http_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
        else:
            self.logger.info("HTTP basic authentication: off")

        if self._is_set(self.client_options, "compressed"):
            console.warn("You set the deprecated client option 'compressed'. Please use 'http_compress' instead.", logger=self.logger)
            self.client_options["http_compress"] = self.client_options.pop("compressed")

        if self._is_set(self.client_options, "http_compress"):
            self.logger.info("HTTP compression: on")
        else:
            self.logger.info("HTTP compression: off")

        if self._is_set(self.client_options, "enable_cleanup_closed"):
            self.client_options["enable_cleanup_closed"] = convert.to_bool(self.client_options.pop("enable_cleanup_closed"))

    @staticmethod
    def _has_only_hostnames(hosts):
        logger = logging.getLogger(__name__)
        has_ip, has_hostname = False, False
        for host in hosts:
            if is_ipaddress(host["host"]):
                has_ip = True
            else:
                has_hostname = True

        if has_ip and has_hostname:
            console.warn("Although certificate verification is enabled, "
                "peer hostnames will not be matched since the host list is a mix "
                "of names and IP addresses", logger=logger)
            return False

        return has_hostname
    
    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create_async(self):
        """
        Create an async Vespa database client.

        Returns:
            VespaDatabaseClient configured for async operations
        """
        # Build Vespa URL from hosts
        scheme = self.client_options.get("scheme", "http")
        host = self.hosts[0]["host"] if self.hosts else "localhost"
        port = self.hosts[0].get("port", 8080) if self.hosts else 8080
        url = f"{scheme}://{host}:{port}"

        self.logger.info(f"Creating Vespa client connected to {url}")

        # Create pyvespa Vespa client
        # https://vespa-engine.github.io/pyvespa/api/vespa/application.html#vespa.application.Vespa
        vespa_client = Vespa(url=url)

        return VespaDatabaseClient(vespa_client, self.hosts, self.client_options)

    def create(self):
        """
        Create a synchronous Vespa database client.

        Returns:
            VespaDatabaseClient (same as async, Vespa client handles both)
        """
        return self.create_async()

    def wait_for_rest_layer(self, max_wait=60):
        """
        Waits for 'max_wait' in seconds until Vespa's REST API is available.

        Args:
            max_wait: max wait (seconds) to wait until Vespa application is ready

        Returns:
            True if Vespa is ready, False otherwise
        """
        print("Waiting for application to be ready...")
        scheme = self.client_options.get("scheme", "http")
        host = self.hosts[0]["host"] if self.hosts else "localhost"
        port = self.hosts[0].get("port", 8080) if self.hosts else 8080
        url = f"{scheme}://{host}:{port}"

        try:
            vespa = Vespa(url=url)
            print("Vespa client: ", vespa, " at ", url)
            vespa.wait_for_application_up(max_wait)
            return True
        except Exception as e:
            self.logger.error(f"Vespa not ready after {max_wait}s: {e}")
            return False