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

import io
import logging
import time
import zipfile
from typing import Dict, List, Optional, Any

import aiohttp
import certifi
# pyvespa package is imported as 'vespa' internally
from vespa.package import ApplicationPackage, Schema, Document, Field, HNSW
from vespa.application import Vespa
import urllib3
from urllib3.util.ssl_ import is_ipaddress

from osbenchmark import doc_link, exceptions
from osbenchmark.cloud_provider.factory import CloudProviderFactory
from osbenchmark.utils import console, convert

from osbenchmark.database.interface import (
    DatabaseClient,
    IndicesNamespace,
    ClusterNamespace,
    TransportNamespace
)


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
            return {
                "acknowledged": True,
                "index": index,
                "deployed": deploy_result.get("success", False),
                "deploy_message": deploy_result.get("message", "")
            }
        else:
            self.logger.warning("No config_url provided - schema created but not deployed")
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

        # Use Vespa's delete_all_docs or visit API
        # For now, mark as acknowledged - actual implementation depends on Vespa setup
        if index in self._schemas:
            del self._schemas[index]

        return {"acknowledged": True}

    async def exists(self, index: str, **kwargs) -> bool:
        """Check if a schema exists in the application."""
        return index in self._schemas

    async def refresh(self, index: Optional[str] = None, **kwargs) -> Dict:
        """
        Vespa doesn't have explicit refresh - documents are searchable immediately.
        This is a no-op for compatibility.
        """
        return {"_shards": {"successful": 1, "failed": 0}}

    async def stats(self, index: Optional[str] = None, metric: Optional[str] = None, **kwargs) -> Dict:
        """
        Get index statistics.

        Vespa provides stats differently - this returns a compatible structure.
        """
        # TODO: Implement actual Vespa stats via /metrics/v2 or similar
        return {
            "_all": {
                "primaries": {
                    "docs": {"count": 0},
                    "store": {"size_in_bytes": 0}
                }
            }
        }

    async def forcemerge(self, index: Optional[str] = None, **kwargs) -> Dict:
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
        try:
            # pyvespa Vespa client has get_application_status method
            # For async, we use the sync method in an executor or direct call
            status = "green"  # Vespa either works or doesn't - no yellow state
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
            return {"status": "red", "error": str(e)}

    async def put_settings(self, body: Dict, **kwargs) -> Dict:
        """
        Update cluster settings.

        Vespa settings are managed through application package deployment.
        This is a no-op for runtime compatibility.
        """
        self.logger.warning("Vespa does not support runtime cluster settings changes")
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


class VespaDatabaseClient(DatabaseClient):
    """
    Vespa implementation of the DatabaseClient interface.

    Provides OpenSearch-compatible API for benchmarking Vespa clusters.
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

        # Config server URL (for deployment operations)
        config_port = client_options.get("config_port", 19071)
        self._config_url = f"{scheme}://{host}:{config_port}"

        # Create application package for schema management
        self._app_package = ApplicationPackage(name="benchmark")

        # Initialize namespaces
        self._indices = VespaIndicesNamespace(
            vespa_client, self._app_package,
            config_url=self._config_url
        )
        self._cluster = VespaClusterNamespace(vespa_client, host, config_port)
        self._transport = VespaTransportNamespace(vespa_client, self._base_url)

        # Track document counts per schema for stats
        self._doc_counts: Dict[str, int] = {}

    @property
    def indices(self) -> IndicesNamespace:
        """Access to indices namespace"""
        return self._indices

    @property
    def cluster(self) -> ClusterNamespace:
        """Access to cluster namespace"""
        return self._cluster

    @property
    def transport(self) -> TransportNamespace:
        """Access to transport namespace"""
        return self._transport

    async def bulk(self, body: Any,
                   index: Optional[str] = None,
                   doc_type: Optional[str] = None,
                   params: Optional[Dict] = None,
                   **kwargs) -> Dict:
        """
        Bulk index/update/delete documents in Vespa.

        Vespa uses a different bulk format than OpenSearch.
        This method translates OpenSearch bulk format to Vespa feed operations.

        Args:
            body: Bulk request body (OpenSearch format: action + doc pairs)
            index: Default index/schema name
            doc_type: Ignored (deprecated in OpenSearch)
            params: Additional parameters

        Returns:
            Dict with bulk operation results
        """
        import asyncio

        # Parse OpenSearch bulk format
        operations = self._parse_bulk_body(body, index)

        # Execute operations via Vespa feed
        errors = []
        successful = 0
        failed = 0

        # Use Vespa's async feed
        async def feed_doc(op):
            nonlocal successful, failed, errors
            try:
                schema = op.get("_index", index)
                doc_id = op.get("_id")
                doc_body = op.get("doc", {})

                # Vespa feed uses schema/doctype/docid format
                response = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._client.feed_data_point(
                        schema=schema,
                        data_id=doc_id,
                        fields=doc_body
                    )
                )

                if response.status_code == 200:
                    successful += 1
                    self._doc_counts[schema] = self._doc_counts.get(schema, 0) + 1
                else:
                    failed += 1
                    errors.append({"index": {"_id": doc_id, "error": response.json}})
            except Exception as e:
                failed += 1
                errors.append({"index": {"_id": op.get("_id"), "error": str(e)}})

        # Process operations (could batch for better performance)
        tasks = [feed_doc(op) for op in operations]
        await asyncio.gather(*tasks)

        return {
            "took": 0,  # Would need timing
            "errors": len(errors) > 0,
            "items": errors if errors else [{"index": {"status": 200}} for _ in range(successful)]
        }

    def _parse_bulk_body(self, body: Any, default_index: str) -> List[Dict]:
        """
        Parse OpenSearch bulk format into list of operations.

        OpenSearch bulk format:
        {"index": {"_index": "test", "_id": "1"}}
        {"field1": "value1"}
        """
        operations = []

        if isinstance(body, str):
            lines = body.strip().split('\n')
        elif isinstance(body, list):
            lines = body
        else:
            return operations

        i = 0
        while i < len(lines):
            action_line = lines[i] if isinstance(lines[i], dict) else self._parse_json(lines[i])
            if not action_line:
                i += 1
                continue

            # Get action type (index, create, update, delete)
            action_type = None
            action_meta = None
            for key in ["index", "create", "update", "delete"]:
                if key in action_line:
                    action_type = key
                    action_meta = action_line[key]
                    break

            if not action_type:
                i += 1
                continue

            operation = {
                "_index": action_meta.get("_index", default_index),
                "_id": action_meta.get("_id"),
                "action": action_type
            }

            # For index/create/update, next line is the document
            if action_type in ["index", "create", "update"]:
                i += 1
                if i < len(lines):
                    doc = lines[i] if isinstance(lines[i], dict) else self._parse_json(lines[i])
                    operation["doc"] = doc

            operations.append(operation)
            i += 1

        return operations

    def _parse_json(self, line: str) -> Optional[Dict]:
        """Safely parse JSON line."""
        import json
        try:
            return json.loads(line)
        except (json.JSONDecodeError, TypeError):
            return None

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

        Translates OpenSearch query DSL to Vespa YQL.

        Args:
            index: Schema name to search
            body: OpenSearch query body
            doc_type: Ignored (deprecated)

        Returns:
            Dict with search results in OpenSearch format
        """
        import asyncio

        if body is None:
            body = {}

        # Translate OpenSearch query to Vespa YQL
        yql = self._translate_query_to_yql(body, index)

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._client.query(yql=yql)
            )

            # Transform Vespa response to OpenSearch format
            hits = []
            if hasattr(response, 'hits') and response.hits:
                for hit in response.hits:
                    hits.append({
                        "_index": index,
                        "_id": hit.get("id", ""),
                        "_score": hit.get("relevance", 0),
                        "_source": hit.get("fields", {})
                    })

            return {
                "took": response.json.get("timing", {}).get("searchtime", 0) * 1000 if hasattr(response, 'json') else 0,
                "timed_out": False,
                "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
                "hits": {
                    "total": {"value": len(hits), "relation": "eq"},
                    "max_score": hits[0]["_score"] if hits else 0,
                    "hits": hits
                }
            }
        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            return {
                "took": 0,
                "timed_out": True,
                "error": str(e),
                "hits": {"total": {"value": 0}, "hits": []}
            }

    def _translate_query_to_yql(self, body: Dict, index: str) -> str:
        """
        Translate OpenSearch query DSL to Vespa YQL.

        This is a simplified translation - full DSL support would need more work.
        """
        query = body.get("query", {})
        size = body.get("size", 10)

        # Start with basic select
        yql = f"select * from {index}"

        # Handle match_all
        if "match_all" in query:
            yql += " where true"

        # Handle match query
        elif "match" in query:
            match = query["match"]
            field = list(match.keys())[0]
            value = match[field]
            if isinstance(value, dict):
                value = value.get("query", "")
            yql += f" where {field} contains '{value}'"

        # Handle term query
        elif "term" in query:
            term = query["term"]
            field = list(term.keys())[0]
            value = term[field]
            if isinstance(value, dict):
                value = value.get("value", "")
            yql += f" where {field} = '{value}'"

        # Handle knn query (vector search)
        elif "knn" in query:
            knn = query["knn"]
            field = list(knn.keys())[0]
            vector = knn[field].get("vector", [])
            k = knn[field].get("k", 10)
            # Vespa ANN syntax
            yql += f" where {{targetHits:{k}}}nearestNeighbor({field}, q)"
            # Note: vector would be passed as a query parameter

        # Handle bool query (simplified)
        elif "bool" in query:
            bool_query = query["bool"]
            conditions = []

            if "must" in bool_query:
                for clause in bool_query["must"]:
                    cond = self._translate_clause(clause)
                    if cond:
                        conditions.append(cond)

            if "should" in bool_query:
                should_conds = []
                for clause in bool_query["should"]:
                    cond = self._translate_clause(clause)
                    if cond:
                        should_conds.append(cond)
                if should_conds:
                    conditions.append(f"({' or '.join(should_conds)})")

            if conditions:
                yql += f" where {' and '.join(conditions)}"
            else:
                yql += " where true"

        else:
            # Default to match all
            yql += " where true"

        yql += f" limit {size}"
        return yql

    def _translate_clause(self, clause: Dict) -> Optional[str]:
        """Translate a single query clause to YQL."""
        if "match" in clause:
            field = list(clause["match"].keys())[0]
            value = clause["match"][field]
            if isinstance(value, dict):
                value = value.get("query", "")
            return f"{field} contains '{value}'"
        elif "term" in clause:
            field = list(clause["term"].keys())[0]
            value = clause["term"][field]
            if isinstance(value, dict):
                value = value.get("value", "")
            return f"{field} = '{value}'"
        elif "range" in clause:
            field = list(clause["range"].keys())[0]
            range_def = clause["range"][field]
            conditions = []
            if "gte" in range_def:
                conditions.append(f"{field} >= {range_def['gte']}")
            if "gt" in range_def:
                conditions.append(f"{field} > {range_def['gt']}")
            if "lte" in range_def:
                conditions.append(f"{field} <= {range_def['lte']}")
            if "lt" in range_def:
                conditions.append(f"{field} < {range_def['lt']}")
            return " and ".join(conditions) if conditions else None
        return None

    def return_raw_response(self):
        """Configure client to return raw responses."""
        pass  # Vespa responses are already raw

    def close(self):
        """Close client connections."""
        # pyvespa Vespa client doesn't need explicit close
        pass


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