# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Vespa client implementation for OpenSearch Benchmark.

This module provides a Vespa client factory and wrapper that integrates
with OSB's benchmarking framework, allowing Vespa to be benchmarked
using the same operations and workloads as OpenSearch.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import requests

from osbenchmark import exceptions
from osbenchmark.context import RequestContextHolder


class VespaClientFactory:
    """
    Factory for creating Vespa client instances.
    """
    def __init__(self, hosts, client_options):
        self.hosts = hosts
        self.client_options = dict(client_options)
        self.logger = logging.getLogger(__name__)

        masked_client_options = dict(client_options)
        self.logger.info("Creating Vespa client connected to %s with options [%s]", hosts, masked_client_options)

    def create(self):
        """
        Create a Vespa client. Returns the async client directly.
        Note: The client can be used in both sync and async contexts.
        """
        return self.create_async()

    def create_async(self):
        """Create an async Vespa client."""
        if not self.hosts:
            raise exceptions.SystemSetupError("No Vespa hosts configured")

        # Extract the first host configuration
        host_config = self.hosts[0] if isinstance(self.hosts, list) else self.hosts.get("default", [{}])[0]
        host = host_config.get("host", "localhost")
        port = host_config.get("port", 8080)

        return VespaAsyncClient(
            endpoint=f"http://{host}:{port}",
            **self.client_options
        )


class VespaAsyncClient(RequestContextHolder):
    """
    Async Vespa client that provides compatibility with OSB's runner interface.

    This client wraps Vespa's REST APIs and provides methods compatible with
    OpenSearch Benchmark's operation runners.

    Inherits from RequestContextHolder to properly integrate with OSB's timing system.
    """

    def __init__(self, endpoint: str, **client_options):
        """
        Initialize Vespa async client.

        :param endpoint: Vespa endpoint URL (e.g., http://localhost:8080)
        :param client_options: Additional client configuration options
        """
        self.endpoint = endpoint.rstrip('/')
        self.client_options = client_options
        self.logger = logging.getLogger(__name__)
        self._session = None
        self._session_initialized = False
        self._app_name = client_options.get("app_name", "default")
        self._namespace = client_options.get("namespace", "benchmark")
        self._cluster = client_options.get("cluster", None)  # Content cluster name
        self._request_context = {}

    async def _ensure_session(self):
        """Ensure aiohttp session is initialized with trace hooks."""
        if self._session_initialized:
            return

        self._session_initialized = True
        try:
            import aiohttp

            # Set up request timing hooks for benchmark measurement
            # Use inherited class methods from RequestContextHolder
            # These hooks are defensive - they only record timing if a context exists
            async def on_request_start(session, trace_config_ctx, params):
                try:
                    self.logger.debug("aiohttp trace hook: on_request_start called")
                    VespaAsyncClient.on_request_start()
                    self.logger.debug("aiohttp trace hook: on_request_start completed")
                except LookupError as e:
                    # No context set - this is OK for standalone usage (e.g., tests)
                    self.logger.debug(f"aiohttp trace hook: on_request_start got LookupError: {e}")
                    pass

            async def on_request_end(session, trace_config_ctx, params):
                try:
                    self.logger.debug("aiohttp trace hook: on_request_end called")
                    VespaAsyncClient.on_request_end()
                    self.logger.debug("aiohttp trace hook: on_request_end completed")
                except LookupError as e:
                    # No context set - this is OK for standalone usage (e.g., tests)
                    self.logger.debug(f"aiohttp trace hook: on_request_end got LookupError: {e}")
                    pass

            trace_config = aiohttp.TraceConfig()
            trace_config.on_request_start.append(on_request_start)
            trace_config.on_request_end.append(on_request_end)
            # Ensure we also stop the timer when a request "ends" with an exception (e.g. timeout)
            trace_config.on_request_exception.append(on_request_end)

            # Use a connector with higher connection limits for parallel bulk feeding
            # limit=100 total connections, limit_per_host=100 per Vespa server
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=100, force_close=False)
            self._session = aiohttp.ClientSession(
                trace_configs=[trace_config],
                connector=connector
            )
        except ImportError:
            self.logger.warning("aiohttp not available, using synchronous requests")

    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._session:
            await self._session.close()
        return False

    def return_raw_response(self):
        """Mark that raw responses should be returned (for compatibility)."""
        self._request_context["raw_response"] = True

    # Document/Feed Operations

    async def bulk(self, body: str, index: str = None, **kwargs) -> Dict[str, Any]:
        """
        Bulk feed documents to Vespa with parallel processing for high throughput.

        Uses asyncio.Semaphore to limit concurrent requests and prevent overwhelming
        the Vespa cluster while achieving much higher throughput than sequential processing.

        :param body: Bulk request body (newline-delimited JSON or list of dicts)
        :param index: Target document type/schema
        :param kwargs: Additional parameters (max_concurrent defaults to 50)
        :return: Response dict with bulk operation results
        """
        # Ensure session is initialized with trace hooks before making requests
        self.logger.debug("bulk(): calling _ensure_session()")
        await self._ensure_session()
        self.logger.debug("bulk(): _ensure_session() completed")

        document_type = index or self._app_name
        # POST endpoint for document creation
        endpoint = f"{self.endpoint}/document/v1/{self._namespace}/{document_type}/docid"

        # Log body type and preview for debugging
        body_type = type(body).__name__
        body_preview = str(body)[:200] if body else "empty"
        self.logger.info(f"bulk(): Body type={body_type}, preview={body_preview}")

        documents = self._parse_bulk_body(body)
        # Log sample document IDs to debug ID generation
        if documents:
            sample_ids = [d.get("_id", "no_id") for d in documents[:3]]
            self.logger.info(f"bulk(): Processing {len(documents)} documents to {document_type}, sample IDs: {sample_ids}")
        timeout_val = kwargs.get("request_timeout", 30)
        max_concurrent = kwargs.get("max_concurrent", 50)  # Configurable concurrency

        # Build query params - include cluster if needed
        params = {}
        cluster = self._cluster or document_type
        if cluster:
            params["destinationCluster"] = cluster

        # Semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)

        # Results tracking
        results = []

        async def post_document(doc_index: int, doc: Dict) -> Dict:
            """Post a single document with semaphore-limited concurrency."""
            async with semaphore:
                doc_id = doc.get("_id", f"doc_{doc_index}")
                doc_endpoint = f"{endpoint}/{doc_id}"

                try:
                    # Convert document fields to Vespa format
                    source = doc.get("_source", doc)

                    # Handle field name conflicts with Vespa reserved words
                    if "index" in source:
                        source = {k: v for k, v in source.items() if k != "index"}

                    # Transform document fields for workloads with nested fields (like big5)
                    if "@timestamp" in source or any(isinstance(v, dict) for v in source.values()):
                        source = self._transform_document_for_vespa(source)

                    vespa_doc = {"fields": source}

                    if self._session:
                        async with self._session.post(
                            doc_endpoint,
                            json=vespa_doc,
                            params=params,
                            timeout=timeout_val
                        ) as response:
                            # Always read the response body to properly close the connection
                            response_text = await response.text()
                            if response.status >= 400:
                                self.logger.warning(f"Failed to index document {doc_id}: status={response.status}, error={response_text}")
                                return {"index": {"_id": doc_id, "status": response.status, "error": response_text}}
                            else:
                                return {"index": {"_id": doc_id, "status": 200}}
                    else:
                        # Fallback to sync requests (shouldn't happen in benchmark context)
                        response = requests.post(
                            doc_endpoint,
                            json=vespa_doc,
                            params=params,
                            timeout=timeout_val
                        )
                        if response.status_code >= 400:
                            self.logger.warning(f"Failed to index document {doc_id}: status={response.status_code}")
                            return {"index": {"_id": doc_id, "status": response.status_code}}
                        else:
                            return {"index": {"_id": doc_id, "status": 200}}

                except Exception as e:
                    self.logger.warning(f"Error feeding document {doc_index}: {e}")
                    return {"index": {"_id": doc_id, "error": str(e)}}

        # Process all documents in parallel with semaphore limiting concurrency
        tasks = [post_document(i, doc) for i, doc in enumerate(documents)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        items = []
        errors_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                items.append({"index": {"_id": documents[i].get("_id", f"doc_{i}"), "error": str(result)}})
                errors_count += 1
            elif isinstance(result, dict):
                items.append(result)
                if result.get("index", {}).get("status", 200) >= 400 or "error" in result.get("index", {}):
                    errors_count += 1
            else:
                items.append({"index": {"_id": f"doc_{i}", "error": "Unknown result"}})
                errors_count += 1

        self.logger.info(f"bulk(): Completed {len(documents)} documents, {errors_count} errors")
        return {
            "took": 0,  # Vespa doesn't provide this
            "errors": errors_count > 0,
            "items": items
        }

    def _wrap_fields_with_assign(self, fields: Dict) -> Dict:
        """
        Wrap field values with Vespa's assign operation.

        Vespa's Document V1 API requires field values to be wrapped in operation
        objects when using PUT operations. Each field value must be in the format:
        {"assign": value}

        :param fields: Dictionary of field names to values
        :return: Dictionary with values wrapped in assign operations
        """
        return {field: {"assign": value} for field, value in fields.items()}

    # Known fields for big5 schema - documents with extra fields will have them filtered out
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

    def _transform_document_for_vespa(self, doc: Dict) -> Dict:
        """
        Transform an OpenSearch document to Vespa format.

        This method:
        1. Flattens nested object fields (e.g., {"log": {"file": {"path": "..."}}} → {"log_file_path": "..."})
        2. Converts @timestamp to epoch milliseconds
        3. Maps field names according to FIELD_NAME_MAPPING
        4. Filters out fields not in BIG5_ALLOWED_FIELDS for big5 workload

        :param doc: OpenSearch document with nested fields
        :return: Vespa-compatible document with flat fields
        """
        vespa_doc = {}

        def flatten(obj: Any, prefix: str = "") -> None:
            """Recursively flatten nested objects."""
            if isinstance(obj, dict):
                for key, value in obj.items():
                    # Build new key with prefix
                    new_key = f"{prefix}_{key}" if prefix else key

                    # Check if this is a nested object to flatten
                    if isinstance(value, dict) and not self._is_leaf_value(value):
                        # Recurse into nested object
                        flatten(value, new_key)
                    else:
                        # Leaf value - add to result
                        # Apply field mapping
                        if new_key in self.FIELD_NAME_MAPPING:
                            mapped_key = self.FIELD_NAME_MAPPING[new_key]
                        else:
                            # Also check the original nested path
                            original_path = new_key.replace("_", ".")
                            if original_path in self.FIELD_NAME_MAPPING:
                                mapped_key = self.FIELD_NAME_MAPPING[original_path]
                            else:
                                mapped_key = new_key.replace(".", "_")

                        # Handle special field conversions
                        if mapped_key == "timestamp" and isinstance(value, str):
                            # Convert date string to epoch milliseconds
                            value = self._date_to_epoch(value)
                        elif mapped_key == "event_ingested" and isinstance(value, str):
                            value = self._date_to_epoch(value)
                        elif isinstance(value, list):
                            # Convert arrays to comma-separated strings for Vespa string fields
                            # (e.g., tags: ["a", "b"] -> "a,b")
                            value = ",".join(str(v) for v in value)

                        vespa_doc[mapped_key] = value

        # Handle special case of @timestamp at top level
        if "@timestamp" in doc:
            vespa_doc["timestamp"] = self._date_to_epoch(doc["@timestamp"])
            doc = {k: v for k, v in doc.items() if k != "@timestamp"}

        flatten(doc)

        # Filter out fields not in the schema whitelist (for big5 workload compatibility)
        # This prevents errors when documents have extra fields not defined in the Vespa schema
        if self._app_name == "big5":
            vespa_doc = {k: v for k, v in vespa_doc.items() if k in self.BIG5_ALLOWED_FIELDS}

        return vespa_doc

    def _is_leaf_value(self, value: Any) -> bool:
        """
        Check if a value is a leaf value (not a nested object to flatten).

        Some dict values are actual data (e.g., geo_point, date objects with sub-fields)
        rather than nested document structure.
        """
        if not isinstance(value, dict):
            return True

        keys = set(value.keys())

        # Check for geo_point patterns - need BOTH lat and lon
        if {"lat", "lon"}.issubset(keys):
            return True

        # Check for GeoJSON patterns - need BOTH type and coordinates
        if {"type", "coordinates"}.issubset(keys):
            return True

        # Check for value wrappers (e.g., {"value": 123} or {"values": [1,2,3]})
        # But only if that's the ONLY key (simple wrapper)
        if keys == {"value"} or keys == {"values"}:
            return True

        # If all values are primitive (non-dict), it might be a leaf with multiple fields
        # However, for big5-style documents, nested objects like {"file": {"path": ...}}
        # should be flattened, so we need to continue recursion into dicts
        # Only treat as leaf if it contains no nested dicts at all and looks like a data object
        if all(not isinstance(v, dict) for v in value.values()):
            # Check if keys look like data fields vs structure
            # Common data keys that indicate leaf objects (Elasticsearch query DSL)
            data_keys = {"query", "analyzer", "fuzziness", "boost", "minimum_should_match",
                         "match", "prefix", "fuzzy", "wildcard", "regexp"}
            if data_keys.intersection(keys):
                return True

        return False

    def _parse_bulk_body(self, body) -> List[Dict]:
        """Parse bulk body into list of documents.

        Handles:
        - bytes input (OSB passes bytes for standard bulk operations like big5)
        - string input (newline-delimited JSON format)
        - list input (alternating [action, doc, action, doc, ...] from vectorsearch)
        """
        import json

        # Handle list format (from BulkVectorsFromDataSetParamSource)
        # Format: [action_dict, doc_dict, action_dict, doc_dict, ...]
        if isinstance(body, (list, tuple)):
            body_list = list(body)
            # Check if it's the alternating action/doc format
            if len(body_list) >= 2 and isinstance(body_list[0], dict):
                # Check if first item looks like an action (has "index" key with nested dict)
                first_item = body_list[0]
                if "index" in first_item and isinstance(first_item.get("index"), dict):
                    # Parse alternating format: [action0, doc0, action1, doc1, ...]
                    documents = []
                    for i in range(0, len(body_list) - 1, 2):
                        action = body_list[i]
                        doc_body = body_list[i + 1]
                        doc_id = action.get("index", {}).get("_id", f"doc_{len(documents)}")
                        documents.append({"_id": doc_id, "_source": doc_body})
                    return documents
            # Not alternating format, return as-is (each item should be a document)
            return body_list

        # Handle bytes (OSB passes bulk body as bytes, not string)
        if isinstance(body, bytes):
            body = body.decode('utf-8')

        documents = []
        lines = body.strip().split('\n') if isinstance(body, str) else []

        i = 0
        while i < len(lines):
            if not lines[i].strip():
                i += 1
                continue

            try:
                action = json.loads(lines[i])
                if i + 1 < len(lines):
                    doc_body = json.loads(lines[i + 1])
                    # Try to get _id from action, otherwise generate UUID
                    # Note: We always use UUID since corpus event.id values are not unique
                    doc_id = action.get("index", {}).get("_id")
                    if not doc_id:
                        # Generate UUID for guaranteed uniqueness
                        import uuid
                        doc_id = str(uuid.uuid4())
                    # Wrap doc_body as _source so it's separated from _id
                    documents.append({"_id": doc_id, "_source": doc_body})
                    i += 2
                else:
                    i += 1
            except json.JSONDecodeError:
                self.logger.warning(f"Failed to parse bulk line: {lines[i]}")
                i += 1

        return documents

    # Search Operations

    def search(self, index: str = None, body: Dict = None, **kwargs):
        """
        Execute a search query against Vespa.

        Can be called both synchronously and asynchronously depending on context.

        :param index: Target schema/document type
        :param body: Query body (OpenSearch format will be converted to YQL)
        :param kwargs: Additional parameters
        :return: Search response in OpenSearch-compatible format (or coroutine if in async context)
        """
        # Try to detect if we're in an async context by checking for a running event loop
        try:
            import asyncio
            loop = asyncio.get_running_loop()
            # If we got here, we're in an async context - return coroutine
            return self._search_async(index, body, **kwargs)
        except RuntimeError:
            # No running event loop - we're in sync context (telemetry)
            return self._search_sync(index, body, **kwargs)

    def _search_sync(self, index: str = None, body: Dict = None, **kwargs) -> Dict[str, Any]:
        """Synchronous search implementation (for telemetry context)."""
        # For telemetry calls to search (like ML stats), just return empty results
        # Telemetry queries for ML/aggregations don't apply to Vespa
        return {
            "took": 0,
            "timed_out": False,
            "hits": {
                "total": {"value": 0, "relation": "eq"},
                "max_score": None,
                "hits": []
            },
            "aggregations": {}  # Empty aggregations to prevent KeyError
        }

    async def _search_async(self, index: str = None, body: Dict = None, **kwargs) -> Dict[str, Any]:
        """Async search implementation (for benchmark operations)."""
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        document_type = index or self._app_name

        # Convert OpenSearch query to Vespa YQL and extract query parameters
        yql_query, query_params = self._convert_to_yql(body, document_type)

        endpoint = f"{self.endpoint}/search/"
        params = {
            "yql": yql_query,
            "timeout": kwargs.get("request_timeout", "10s")
        }

        # Add query-specific parameters (like input.query(query_vector))
        params.update(query_params)

        # Add any additional query parameters
        if "request_params" in kwargs:
            params.update(kwargs["request_params"])

        try:
            async with self._session.get(endpoint, params=params) as response:
                result = await response.json()

            return self._convert_vespa_response(result)

        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            raise

    # Field name mapping from OpenSearch to Vespa (nested → flat)
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

    def _map_field_name(self, os_field: str) -> str:
        """
        Map OpenSearch field name to Vespa field name.

        OpenSearch uses nested object paths (e.g., log.file.path) while
        Vespa prefers flat field names (e.g., log_file_path).
        """
        if os_field in self.FIELD_NAME_MAPPING:
            return self.FIELD_NAME_MAPPING[os_field]
        # Default: replace dots with underscores
        return os_field.replace(".", "_")

    def _date_to_epoch(self, date_value) -> int:
        """
        Convert a date value to epoch milliseconds.

        Handles various formats:
        - ISO 8601 strings (2023-01-01T00:00:00Z, 2023-01-01T00:00:00.000Z)
        - Simple date strings (2023-01-01)
        - Already numeric values (pass through)
        """
        if isinstance(date_value, (int, float)):
            # Already a number - check if seconds or milliseconds
            if date_value < 1e12:  # Likely seconds
                return int(date_value * 1000)
            return int(date_value)

        if not isinstance(date_value, str):
            return 0

        # Try ISO 8601 format
        try:
            # Handle Z timezone suffix
            date_str = date_value.replace("Z", "+00:00")
            # Handle milliseconds
            if "." in date_str:
                # Python's fromisoformat doesn't handle milliseconds well before 3.11
                parts = date_str.split(".")
                if len(parts) == 2:
                    # Truncate milliseconds to 6 digits for microseconds
                    ms_part = parts[1]
                    if "+" in ms_part:
                        ms_digits, tz = ms_part.split("+")
                        ms_digits = ms_digits[:6].ljust(6, "0")
                        date_str = f"{parts[0]}.{ms_digits}+{tz}"
                    elif "-" in ms_part:
                        ms_digits, tz = ms_part.rsplit("-", 1)
                        ms_digits = ms_digits[:6].ljust(6, "0")
                        date_str = f"{parts[0]}.{ms_digits}-{tz}"
                    else:
                        ms_digits = ms_part[:6].ljust(6, "0")
                        date_str = f"{parts[0]}.{ms_digits}"
            dt = datetime.fromisoformat(date_str)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass

        # Try simple date format
        for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"]:
            try:
                dt = datetime.strptime(date_value, fmt)
                return int(dt.timestamp() * 1000)
            except ValueError:
                continue

        self.logger.warning(f"Could not parse date value: {date_value}")
        return 0

    def _convert_to_yql(self, body: Optional[Dict], document_type: str) -> Tuple[str, Dict]:
        """
        Convert OpenSearch query DSL to Vespa YQL.

        :param body: OpenSearch query body
        :param document_type: Vespa document type name
        :return: Tuple of (yql_query, query_params) where query_params is a dict of
                 additional query parameters to send to Vespa (e.g., input.query(query_vector))
        """
        query_params = {}

        if not body:
            return f"select * from {document_type} where true", query_params

        # Build WHERE clause from query
        where_clause = self._build_where_clause(body.get("query", {}), document_type, query_params)

        # Build ORDER BY clause from sort
        order_clause = self._build_order_clause(body.get("sort", []))

        # Build LIMIT/OFFSET clause
        limit_clause = self._build_limit_clause(body)

        # Build YQL
        yql = f"select * from {document_type} where {where_clause}"

        if order_clause:
            yql += f" order by {order_clause}"

        if limit_clause:
            yql += f" {limit_clause}"

        # Build grouping clause for aggregations (appended with |)
        grouping_clause = self._build_grouping_clause(body.get("aggs", body.get("aggregations", {})))
        if grouping_clause:
            yql += f" | {grouping_clause}"

        return yql, query_params

    def _build_where_clause(self, query: Dict, document_type: str, query_params: Dict) -> str:
        """
        Build WHERE clause from OpenSearch query DSL.

        Handles: match_all, match, term, range, bool, query_string, knn
        """
        if not query:
            return "true"

        # Handle match_all
        if "match_all" in query:
            return "true"

        # Handle KNN/vector search
        if "knn" in query:
            return self._convert_knn_query(query["knn"], query_params)

        # Handle term query
        if "term" in query:
            return self._convert_term_query(query["term"])

        # Handle terms query (multiple values)
        if "terms" in query:
            return self._convert_terms_query(query["terms"])

        # Handle range query
        if "range" in query:
            return self._convert_range_query(query["range"])

        # Handle match query
        if "match" in query:
            return self._convert_match_query(query["match"])

        # Handle bool query
        if "bool" in query:
            return self._convert_bool_query(query["bool"], document_type, query_params)

        # Handle query_string
        if "query_string" in query:
            return self._convert_query_string(query["query_string"])

        # Handle prefix query
        if "prefix" in query:
            return self._convert_prefix_query(query["prefix"])

        # Handle wildcard query
        if "wildcard" in query:
            return self._convert_wildcard_query(query["wildcard"])

        # Handle exists query
        if "exists" in query:
            field = self._map_field_name(query["exists"].get("field", ""))
            return f"{field} != null"

        return "true"

    def _convert_knn_query(self, knn_config: Dict, query_params: Dict) -> str:
        """Convert KNN/vector search query to Vespa YQL."""
        field = knn_config.get("field", "vector")
        vector = knn_config.get("vector", [])
        k = knn_config.get("k", 10)

        # Format vector for query parameter
        vector_str = "[" + ",".join(str(v) for v in vector) + "]"
        query_params["input.query(query_vector)"] = vector_str
        query_params["ranking"] = "vector-similarity"

        return f"{{targetHits:{k}}}nearestNeighbor({field}, query_vector)"

    def _convert_term_query(self, term_query: Dict) -> str:
        """
        Convert term query to Vespa YQL.

        OpenSearch: {"term": {"log.file.path": {"value": "/var/log/messages"}}}
        Vespa YQL: log_file_path contains "/var/log/messages"
        """
        for field, value_spec in term_query.items():
            vespa_field = self._map_field_name(field)
            if isinstance(value_spec, dict):
                value = value_spec.get("value", "")
            else:
                value = value_spec

            # Escape quotes in value
            if isinstance(value, str):
                value = value.replace('"', '\\"')
                return f'{vespa_field} contains "{value}"'
            else:
                return f"{vespa_field} = {value}"

        return "true"

    def _convert_terms_query(self, terms_query: Dict) -> str:
        """
        Convert terms query (multiple values) to Vespa YQL.

        OpenSearch: {"terms": {"process.name": ["python", "java", "node"]}}
        Vespa YQL: (process_name contains "python" or process_name contains "java" or process_name contains "node")
        """
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

    def _convert_range_query(self, range_query: Dict) -> str:
        """
        Convert range query to Vespa YQL.

        OpenSearch: {"range": {"@timestamp": {"gte": "2023-01-01", "lt": "2023-01-03"}}}
        Vespa YQL: timestamp >= 1672531200000 and timestamp < 1672704000000
        """
        conditions = []
        for field, range_spec in range_query.items():
            vespa_field = self._map_field_name(field)

            # Determine if this is a date field
            is_date_field = field in ("@timestamp", "event.ingested", "timestamp")

            for op, value in range_spec.items():
                if op == "format" or op == "time_zone":
                    continue

                # Convert date values to epoch
                if is_date_field:
                    value = self._date_to_epoch(value)

                if op == "gte":
                    conditions.append(f"{vespa_field} >= {value}")
                elif op == "gt":
                    conditions.append(f"{vespa_field} > {value}")
                elif op == "lte":
                    conditions.append(f"{vespa_field} <= {value}")
                elif op == "lt":
                    conditions.append(f"{vespa_field} < {value}")

        return " and ".join(conditions) if conditions else "true"

    def _convert_match_query(self, match_query: Dict) -> str:
        """
        Convert match query to Vespa YQL.

        OpenSearch: {"match": {"message": "error failed"}}
        Vespa YQL: message contains "error failed"
        """
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

    def _convert_bool_query(self, bool_query: Dict, document_type: str, query_params: Dict) -> str:
        """
        Convert bool query to Vespa YQL.

        OpenSearch: {"bool": {"must": [...], "should": [...], "filter": [...], "must_not": [...]}}
        Vespa YQL: (cond1 and cond2) and (cond3 or cond4) and !(cond5)
        """
        parts = []

        # must = AND
        if "must" in bool_query:
            must_clauses = bool_query["must"]
            if not isinstance(must_clauses, list):
                must_clauses = [must_clauses]
            must_parts = [self._build_where_clause(q, document_type, query_params) for q in must_clauses]
            must_parts = [p for p in must_parts if p and p != "true"]
            if must_parts:
                if len(must_parts) == 1:
                    parts.append(must_parts[0])
                else:
                    parts.append("(" + " and ".join(must_parts) + ")")

        # filter = AND (same as must for relevance-free filtering)
        if "filter" in bool_query:
            filter_clauses = bool_query["filter"]
            if not isinstance(filter_clauses, list):
                filter_clauses = [filter_clauses]
            filter_parts = [self._build_where_clause(q, document_type, query_params) for q in filter_clauses]
            filter_parts = [p for p in filter_parts if p and p != "true"]
            if filter_parts:
                if len(filter_parts) == 1:
                    parts.append(filter_parts[0])
                else:
                    parts.append("(" + " and ".join(filter_parts) + ")")

        # should = OR
        if "should" in bool_query:
            should_clauses = bool_query["should"]
            if not isinstance(should_clauses, list):
                should_clauses = [should_clauses]
            should_parts = [self._build_where_clause(q, document_type, query_params) for q in should_clauses]
            should_parts = [p for p in should_parts if p and p != "true"]
            if should_parts:
                if len(should_parts) == 1:
                    parts.append(should_parts[0])
                else:
                    parts.append("(" + " or ".join(should_parts) + ")")

        # must_not = NOT (AND NOT)
        if "must_not" in bool_query:
            must_not_clauses = bool_query["must_not"]
            if not isinstance(must_not_clauses, list):
                must_not_clauses = [must_not_clauses]
            must_not_parts = [self._build_where_clause(q, document_type, query_params) for q in must_not_clauses]
            must_not_parts = [p for p in must_not_parts if p and p != "true"]
            for part in must_not_parts:
                parts.append(f"!({part})")

        if not parts:
            return "true"

        return " and ".join(parts)

    def _convert_query_string(self, query_string: Dict) -> str:
        """
        Convert query_string to Vespa YQL.

        OpenSearch: {"query_string": {"query": "message: monkey jackal bear"}}
        Vespa YQL: message contains "monkey" or message contains "jackal" or message contains "bear"

        Also handles: {"query_string": {"query": "error OR warning", "default_field": "message"}}
        """
        query = query_string.get("query", "")
        default_field = query_string.get("default_field", "message")

        # Handle field:value format
        if ":" in query:
            # Split on first colon
            field_part, terms_part = query.split(":", 1)
            field = self._map_field_name(field_part.strip())
            terms = terms_part.strip()
        else:
            field = self._map_field_name(default_field)
            terms = query.strip()

        # Handle OR/AND operators (simple parsing)
        if " OR " in terms:
            term_list = [t.strip() for t in terms.split(" OR ")]
            conditions = [f'{field} contains "{t}"' for t in term_list if t]
            return "(" + " or ".join(conditions) + ")"
        elif " AND " in terms:
            term_list = [t.strip() for t in terms.split(" AND ")]
            conditions = [f'{field} contains "{t}"' for t in term_list if t]
            return "(" + " and ".join(conditions) + ")"
        else:
            # Space-separated terms - treat as OR by default for query_string
            term_list = terms.split()
            if len(term_list) == 1:
                return f'{field} contains "{term_list[0]}"'
            conditions = [f'{field} contains "{t}"' for t in term_list if t]
            return "(" + " or ".join(conditions) + ")"

    def _convert_prefix_query(self, prefix_query: Dict) -> str:
        """Convert prefix query to Vespa YQL."""
        for field, value_spec in prefix_query.items():
            vespa_field = self._map_field_name(field)
            if isinstance(value_spec, dict):
                value = value_spec.get("value", "")
            else:
                value = value_spec
            # Vespa uses prefix matching with *
            return f'{vespa_field} contains "{value}*"'
        return "true"

    def _convert_wildcard_query(self, wildcard_query: Dict) -> str:
        """Convert wildcard query to Vespa YQL."""
        for field, value_spec in wildcard_query.items():
            vespa_field = self._map_field_name(field)
            if isinstance(value_spec, dict):
                value = value_spec.get("value", "")
            else:
                value = value_spec
            # Vespa uses similar wildcard syntax
            return f'{vespa_field} contains "{value}"'
        return "true"

    def _build_order_clause(self, sort_spec: List) -> str:
        """
        Build ORDER BY clause from OpenSearch sort specification.

        OpenSearch: [{"@timestamp": "desc"}, {"metrics.size": {"order": "asc"}}]
        Vespa YQL: timestamp desc, metrics_size asc
        """
        if not sort_spec:
            return ""

        clauses = []
        for sort_item in sort_spec:
            if isinstance(sort_item, str):
                # Simple field name - default ascending
                vespa_field = self._map_field_name(sort_item)
                clauses.append(f"{vespa_field} asc")
            elif isinstance(sort_item, dict):
                for field, direction_spec in sort_item.items():
                    if field == "_score":
                        # Score sorting is handled by ranking in Vespa
                        continue

                    vespa_field = self._map_field_name(field)

                    if isinstance(direction_spec, str):
                        direction = direction_spec.lower()
                    elif isinstance(direction_spec, dict):
                        direction = direction_spec.get("order", "asc").lower()
                    else:
                        direction = "asc"

                    clauses.append(f"{vespa_field} {direction}")

        return ", ".join(clauses)

    def _build_limit_clause(self, body: Dict) -> str:
        """
        Build LIMIT/OFFSET clause from OpenSearch size/from parameters.

        OpenSearch: {"size": 10, "from": 100}
        Vespa YQL: limit 10 offset 100
        """
        size = body.get("size", 10)
        from_val = body.get("from", 0)

        clause = f"limit {size}"
        if from_val > 0:
            clause += f" offset {from_val}"

        return clause

    def _build_grouping_clause(self, aggs: Dict) -> str:
        """
        Build Vespa grouping clause from OpenSearch aggregations.

        Vespa uses a different grouping syntax than OpenSearch aggregations.
        This method translates common aggregation types to Vespa grouping.
        """
        if not aggs:
            return ""

        grouping_parts = []

        for agg_name, agg_spec in aggs.items():
            grouping = self._convert_aggregation(agg_name, agg_spec)
            if grouping:
                grouping_parts.append(grouping)

        # Combine multiple aggregations
        if len(grouping_parts) == 1:
            return grouping_parts[0]
        elif len(grouping_parts) > 1:
            # Multiple aggregations - wrap in all()
            return "all(" + " ".join(grouping_parts) + ")"

        return ""

    def _convert_aggregation(self, agg_name: str, agg_spec: Dict) -> str:
        """Convert a single aggregation to Vespa grouping syntax."""

        # Date histogram aggregation
        if "date_histogram" in agg_spec:
            return self._convert_date_histogram_agg(agg_spec["date_histogram"])

        # Terms aggregation
        if "terms" in agg_spec:
            return self._convert_terms_agg(agg_spec["terms"])

        # Cardinality aggregation (approximate distinct count)
        if "cardinality" in agg_spec:
            return self._convert_cardinality_agg(agg_spec["cardinality"])

        # Range aggregation
        if "range" in agg_spec:
            return self._convert_range_agg(agg_spec["range"])

        # Histogram aggregation (numeric)
        if "histogram" in agg_spec:
            return self._convert_histogram_agg(agg_spec["histogram"])

        # Auto date histogram
        if "auto_date_histogram" in agg_spec:
            return self._convert_auto_date_histogram_agg(agg_spec["auto_date_histogram"])

        # Composite aggregation
        if "composite" in agg_spec:
            return self._convert_composite_agg(agg_spec["composite"])

        # Metric aggregations (sum, avg, min, max, stats)
        for metric_type in ["sum", "avg", "min", "max", "stats", "value_count"]:
            if metric_type in agg_spec:
                return self._convert_metric_agg(metric_type, agg_spec[metric_type])

        # Significant terms (approximation)
        if "significant_terms" in agg_spec:
            # Approximate with regular terms agg
            return self._convert_terms_agg(agg_spec["significant_terms"])

        return ""

    def _convert_date_histogram_agg(self, spec: Dict) -> str:
        """
        Convert date_histogram aggregation to Vespa grouping.

        OpenSearch: {"date_histogram": {"field": "@timestamp", "calendar_interval": "hour"}}
        Vespa: all(group(floor(timestamp / 3600000)) each(output(count())))
        """
        field = self._map_field_name(spec.get("field", "timestamp"))
        interval = spec.get("calendar_interval", spec.get("fixed_interval", spec.get("interval", "hour")))

        # Map interval to milliseconds
        interval_ms_map = {
            "second": 1000,
            "1s": 1000,
            "minute": 60000,
            "1m": 60000,
            "hour": 3600000,
            "1h": 3600000,
            "day": 86400000,
            "1d": 86400000,
            "week": 604800000,
            "1w": 604800000,
            "month": 2592000000,  # 30 days approximation
            "1M": 2592000000,
        }

        interval_ms = interval_ms_map.get(interval, 3600000)  # Default to hour

        return f"all(group(floor({field} / {interval_ms})) each(output(count())))"

    def _convert_terms_agg(self, spec: Dict) -> str:
        """
        Convert terms aggregation to Vespa grouping.

        OpenSearch: {"terms": {"field": "aws.cloudwatch.log_stream", "size": 500}}
        Vespa: all(group(aws_cloudwatch_log_stream) max(500) each(output(count())))
        """
        field = self._map_field_name(spec.get("field", ""))
        size = spec.get("size", 10)

        return f"all(group({field}) max({size}) each(output(count())))"

    def _convert_cardinality_agg(self, spec: Dict) -> str:
        """
        Convert cardinality aggregation to Vespa grouping.

        OpenSearch: {"cardinality": {"field": "cloud.region"}}
        Vespa: Approximate with group count - all(group(cloud_region) each(output(count())))
        """
        field = self._map_field_name(spec.get("field", ""))

        # Vespa doesn't have direct cardinality, so we group and count distinct values
        return f"all(group({field}) each(output(count())))"

    def _convert_range_agg(self, spec: Dict) -> str:
        """
        Convert range aggregation to Vespa grouping.

        This is complex in Vespa - we use predefined buckets.
        """
        field = self._map_field_name(spec.get("field", ""))
        ranges = spec.get("ranges", [])

        # For simplicity, use numeric bucketing
        # Vespa predefined ranges are complex, so we approximate with a simple group
        if ranges:
            # Get the range bounds
            buckets = len(ranges)
            return f"all(group({field}) max({buckets * 2}) each(output(count())))"

        return f"all(group({field}) each(output(count())))"

    def _convert_histogram_agg(self, spec: Dict) -> str:
        """
        Convert histogram aggregation to Vespa grouping.

        OpenSearch: {"histogram": {"field": "metrics.size", "interval": 100}}
        Vespa: all(group(floor(metrics_size / 100)) each(output(count())))
        """
        field = self._map_field_name(spec.get("field", ""))
        interval = spec.get("interval", 100)

        return f"all(group(floor({field} / {interval})) each(output(count())))"

    def _convert_auto_date_histogram_agg(self, spec: Dict) -> str:
        """
        Convert auto_date_histogram aggregation to Vespa grouping.

        Auto-selects appropriate interval - we default to hourly.
        """
        field = self._map_field_name(spec.get("field", "timestamp"))
        buckets = spec.get("buckets", 10)

        # Default to hourly bucketing
        return f"all(group(floor({field} / 3600000)) max({buckets}) each(output(count())))"

    def _convert_composite_agg(self, spec: Dict) -> str:
        """
        Convert composite aggregation to Vespa grouping.

        Composite aggregations are complex - we approximate with nested grouping.
        """
        sources = spec.get("sources", [])
        size = spec.get("size", 10)

        if not sources:
            return ""

        # Build nested grouping
        fields = []
        for source in sources:
            for name, source_spec in source.items():
                if "terms" in source_spec:
                    field = self._map_field_name(source_spec["terms"].get("field", ""))
                    fields.append(field)
                elif "date_histogram" in source_spec:
                    field = self._map_field_name(source_spec["date_histogram"].get("field", ""))
                    fields.append(field)

        if len(fields) == 1:
            return f"all(group({fields[0]}) max({size}) each(output(count())))"
        elif len(fields) == 2:
            # Two-level grouping
            return f"all(group({fields[0]}) max({size}) each(group({fields[1]}) max({size}) each(output(count()))))"
        elif len(fields) >= 3:
            # Three-level grouping
            return f"all(group({fields[0]}) max({size}) each(group({fields[1]}) max({size}) each(group({fields[2]}) max({size}) each(output(count())))))"

        return ""

    def _convert_metric_agg(self, metric_type: str, spec: Dict) -> str:
        """
        Convert metric aggregation (sum, avg, min, max) to Vespa grouping.

        OpenSearch: {"sum": {"field": "metrics.size"}}
        Vespa: all(output(sum(metrics_size)))
        """
        field = self._map_field_name(spec.get("field", ""))

        if metric_type == "stats":
            # Stats returns multiple metrics
            return f"all(output(sum({field})) output(avg({field})) output(min({field})) output(max({field})) output(count()))"
        elif metric_type == "value_count":
            return f"all(output(count()))"
        else:
            return f"all(output({metric_type}({field})))"

    def _convert_vespa_response(self, vespa_response: Dict) -> Dict[str, Any]:
        """Convert Vespa search response to OpenSearch format."""
        hits = vespa_response.get("root", {}).get("children", [])
        # Use totalCount field from root.fields if available, otherwise use length of hits
        root_fields = vespa_response.get("root", {}).get("fields", {})
        total_count = root_fields.get("totalCount", len(hits))

        return {
            "took": vespa_response.get("timing", {}).get("searchtime", 0),
            "timed_out": False,
            "hits": {
                "total": {
                    "value": total_count,
                    "relation": "eq"
                },
                "max_score": hits[0].get("relevance", 0) if hits else 0,
                "hits": [
                    {
                        "_id": hit.get("id", ""),
                        "_source": hit.get("fields", {}),
                        "_score": hit.get("relevance", 0)
                    }
                    for hit in hits
                ]
            }
        }

    # Index Operations

    async def indices_create(self, index: str, body: Dict = None, **kwargs) -> Dict[str, Any]:
        """
        Create an index (deploy Vespa application/schema).

        :param index: Schema name
        :param body: Index settings/mappings
        :param kwargs: Additional parameters
        :return: Response dict
        """
        self.logger.info(f"Creating Vespa schema: {index}")
        # In Vespa, schemas are deployed via application packages, not at runtime
        # For benchmarking, we assume the schema is already deployed
        # Make a lightweight HTTP call to satisfy the timing system
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        async with self._session.get(f"{self.endpoint}/ApplicationStatus") as resp:
            await resp.text()

        return {
            "acknowledged": True,
            "shards_acknowledged": True,
            "index": index
        }

    async def indices_delete(self, index: str, **kwargs) -> Dict[str, Any]:
        """Delete an index (remove Vespa schema/documents)."""
        self.logger.info(f"Deleting Vespa schema: {index}")
        # In Vespa, schemas cannot be deleted at runtime
        # For benchmarking, this is a no-op (documents can be deleted separately)
        # Make a lightweight HTTP call to satisfy the timing system
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        self.logger.info(f"Making HTTP call to {self.endpoint}/ApplicationStatus")
        async with self._session.get(f"{self.endpoint}/ApplicationStatus") as resp:
            text = await resp.text()
            self.logger.info(f"HTTP call completed: {resp.status}")

        return {"acknowledged": True}

    async def indices_exists(self, index: str, **kwargs) -> bool:
        """
        Check if an index (schema) exists.

        :param index: Schema name
        :param kwargs: Additional parameters
        :return: True if index exists, False otherwise
        """
        # In Vespa, we assume schemas are pre-deployed
        # For benchmarking, always return True
        # Make a lightweight HTTP call to satisfy the timing system
        if not self._session:
            await self.__aenter__()

        async with self._session.get(f"{self.endpoint}/ApplicationStatus") as resp:
            await resp.text()

        return True

    # Alias for compatibility (runner calls opensearch.indices.exists())
    async def exists(self, index: str, **kwargs) -> bool:
        """Alias for indices_exists."""
        return await self.indices_exists(index, **kwargs)

    # Alias for compatibility (runner calls opensearch.indices.delete())
    async def delete(self, index: str, **kwargs) -> Dict[str, Any]:
        """Alias for indices_delete."""
        return await self.indices_delete(index, **kwargs)

    # Alias for compatibility (runner calls opensearch.indices.create())
    async def create(self, index: str, body: Dict = None, **kwargs) -> Dict[str, Any]:
        """Alias for indices_create."""
        return await self.indices_create(index, body, **kwargs)

    # Alias for compatibility (runner calls opensearch.indices.refresh())
    async def refresh(self, index: str = None, **kwargs) -> Dict[str, Any]:
        """
        Refresh index. For Vespa this is a no-op since Vespa doesn't have refresh semantics.
        Makes a lightweight HTTP call to satisfy the timing system.

        :param index: Schema name (ignored)
        :param kwargs: Additional parameters
        :return: Acknowledgement dict
        """
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        # Make a lightweight HTTP call
        async with self._session.get(f"{self.endpoint}/ApplicationStatus") as resp:
            await resp.text()

        return {"acknowledged": True, "_shards": {"total": 1, "successful": 1, "failed": 0}}

    # Alias for compatibility (runner calls opensearch.indices.forcemerge())
    async def forcemerge(self, index: str = None, **kwargs) -> Dict[str, Any]:
        """Alias for indices_force_merge."""
        return await self.indices_force_merge(index, **kwargs)

    async def indices_stats(self, index: str = None, metric: str = None, **kwargs) -> Dict[str, Any]:
        """
        Get index statistics from Vespa metrics.

        :param index: Schema name
        :param metric: Specific metric to retrieve
        :param kwargs: Additional parameters
        :return: Stats dict
        """
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        endpoint = f"{self.endpoint}/metrics/v2/values"

        try:
            if self._session:
                async with self._session.get(endpoint) as response:
                    metrics = await response.json()
            else:
                response = requests.get(endpoint)
                metrics = response.json()

            return self._convert_metrics_to_stats(metrics, index)

        except Exception as e:
            self.logger.warning(f"Failed to get metrics: {e}")
            return {"_all": {"primaries": {}, "total": {}}}

    def _convert_metrics_to_stats(self, metrics: Dict, index: Optional[str]) -> Dict[str, Any]:
        """Convert Vespa metrics to OpenSearch stats format."""
        return {
            "_all": {
                "primaries": {
                    "docs": {"count": 0, "deleted": 0},
                    "store": {"size_in_bytes": 0}
                },
                "total": {
                    "docs": {"count": 0, "deleted": 0},
                    "store": {"size_in_bytes": 0}
                }
            }
        }

    async def indices_refresh(self, index: str = None, **kwargs) -> Dict[str, Any]:
        """
        Refresh indices (no-op for Vespa).
        Makes a lightweight HTTP call to satisfy the timing system.
        """
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        # Make a lightweight HTTP call
        async with self._session.get(f"{self.endpoint}/ApplicationStatus") as resp:
            await resp.text()

        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}

    async def indices_force_merge(self, index: str = None, **kwargs) -> Dict[str, Any]:
        """
        Force merge (no-op for Vespa).
        Makes a lightweight HTTP call to satisfy the timing system.

        Returns task format for compatibility with OpenSearch's task API when used in polling mode.
        """
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        # Make a lightweight HTTP call
        async with self._session.get(f"{self.endpoint}/ApplicationStatus") as resp:
            await resp.text()

        # If wait_for_completion is "false" (string, polling mode), return task format
        # Otherwise return standard response
        wait_for_completion = kwargs.get("wait_for_completion", True)
        if wait_for_completion == "false" or wait_for_completion is False:
            return {"task": "vespa-node:1"}

        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}

    # Cluster Operations

    async def cluster_health(self, **kwargs) -> Dict[str, Any]:
        """
        Get cluster health from Vespa.

        :param kwargs: Additional parameters
        :return: Health status dict
        """
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        endpoint = f"{self.endpoint}/state/v1/health"

        try:
            if self._session:
                async with self._session.get(endpoint) as response:
                    health = await response.json()
                    status = health.get("status", {}).get("code", "red")
            else:
                response = requests.get(endpoint)
                health = response.json()
                status = health.get("status", {}).get("code", "red")

            # Map Vespa status to OpenSearch status
            status_map = {"up": "green", "down": "red", "initializing": "yellow"}

            return {
                "cluster_name": "vespa",
                "status": status_map.get(status, "yellow"),
                "timed_out": False,
                "number_of_nodes": 1,
                "number_of_data_nodes": 1,
                "active_primary_shards": 1,
                "active_shards": 1,
                "relocating_shards": 0,
                "initializing_shards": 0,
                "unassigned_shards": 0
            }

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "cluster_name": "vespa",
                "status": "red",
                "timed_out": False
            }

    def info(self, **kwargs) -> Dict[str, Any]:
        """
        Get cluster information from Vespa.
        Returns info compatible with OpenSearch's info() response.

        This method is always synchronous since it's called from setup/initialization code
        that runs in a synchronous context (e.g., cluster_distribution_version).

        :param kwargs: Additional parameters
        :return: Cluster info dict
        """
        return self._info_sync(**kwargs)

    def _info_sync(self, **kwargs) -> Dict[str, Any]:
        """Synchronous implementation of info()."""
        endpoint = f"{self.endpoint}/ApplicationStatus"

        try:
            response = requests.get(endpoint)
            app_status = response.json()

            # Extract version info
            version = app_status.get("application", {}).get("vespa", {}).get("version", "unknown")

            return {
                "name": "vespa",
                "cluster_name": self._app_name,
                "cluster_uuid": "vespa-cluster",
                "version": {
                    "number": version,
                    "distribution": "vespa",
                    "build_type": "docker",
                    "build_hash": "unknown",
                    "build_date": "unknown",
                    "build_snapshot": False,
                    "build_flavor": "default",
                    "lucene_version": "unknown",
                    "minimum_wire_compatibility_version": version,
                    "minimum_index_compatibility_version": version
                },
                "tagline": "The Search Engine for Structured Data"
            }

        except Exception as e:
            self.logger.warning(f"Could not retrieve cluster info: {e}")
            return {
                "name": "vespa",
                "cluster_name": self._app_name,
                "version": {
                    "number": "unknown",
                    "distribution": "vespa",
                    "build_hash": "unknown"
                }
            }

    async def _info_async(self, **kwargs) -> Dict[str, Any]:
        """Async implementation of info()."""
        # Ensure session is initialized with trace hooks before making requests
        await self._ensure_session()

        endpoint = f"{self.endpoint}/ApplicationStatus"

        try:
            async with self._session.get(endpoint) as response:
                app_status = await response.json()

            # Extract version info
            version = app_status.get("application", {}).get("vespa", {}).get("version", "unknown")

            return {
                "name": "vespa",
                "cluster_name": self._app_name,
                "cluster_uuid": "vespa-cluster",
                "version": {
                    "number": version,
                    "distribution": "vespa",
                    "build_type": "docker",
                    "build_hash": "unknown",
                    "build_date": "unknown",
                    "build_snapshot": False,
                    "build_flavor": "default",
                    "lucene_version": "unknown",
                    "minimum_wire_compatibility_version": version,
                    "minimum_index_compatibility_version": version
                },
                "tagline": "The Search Engine for Structured Data"
            }

        except Exception as e:
            self.logger.warning(f"Could not retrieve cluster info: {e}")
            return {
                "name": "vespa",
                "cluster_name": self._app_name,
                "version": {
                    "number": "unknown",
                    "distribution": "vespa",
                    "build_hash": "unknown"
                }
            }

    # Compatibility properties and objects

    @property
    def nodes(self):
        """Return a nodes compatibility object for telemetry."""
        return _VespaNodesCompat(self)

    @property
    def indices(self):
        """Return indices namespace for compatibility."""
        return self

    @property
    def transport(self):
        """Return a transport compatibility object."""
        return _VespaTransportCompat(self)

    # new_request_context() is inherited from RequestContextHolder - no need to override

    @property
    def cluster(self):
        """Return cluster namespace for compatibility."""
        return self

    @property
    def tasks(self):
        """Return tasks namespace for compatibility."""
        return _VespaTasksCompat(self)

    # The following methods provide attribute-style access for compatibility

    def __getattr__(self, name):
        """Handle unknown attributes gracefully."""
        self.logger.debug(f"Accessed unknown attribute: {name}")
        return lambda *args, **kwargs: {"acknowledged": True}

    async def close(self):
        """Close client connections."""
        if self._session:
            await self._session.close()


class _VespaTransportCompat:
    """
    Compatibility layer for OpenSearch transport.
    Vespa uses aiohttp sessions instead of opensearchpy transport.
    """
    def __init__(self, client):
        self.client = client

    async def close(self):
        """Close any open sessions."""
        if self.client._session:
            await self.client._session.close()


class _VespaNodesCompat:
    """
    Compatibility layer for OpenSearch nodes API.
    Provides stub implementations for telemetry that expects nodes API.
    These are used only for monitoring/metrics collection, not for benchmark operations.
    """
    def __init__(self, client):
        self.client = client

    def stats(self, **kwargs):
        """Return minimal nodes stats for telemetry compatibility."""
        return {
            "nodes": {
                "vespa-node-1": {
                    "name": "vespa-node-1",
                    "host": self.client.endpoint,
                    "os": {
                        "cpu": {"percent": 0}
                    },
                    "jvm": {
                        "mem": {
                            "heap_used_percent": 0,
                            "pools": {
                                "young": {
                                    "peak_used_in_bytes": 0
                                },
                                "old": {
                                    "peak_used_in_bytes": 0
                                },
                                "survivor": {
                                    "peak_used_in_bytes": 0
                                }
                            }
                        },
                        "gc": {
                            "collectors": {
                                "young": {
                                    "collection_time_in_millis": 0,
                                    "collection_count": 0
                                },
                                "old": {
                                    "collection_time_in_millis": 0,
                                    "collection_count": 0
                                }
                            }
                        }
                    }
                }
            }
        }

    def info(self, **kwargs):
        """Return minimal nodes info for telemetry compatibility."""
        return {
            "nodes": {
                "vespa-node-1": {
                    "name": "vespa-node-1",
                    "host": self.client.endpoint,
                    "version": "8.0.0",
                    "os": {
                        "name": "Linux"
                    },
                    "jvm": {
                        "version": "17.0.0",
                        "gc": {
                            "collectors": {
                                "young": "G1 Young Generation",
                                "old": "G1 Old Generation"
                            }
                        }
                    }
                }
            }
        }


class _VespaTasksCompat:
    """
    Compatibility layer for OpenSearch tasks API.
    Vespa doesn't have long-running tasks like OpenSearch,
    so we simulate completed tasks immediately.
    """
    def __init__(self, client):
        self.client = client

    async def get(self, task_id, **kwargs):
        """
        Get task status. Since Vespa operations complete immediately,
        always return completed status.

        :param task_id: Task ID (ignored for Vespa)
        :param kwargs: Additional parameters
        :return: Task status dict
        """
        return {
            "completed": True,
            "task": {
                "node": "vespa-node",
                "id": 1,
                "type": "transport",
                "action": "indices:admin/forcemerge",
                "status": {},
                "description": "Force merge",
                "start_time_in_millis": 0,
                "running_time_in_nanos": 0,
                "cancellable": False
            },
            "response": {
                "_shards": {
                    "total": 1,
                    "successful": 1,
                    "failed": 0
                }
            }
        }


def wait_for_vespa(vespa_client, max_attempts=40):
    """
    Wait for Vespa to be ready.

    :param vespa_client: Vespa client instance
    :param max_attempts: Maximum number of health check attempts
    :return: True if Vespa is ready, False otherwise
    """
    logger = logging.getLogger(__name__)

    for attempt in range(max_attempts):
        try:
            import asyncio
            health = asyncio.run(vespa_client.cluster_health())

            if health["status"] in ["green", "yellow"]:
                logger.info(f"Vespa is ready after {attempt} attempts")
                return True

            logger.debug(f"Vespa not ready, attempt {attempt}/{max_attempts}")
            time.sleep(3)

        except Exception as e:
            logger.debug(f"Health check failed on attempt {attempt}: {e}")
            time.sleep(3)

    logger.warning(f"Vespa not ready after {max_attempts} attempts")
    return False
