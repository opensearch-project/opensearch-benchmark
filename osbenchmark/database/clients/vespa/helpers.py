# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Pure helper functions for Vespa translation.

This module contains all translation logic for converting between OpenSearch
and Vespa formats. No HTTP calls or session management — just pure functions
and constants.

Organized into sections:
- Constants (field mappings, allowed fields)
- Field/Document Translation
- Query Translation (DSL → YQL)
- Sort/Limit
- Aggregation Translation
- Response Conversion
- Utility
"""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

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


# =============================================================================
# Field/Document Translation
# =============================================================================

def map_field_name(os_field: str) -> str:
    """Map OpenSearch field name to Vespa field name.

    OpenSearch uses nested object paths (e.g., log.file.path) while
    Vespa prefers flat field names (e.g., log_file_path).
    """
    if os_field in FIELD_NAME_MAPPING:
        return FIELD_NAME_MAPPING[os_field]
    return os_field.replace(".", "_")


def is_leaf_value(value: Any) -> bool:
    """Check if a value is a leaf value (not a nested object to flatten).

    Some dict values are actual data (e.g., geo_point, date objects)
    rather than nested document structure.
    """
    if not isinstance(value, dict):
        return True

    keys = set(value.keys())

    if {"lat", "lon"}.issubset(keys):
        return True
    if {"type", "coordinates"}.issubset(keys):
        return True
    if keys == {"value"} or keys == {"values"}:
        return True

    if all(not isinstance(v, dict) for v in value.values()):
        data_keys = {"query", "analyzer", "fuzziness", "boost", "minimum_should_match",
                     "match", "prefix", "fuzzy", "wildcard", "regexp"}
        if data_keys.intersection(keys):
            return True

    return False


def date_to_epoch(date_value) -> int:
    """Convert a date value to epoch milliseconds.

    Handles ISO 8601 strings, simple date strings, and numeric values.
    """
    if isinstance(date_value, (int, float)):
        if date_value < 1e12:
            return int(date_value * 1000)
        return int(date_value)

    if not isinstance(date_value, str):
        return 0

    try:
        date_str = date_value.replace("Z", "+00:00")
        if "." in date_str:
            parts = date_str.split(".")
            if len(parts) == 2:
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

    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"]:
        try:
            dt = datetime.strptime(date_value, fmt)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue

    logger.warning("Could not parse date value: %s", date_value)
    return 0


def transform_document_for_vespa(doc: Dict, app_name: str = "") -> Dict:
    """Transform an OpenSearch document to Vespa format.

    Flattens nested fields, converts timestamps, maps field names,
    and filters fields for big5 workload compatibility.
    """
    vespa_doc = {}

    def flatten(obj: Any, prefix: str = "") -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}_{key}" if prefix else key

                if isinstance(value, dict) and not is_leaf_value(value):
                    flatten(value, new_key)
                else:
                    if new_key in FIELD_NAME_MAPPING:
                        mapped_key = FIELD_NAME_MAPPING[new_key]
                    else:
                        original_path = new_key.replace("_", ".")
                        if original_path in FIELD_NAME_MAPPING:
                            mapped_key = FIELD_NAME_MAPPING[original_path]
                        else:
                            mapped_key = new_key.replace(".", "_")

                    if mapped_key == "timestamp" and isinstance(value, str):
                        value = date_to_epoch(value)
                    elif mapped_key == "event_ingested" and isinstance(value, str):
                        value = date_to_epoch(value)
                    elif isinstance(value, list):
                        value = ",".join(str(v) for v in value)

                    vespa_doc[mapped_key] = value

    if "@timestamp" in doc:
        vespa_doc["timestamp"] = date_to_epoch(doc["@timestamp"])
        doc = {k: v for k, v in doc.items() if k != "@timestamp"}

    flatten(doc)

    if app_name == "big5":
        vespa_doc = {k: v for k, v in vespa_doc.items() if k in BIG5_ALLOWED_FIELDS}

    return vespa_doc


def wrap_fields_with_assign(fields: Dict) -> Dict:
    """Wrap field values with Vespa's assign operation for PUT requests."""
    return {field: {"assign": value} for field, value in fields.items()}


def parse_bulk_body(body) -> List[Dict]:
    """Parse bulk body into list of documents.

    Handles bytes input (OSB standard), string input (newline-delimited JSON),
    and list input (alternating [action, doc, ...] from vector datasets).
    """
    if isinstance(body, (list, tuple)):
        body_list = list(body)
        if len(body_list) >= 2 and isinstance(body_list[0], dict):
            first_item = body_list[0]
            if "index" in first_item and isinstance(first_item.get("index"), dict):
                documents = []
                for i in range(0, len(body_list) - 1, 2):
                    action = body_list[i]
                    doc_body = body_list[i + 1]
                    doc_id = action.get("index", {}).get("_id", f"doc_{len(documents)}")
                    documents.append({"_id": doc_id, "_source": doc_body})
                return documents
        return body_list

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
                doc_id = action.get("index", {}).get("_id")
                if not doc_id:
                    doc_id = str(uuid.uuid4())
                documents.append({"_id": doc_id, "_source": doc_body})
                i += 2
            else:
                i += 1
        except json.JSONDecodeError:
            logger.warning("Failed to parse bulk line: %s", lines[i])
            i += 1

    return documents


# =============================================================================
# Query Translation — DSL → YQL
# =============================================================================

def convert_to_yql(body: Optional[Dict], document_type: str) -> Tuple[str, Dict]:
    """Convert OpenSearch query DSL to Vespa YQL.

    Returns (yql_query, query_params) where query_params contains additional
    parameters like input.query(query_vector) for KNN search.

    Handles search_after by converting to range filter in WHERE clause
    (keyset pagination equivalent).
    """
    query_params = {}

    if not body:
        return f"select * from {document_type} where true", query_params

    where_clause = build_where_clause(body.get("query", {}), document_type, query_params)
    sort_spec = body.get("sort", [])
    order_clause = build_order_clause(sort_spec)
    limit_clause = build_limit_clause(body)

    # Convert search_after to range filter (keyset pagination)
    search_after = body.get("search_after")
    if search_after and sort_spec:
        sa_conditions = _build_search_after_filter(search_after, sort_spec)
        if sa_conditions:
            if where_clause == "true":
                where_clause = sa_conditions
            else:
                where_clause = f"{where_clause} and {sa_conditions}"

    yql = f"select * from {document_type} where {where_clause}"

    if order_clause:
        yql += f" order by {order_clause}"

    if limit_clause:
        yql += f" {limit_clause}"

    grouping_clause = build_grouping_clause(body.get("aggs", body.get("aggregations", {})))
    if grouping_clause:
        yql += f" | {grouping_clause}"

    # Pass request timeout to Vespa query timeout
    request_timeout = body.get("request-timeout")
    if request_timeout:
        query_params["timeout"] = f"{request_timeout}s"

    return yql, query_params


def _build_search_after_filter(search_after: List, sort_spec: List) -> str:
    """Convert search_after values + sort spec to a range filter.

    For desc sort with search_after value V: timestamp < V (next page is lower)
    For asc sort with search_after value V: timestamp > V (next page is higher)
    """
    conditions = []
    for i, sa_value in enumerate(search_after):
        if i >= len(sort_spec):
            break
        sort_item = sort_spec[i]
        if not isinstance(sort_item, dict):
            continue
        for field, direction_spec in sort_item.items():
            if field == "_score":
                continue
            vespa_field = map_field_name(field)
            is_date = field in ("@timestamp", "timestamp", "event.ingested")

            if isinstance(direction_spec, str):
                direction = direction_spec.lower()
            elif isinstance(direction_spec, dict):
                direction = direction_spec.get("order", "asc").lower()
            else:
                direction = "asc"

            if is_date:
                sa_value = date_to_epoch(sa_value)

            if direction == "desc":
                conditions.append(f"{vespa_field} < {sa_value}")
            else:
                conditions.append(f"{vespa_field} > {sa_value}")

    return " and ".join(conditions) if conditions else ""


def build_where_clause(query: Dict, document_type: str, query_params: Dict) -> str:
    """Build WHERE clause from OpenSearch query DSL.

    Handles: match_all, match, term, range, bool, query_string, knn,
    prefix, wildcard, exists.
    """
    if not query:
        return "true"

    if "match_all" in query:
        query_params["ranking"] = "unranked"
        return "true"

    if "knn" in query:
        return convert_knn_query(query["knn"], query_params)

    if "term" in query:
        return convert_term_query(query["term"])

    if "terms" in query:
        return convert_terms_query(query["terms"])

    if "range" in query:
        return convert_range_query(query["range"])

    if "match" in query:
        return convert_match_query(query["match"])

    if "bool" in query:
        return convert_bool_query(query["bool"], document_type, query_params)

    if "query_string" in query:
        return convert_query_string(query["query_string"])

    if "prefix" in query:
        return convert_prefix_query(query["prefix"])

    if "wildcard" in query:
        return convert_wildcard_query(query["wildcard"])

    if "exists" in query:
        field = map_field_name(query["exists"].get("field", ""))
        return f"{field} != null"

    return "true"


def convert_knn_query(knn_config: Dict, query_params: Dict) -> str:
    """Convert KNN/vector search query to Vespa YQL nearestNeighbor.

    OpenSearch format: {"knn": {"field_name": {"vector": [...], "k": 100}}}
    The field name is used as a key, with vector/k nested inside.
    """
    # OpenSearch nests vector/k under the field name as key
    field = None
    vector = []
    k = 10

    for key, value in knn_config.items():
        if isinstance(value, dict):
            field = map_field_name(key)
            vector = value.get("vector", [])
            k = value.get("k", 10)
            break

    # Fallback for flat format {"field": "x", "vector": [...], "k": 10}
    if field is None:
        field = map_field_name(knn_config.get("field", "vector"))
        vector = knn_config.get("vector", [])
        k = knn_config.get("k", 10)

    vector_str = "[" + ",".join(str(v) for v in vector) + "]"
    query_params["input.query(query_vector)"] = vector_str
    query_params["ranking"] = "vector-similarity"

    return f"{{targetHits:{k}}}nearestNeighbor({field}, query_vector)"


def convert_term_query(term_query: Dict) -> str:
    """Convert term query to Vespa YQL.

    {"term": {"field": {"value": "val"}}} → field contains "val"
    """
    for field, value_spec in term_query.items():
        vespa_field = map_field_name(field)
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


def convert_terms_query(terms_query: Dict) -> str:
    """Convert terms query (multiple values) to Vespa YQL.

    {"terms": {"field": ["a", "b"]}} → (field contains "a" or field contains "b")
    """
    for field, values in terms_query.items():
        if field == "boost":
            continue
        vespa_field = map_field_name(field)
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


def convert_range_query(range_query: Dict) -> str:
    """Convert range query to Vespa YQL.

    {"range": {"@timestamp": {"gte": "2023-01-01", "lt": "2023-01-03"}}}
    → timestamp >= 1672531200000 and timestamp < 1672704000000
    """
    conditions = []
    for field, range_spec in range_query.items():
        vespa_field = map_field_name(field)
        is_date_field = field in ("@timestamp", "event.ingested", "timestamp")

        for op, value in range_spec.items():
            if op in ("format", "time_zone"):
                continue

            if is_date_field:
                value = date_to_epoch(value)

            if op == "gte":
                conditions.append(f"{vespa_field} >= {value}")
            elif op == "gt":
                conditions.append(f"{vespa_field} > {value}")
            elif op == "lte":
                conditions.append(f"{vespa_field} <= {value}")
            elif op == "lt":
                conditions.append(f"{vespa_field} < {value}")

    return " and ".join(conditions) if conditions else "true"


def convert_match_query(match_query: Dict) -> str:
    """Convert match query to Vespa YQL.

    {"match": {"message": "error failed"}} → message contains "error failed"
    """
    for field, value_spec in match_query.items():
        vespa_field = map_field_name(field)
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


def convert_bool_query(bool_query: Dict, document_type: str, query_params: Dict) -> str:
    """Convert bool query to Vespa YQL.

    {"bool": {"must": [...], "should": [...], "filter": [...], "must_not": [...]}}
    → (cond1 and cond2) and (cond3 or cond4) and !(cond5)
    """
    parts = []

    if "must" in bool_query:
        must_clauses = bool_query["must"]
        if not isinstance(must_clauses, list):
            must_clauses = [must_clauses]
        must_parts = [build_where_clause(q, document_type, query_params) for q in must_clauses]
        must_parts = [p for p in must_parts if p and p != "true"]
        if must_parts:
            if len(must_parts) == 1:
                parts.append(must_parts[0])
            else:
                parts.append("(" + " and ".join(must_parts) + ")")

    if "filter" in bool_query:
        filter_clauses = bool_query["filter"]
        if not isinstance(filter_clauses, list):
            filter_clauses = [filter_clauses]
        filter_parts = [build_where_clause(q, document_type, query_params) for q in filter_clauses]
        filter_parts = [p for p in filter_parts if p and p != "true"]
        if filter_parts:
            if len(filter_parts) == 1:
                parts.append(filter_parts[0])
            else:
                parts.append("(" + " and ".join(filter_parts) + ")")

    if "should" in bool_query:
        should_clauses = bool_query["should"]
        if not isinstance(should_clauses, list):
            should_clauses = [should_clauses]
        should_parts = [build_where_clause(q, document_type, query_params) for q in should_clauses]
        should_parts = [p for p in should_parts if p and p != "true"]
        if should_parts:
            if len(should_parts) == 1:
                parts.append(should_parts[0])
            else:
                parts.append("(" + " or ".join(should_parts) + ")")

    if "must_not" in bool_query:
        must_not_clauses = bool_query["must_not"]
        if not isinstance(must_not_clauses, list):
            must_not_clauses = [must_not_clauses]
        must_not_parts = [build_where_clause(q, document_type, query_params) for q in must_not_clauses]
        must_not_parts = [p for p in must_not_parts if p and p != "true"]
        for part in must_not_parts:
            parts.append(f"!({part})")

    if not parts:
        return "true"

    return " and ".join(parts)


def convert_query_string(query_string: Dict) -> str:
    """Convert query_string to Vespa YQL.

    Handles field:value format and OR/AND operators.
    """
    query = query_string.get("query", "")
    default_field = query_string.get("default_field", "message")

    if ":" in query:
        field_part, terms_part = query.split(":", 1)
        field = map_field_name(field_part.strip())
        terms = terms_part.strip()
    else:
        field = map_field_name(default_field)
        terms = query.strip()

    if " OR " in terms:
        term_list = [t.strip() for t in terms.split(" OR ")]
        conditions = [f'{field} contains "{t}"' for t in term_list if t]
        return "(" + " or ".join(conditions) + ")"
    elif " AND " in terms:
        term_list = [t.strip() for t in terms.split(" AND ")]
        conditions = [f'{field} contains "{t}"' for t in term_list if t]
        return "(" + " and ".join(conditions) + ")"
    else:
        term_list = terms.split()
        if len(term_list) == 1:
            return f'{field} contains "{term_list[0]}"'
        conditions = [f'{field} contains "{t}"' for t in term_list if t]
        return "(" + " or ".join(conditions) + ")"


def convert_prefix_query(prefix_query: Dict) -> str:
    """Convert prefix query to Vespa YQL."""
    for field, value_spec in prefix_query.items():
        vespa_field = map_field_name(field)
        if isinstance(value_spec, dict):
            value = value_spec.get("value", "")
        else:
            value = value_spec
        return f'{vespa_field} contains "{value}*"'
    return "true"


def convert_wildcard_query(wildcard_query: Dict) -> str:
    """Convert wildcard query to Vespa YQL."""
    for field, value_spec in wildcard_query.items():
        vespa_field = map_field_name(field)
        if isinstance(value_spec, dict):
            value = value_spec.get("value", "")
        else:
            value = value_spec
        return f'{vespa_field} contains "{value}"'
    return "true"


# =============================================================================
# Sort/Limit
# =============================================================================

def build_order_clause(sort_spec: List) -> str:
    """Build ORDER BY clause from OpenSearch sort specification.

    [{"@timestamp": "desc"}, {"metrics.size": {"order": "asc"}}]
    → timestamp desc, metrics_size asc
    """
    if not sort_spec:
        return ""

    clauses = []
    for sort_item in sort_spec:
        if isinstance(sort_item, str):
            vespa_field = map_field_name(sort_item)
            clauses.append(f"{vespa_field} asc")
        elif isinstance(sort_item, dict):
            for field, direction_spec in sort_item.items():
                if field == "_score":
                    continue

                vespa_field = map_field_name(field)

                if isinstance(direction_spec, str):
                    direction = direction_spec.lower()
                elif isinstance(direction_spec, dict):
                    direction = direction_spec.get("order", "asc").lower()
                else:
                    direction = "asc"

                clauses.append(f"{vespa_field} {direction}")

    return ", ".join(clauses)


def build_limit_clause(body: Dict) -> str:
    """Build LIMIT/OFFSET clause from OpenSearch size/from parameters.

    {"size": 10, "from": 100} → limit 10 offset 100
    """
    size = body.get("size", 10)
    from_val = body.get("from", 0)

    clause = f"limit {size}"
    if from_val > 0:
        clause += f" offset {from_val}"

    return clause


# =============================================================================
# Aggregation Translation
# =============================================================================

def build_grouping_clause(aggs: Dict) -> str:
    """Build Vespa grouping clause from OpenSearch aggregations.

    Aggregation converters return two formats:
    - Metric aggs: "output(...)" — needs all() wrapper at top level
    - Bucket aggs: "group(...) each(...)" — needs all() wrapper at top level
    """
    if not aggs:
        return ""

    parts = []
    for agg_name, agg_spec in aggs.items():
        result = convert_aggregation(agg_name, agg_spec)
        if result:
            parts.append(result)

    if not parts:
        return ""

    # Separate metrics (output(...)) from buckets (group(...))
    metric_parts = [p for p in parts if p.startswith("output(")]
    bucket_parts = [p for p in parts if not p.startswith("output(")]

    wrapped = []
    if metric_parts:
        wrapped.append("all(" + " ".join(metric_parts) + ")")
    for bp in bucket_parts:
        wrapped.append(f"all({bp})")

    if len(wrapped) == 1:
        return wrapped[0]
    return " ".join(wrapped)


def convert_aggregation(agg_name: str, agg_spec: Dict) -> str:
    """Convert a single aggregation to Vespa grouping syntax.

    Handles nested sub-aggregations by recursing into agg_spec["aggs"].
    Bucket converters receive nested_content to embed in their each() clause.
    Metric converters return output(...) format.
    """
    # Extract nested sub-aggregations
    nested_aggs = agg_spec.get("aggs", agg_spec.get("aggregations", {}))
    nested_content = ""

    if nested_aggs:
        nested_parts = []
        for sub_name, sub_spec in nested_aggs.items():
            sub_result = convert_aggregation(sub_name, sub_spec)
            if sub_result:
                nested_parts.append(sub_result)

        if nested_parts:
            content_pieces = []
            for part in nested_parts:
                if part.startswith("output("):
                    content_pieces.append(part)
                else:
                    # Nested bucket agg needs all() wrapper
                    content_pieces.append(f"all({part})")
            nested_content = " ".join(content_pieces)

    # Route to specific converter
    if "date_histogram" in agg_spec:
        return convert_date_histogram_agg(agg_spec["date_histogram"], nested_content)

    if "terms" in agg_spec:
        return convert_terms_agg(agg_spec["terms"], nested_content)

    if "multi_terms" in agg_spec:
        return convert_multi_terms_agg(agg_spec["multi_terms"], nested_content)

    if "cardinality" in agg_spec:
        return convert_cardinality_agg(agg_spec["cardinality"])

    if "range" in agg_spec:
        return convert_range_agg(agg_spec["range"], nested_content)

    if "histogram" in agg_spec:
        return convert_histogram_agg(agg_spec["histogram"], nested_content)

    if "auto_date_histogram" in agg_spec:
        return convert_auto_date_histogram_agg(agg_spec["auto_date_histogram"], nested_content)

    if "composite" in agg_spec:
        return convert_composite_agg(agg_spec["composite"])

    for metric_type in ["sum", "avg", "min", "max", "stats", "value_count"]:
        if metric_type in agg_spec:
            return convert_metric_agg(metric_type, agg_spec[metric_type])

    if "significant_terms" in agg_spec:
        return convert_terms_agg(agg_spec["significant_terms"], nested_content)

    return ""


def convert_date_histogram_agg(spec: Dict, nested_content: str = "") -> str:
    """Convert date_histogram aggregation to Vespa grouping.

    group(floor(timestamp / 3600000)) each(output(count()))
    """
    field = map_field_name(spec.get("field", "timestamp"))
    interval = spec.get("calendar_interval", spec.get("fixed_interval", spec.get("interval", "hour")))

    interval_ms_map = {
        "second": 1000, "1s": 1000,
        "minute": 60000, "1m": 60000,
        "hour": 3600000, "1h": 3600000,
        "day": 86400000, "1d": 86400000,
        "week": 604800000, "1w": 604800000,
        "month": 2592000000, "1M": 2592000000,
    }

    interval_ms = interval_ms_map.get(interval, 3600000)

    each_content = "output(count())"
    if nested_content:
        each_content += f" {nested_content}"

    return f"group(floor({field} / {interval_ms})) each({each_content})"


def convert_terms_agg(spec: Dict, nested_content: str = "") -> str:
    """Convert terms aggregation to Vespa grouping.

    group(field_name) max(10) order(-count()) each(output(count()))
    OpenSearch default order is by doc count descending — order(-count()) matches.
    """
    field = map_field_name(spec.get("field", ""))
    size = spec.get("size", 10)

    each_content = "output(count())"
    if nested_content:
        each_content += f" {nested_content}"

    return f"group({field}) max({size}) order(-count()) each({each_content})"


def convert_cardinality_agg(spec: Dict) -> str:
    """Convert cardinality aggregation to Vespa grouping.

    OpenSearch uses HyperLogLog (approximate). Vespa equivalent: group by field
    and count groups, bounded by max() to prevent full enumeration.
    precision_threshold maps to max groups (default 3000, capped at 10000).
    """
    field = map_field_name(spec.get("field", ""))
    precision = spec.get("precision_threshold", 3000)
    max_groups = min(precision, 10000)
    return f"group({field}) max({max_groups}) each(output(count()))"


def convert_range_agg(spec: Dict, nested_content: str = "") -> str:
    """Convert range aggregation to Vespa grouping with predefined buckets.

    OpenSearch range agg defines explicit bucket boundaries. Vespa equivalent
    uses predefined() with bucket(from, to) where [from, to) semantics match.

    {"ranges": [{"to": -10}, {"from": -10, "to": 10}, {"from": 2000}]}
    → group(predefined(field, bucket(-inf, -10), bucket(-10, 10), bucket(2000, inf)))
      each(output(count()))
    """
    field = map_field_name(spec.get("field", ""))
    ranges = spec.get("ranges", [])

    each_content = "output(count())"
    if nested_content:
        each_content += f" {nested_content}"

    if not ranges:
        return f"group({field}) each({each_content})"

    bucket_strs = []
    for r in ranges:
        low = r.get("from", None)
        high = r.get("to", None)
        low_str = str(low) if low is not None else "-inf"
        high_str = str(high) if high is not None else "inf"
        bucket_strs.append(f"bucket({low_str}, {high_str})")

    predefined_expr = f"predefined({field}, {', '.join(bucket_strs)})"
    return f"group({predefined_expr}) each({each_content})"


def convert_histogram_agg(spec: Dict, nested_content: str = "") -> str:
    """Convert histogram aggregation to Vespa grouping.

    group(floor(field / interval)) each(output(count()))
    """
    field = map_field_name(spec.get("field", ""))
    interval = spec.get("interval", 100)

    each_content = "output(count())"
    if nested_content:
        each_content += f" {nested_content}"

    return f"group(floor({field} / {interval})) each({each_content})"


def convert_auto_date_histogram_agg(spec: Dict, nested_content: str = "") -> str:
    """Convert auto_date_histogram to Vespa grouping (defaults to hourly)."""
    field = map_field_name(spec.get("field", "timestamp"))
    buckets = spec.get("buckets", 10)

    each_content = "output(count())"
    if nested_content:
        each_content += f" {nested_content}"

    return f"group(floor({field} / 3600000)) max({buckets}) each({each_content})"


def convert_composite_agg(spec: Dict) -> str:
    """Convert composite aggregation to Vespa nested grouping.

    For date_histogram sources, applies floor(field / interval_ms) to create
    time buckets instead of grouping by raw millisecond timestamp.
    """
    sources = spec.get("sources", [])
    size = spec.get("size", 10)

    if not sources:
        return ""

    interval_ms_map = {
        "second": 1000, "1s": 1000,
        "minute": 60000, "1m": 60000,
        "hour": 3600000, "1h": 3600000,
        "day": 86400000, "1d": 86400000,
        "week": 604800000, "1w": 604800000,
        "month": 2592000000, "1M": 2592000000,
    }

    group_exprs = []
    for source in sources:
        for _, source_spec in source.items():
            if "terms" in source_spec:
                field = map_field_name(source_spec["terms"].get("field", ""))
                group_exprs.append(field)
            elif "date_histogram" in source_spec:
                dh = source_spec["date_histogram"]
                field = map_field_name(dh.get("field", ""))
                interval = dh.get("calendar_interval", dh.get("fixed_interval", dh.get("interval", "day")))
                interval_ms = interval_ms_map.get(interval, 86400000)
                group_exprs.append(f"floor({field} / {interval_ms})")

    if len(group_exprs) == 1:
        return f"group({group_exprs[0]}) max({size}) each(output(count()))"
    elif len(group_exprs) == 2:
        return (f"group({group_exprs[0]}) max({size}) "
                f"each(group({group_exprs[1]}) max({size}) each(output(count())))")
    elif len(group_exprs) >= 3:
        return (f"group({group_exprs[0]}) max({size}) "
                f"each(group({group_exprs[1]}) max({size}) "
                f"each(group({group_exprs[2]}) max({size}) each(output(count()))))")

    return ""


def convert_multi_terms_agg(spec: Dict, nested_content: str = "") -> str:
    """Convert multi_terms aggregation to Vespa nested grouping.

    OpenSearch multi_terms groups by multiple fields simultaneously.
    Vespa equivalent: nested group() calls, one per field.

    {"terms": [{"field": "process.name"}, {"field": "cloud.region"}]}
    → group(process_name) max(10) each(group(cloud_region) max(10) each(output(count())))
    """
    terms_list = spec.get("terms", [])
    size = spec.get("size", 10)

    fields = [map_field_name(t.get("field", "")) for t in terms_list]

    if not fields:
        return ""

    # Build from innermost to outermost
    inner_content = "output(count())"
    if nested_content:
        inner_content += f" {nested_content}"

    # Start with innermost field
    result = f"group({fields[-1]}) max({size}) each({inner_content})"

    # Wrap with outer fields (right to left, skipping last which is already used)
    for field in reversed(fields[:-1]):
        result = f"group({field}) max({size}) each({result})"

    return result


def convert_metric_agg(metric_type: str, spec: Dict) -> str:
    """Convert metric aggregation (sum, avg, min, max) to Vespa output clause.

    Returns output(...) format — caller wraps in all() if standalone.
    """
    field = map_field_name(spec.get("field", ""))

    if metric_type == "stats":
        return (f"output(sum({field})) output(avg({field})) "
                f"output(min({field})) output(max({field})) output(count())")
    elif metric_type == "value_count":
        return "output(count())"
    else:
        return f"output({metric_type}({field}))"


# =============================================================================
# Response Conversion
# =============================================================================

def convert_vespa_response(vespa_response: Dict) -> Dict[str, Any]:
    """Convert Vespa search response to OpenSearch format."""
    hits = vespa_response.get("root", {}).get("children", [])
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


def convert_metrics_to_stats(metrics: Dict, index: Optional[str]) -> Dict[str, Any]:
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


# =============================================================================
# Utility
# =============================================================================

def wait_for_vespa(vespa_client, max_attempts=40):
    """Wait for Vespa to be ready by polling health endpoint.

    :param vespa_client: Vespa client instance (sync, with endpoint attribute)
    :param max_attempts: Maximum number of health check attempts
    :return: True if Vespa is ready, False otherwise
    """
    import requests

    for attempt in range(max_attempts):
        try:
            endpoint = f"{vespa_client.endpoint}/state/v1/health"
            response = requests.get(endpoint, timeout=5)
            health = response.json()
            status = health.get("status", {}).get("code", "down")

            if status in ("up", "initializing"):
                logger.info("Vespa is ready after %d attempts", attempt)
                return True

            logger.debug("Vespa not ready (status=%s), attempt %d/%d", status, attempt, max_attempts)
            time.sleep(3)

        except Exception as e:
            logger.debug("Health check failed on attempt %d: %s", attempt, e)
            time.sleep(3)

    logger.warning("Vespa not ready after %d attempts", max_attempts)
    return False
