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
from typing import Any, Dict, List, Optional


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
    if keys in ({"value"}, {"values"}):
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
        if abs(date_value) < 1e12:
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

    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y/%m/%d",
                "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S"]:
        try:
            dt = datetime.strptime(date_value, fmt)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue

    logger.warning("Could not parse date value: %s", date_value)
    return 0


def transform_document_for_vespa(doc: Dict) -> Dict:
    """Transform an OpenSearch document to Vespa format.

    Flattens nested fields, converts timestamps, and maps field names.
    Unknown fields should be handled by Vespa's ignore-undefined-fields schema setting.
    """
    vespa_doc = {}

    def flatten(obj: Any, prefix: str = "") -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}_{key}" if prefix else key

                if isinstance(value, dict) and not is_leaf_value(value):
                    flatten(value, new_key)
                else:
                    mapped_key = FIELD_NAME_MAPPING.get(new_key)
                    if mapped_key is None:
                        original_path = new_key.replace("_", ".")
                        mapped_key = FIELD_NAME_MAPPING.get(original_path, new_key.replace(".", "_"))

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
    elif "timestamp" in doc and isinstance(doc["timestamp"], str):
        vespa_doc["timestamp"] = date_to_epoch(doc["timestamp"])
        doc = {k: v for k, v in doc.items() if k != "timestamp"}

    flatten(doc)

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
            action_type = "index"
            for act in ("index", "update", "create"):
                if act in first_item and isinstance(first_item.get(act), dict):
                    action_type = act
                    break
            if action_type:
                documents = []
                for i in range(0, len(body_list) - 1, 2):
                    action = body_list[i]
                    doc_body = body_list[i + 1]
                    act_meta = action.get(action_type, action.get("index", {}))
                    doc_id = act_meta.get("_id", f"doc_{len(documents)}")
                    # For updates, the doc body is inside {"doc": {...}}
                    if action_type == "update" and "doc" in doc_body:
                        doc_body = doc_body["doc"]
                    documents.append({"_id": doc_id, "_source": doc_body, "_action": action_type})
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
                # Detect action type
                action_type = "index"
                doc_id = None
                for act in ("index", "update", "create"):
                    if act in action:
                        action_type = act
                        doc_id = action[act].get("_id")
                        break
                if not doc_id:
                    doc_id = str(uuid.uuid4())
                # For updates, the doc body is inside {"doc": {...}}
                if action_type == "update" and "doc" in doc_body:
                    doc_body = doc_body["doc"]
                documents.append({"_id": doc_id, "_source": doc_body, "_action": action_type})
                i += 2
            else:
                i += 1
        except json.JSONDecodeError:
            logger.warning("Failed to parse bulk line: %s", lines[i])
            i += 1

    return documents


# =============================================================================
# Response Conversion
# =============================================================================

def convert_vespa_response(vespa_response: Dict) -> Dict[str, Any]:
    """Convert Vespa search response to OpenSearch format."""
    hits = vespa_response.get("root", {}).get("children", [])
    root_fields = vespa_response.get("root", {}).get("fields", {})
    total_count = root_fields.get("totalCount", len(hits))

    return {
        # Vespa returns seconds, OpenSearch expects milliseconds
        "took": int(vespa_response.get("timing", {}).get("searchtime", 0) * 1000),
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
    import requests  # pylint: disable=import-outside-toplevel

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
