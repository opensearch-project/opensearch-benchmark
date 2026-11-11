# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

"""
Flattens documents along the field paths that an index mapping defines.

The flattener is independent of how the documents have been sampled: it only needs the mapping, as returned by
GET /<index>/_mapping, and the ``_source`` of the documents.
"""

import logging
from typing import Any, Dict, Iterable, List, Optional, Set

CONTAINER_TYPES = frozenset(["object", "nested"])
# Types whose _source representation is an array or an object but that are one logical value
# (a vector is not a list of values, a geo_point is not an array of coordinates).
ATOMIC_TYPES = frozenset(["knn_vector", "sparse_vector", "geo_point"])

MAX_LOGGED_SKIPPED_FIELDS = 20


def _is_number_list(values: List[Any]) -> bool:
    return bool(values) and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values)


def mapping_properties(mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts the properties from an index mapping.

    Accepts the response of GET /<index>/_mapping, the mapping of an index (``mappings``) and the properties themselves.
    """
    if "mappings" in mapping:
        return mapping["mappings"].get("properties", {})
    if "properties" in mapping:
        return mapping["properties"]
    # response of GET /<index>/_mapping: {"<index>": {"mappings": {...}}}
    indices = [v for v in mapping.values() if isinstance(v, dict) and "mappings" in v]
    if len(indices) == 1:
        return indices[0]["mappings"].get("properties", {})
    if len(indices) > 1:
        raise ValueError("The mapping contains [%d] indices but exactly one index is expected." % len(indices))
    raise ValueError("The mapping contains neither 'mappings' nor 'properties'.")


class _Node:
    def __init__(self, name: str, path: str, field_type: str, definition: Dict[str, Any]):
        self.name = name
        self.path = path
        self.type = field_type
        self.definition = definition
        self.children: Dict[str, "_Node"] = {}

    @property
    def is_container(self) -> bool:
        return self.type in CONTAINER_TYPES


def _expand_dotted_names(properties: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expands field names with dots into objects, as OpenSearch does: {"user.name": {...}} is the same as
    {"user": {"properties": {"name": {...}}}}.
    """
    expanded: Dict[str, Any] = {}
    for name, definition in properties.items():
        *parents, last = name.split(".")
        target = expanded
        for parent in parents:
            target = target.setdefault(parent, {"properties": {}}).setdefault("properties", {})
        existing = target.get(last)
        if existing is not None and "properties" in definition:
            # the field is also defined implicitly through a dotted name: keep the properties of both
            definition = dict(definition, properties={**existing.get("properties", {}), **definition["properties"]})
        elif existing is not None and "properties" in existing:
            definition = dict(definition, properties=existing["properties"])
        target[last] = definition
    return expanded


def _build_nodes(properties: Dict[str, Any], prefix: str) -> Dict[str, _Node]:
    nodes = {}
    for name, definition in _expand_dotted_names(properties).items():
        path = "%s.%s" % (prefix, name) if prefix else name
        field_type = definition.get("type")
        # a field without a type but with properties is an object. Multi-fields ("fields") only index the same value in another way.
        if field_type is None and "properties" in definition:
            field_type = "object"
        if field_type is None:
            continue
        node = _Node(name, path, field_type, definition)
        if node.is_container:
            node.children = _build_nodes(definition.get("properties", {}), path)
        nodes[name] = node
    return nodes


class Flattener:
    """
    Flattens documents into the values of every field path of the mapping.

    Fields that are in a document but not in the mapping (e.g. dynamically mapped fields) are skipped. Their paths are collected in
    :attr:`skipped_paths`.
    """

    def __init__(self, mapping: Dict[str, Any]):
        self._nodes = _build_nodes(mapping_properties(mapping), "")
        self.fields: Dict[str, _Node] = {}
        self._collect_leaves(self._nodes)
        self.skipped_paths: Set[str] = set()

    def _collect_leaves(self, nodes: Dict[str, _Node]):
        for node in nodes.values():
            if node.is_container:
                self._collect_leaves(node.children)
            else:
                self.fields[node.path] = node

    @staticmethod
    def _lookup(nodes: Dict[str, _Node], name: str) -> Optional[_Node]:
        """
        Finds the node of a key of the ``_source``. OpenSearch accepts keys with dots, e.g. {"user.name": "x"}, and indexes them
        like {"user": {"name": "x"}}.
        """
        node = nodes.get(name)
        if node is not None or "." not in name:
            return node
        head, _, rest = name.partition(".")
        parent = nodes.get(head)
        if parent is None or not parent.is_container:
            return None
        return Flattener._lookup(parent.children, rest)

    @staticmethod
    def _atomic_values(field_type: str, raw: Any) -> List[Any]:
        """
        The values of a field whose representation is an array or an object but that is one logical value.

        A geo_point can have several values: an array of points (objects, strings or [lon, lat] arrays). A list of numbers is one
        point ([lon, lat]), so it is not split. A vector is always one value.
        """
        if field_type != "geo_point" or not isinstance(raw, list) or _is_number_list(raw):
            return [raw]
        return [point for point in raw if point is not None]

    def flatten(self, doc: Dict[str, Any]) -> Dict[str, List[Any]]:
        """
        :param doc: The ``_source`` of a document.
        :return: The values of the document per field path. Fields that are missing, null or empty are not contained.
        """
        values: Dict[str, List[Any]] = {}
        self._flatten_object(doc, self._nodes, "", values)
        return values

    def _flatten_object(self, obj: Any, nodes: Dict[str, _Node], prefix: str, values: Dict[str, List[Any]]):
        if not isinstance(obj, dict):
            return
        for name, raw in obj.items():
            node = self._lookup(nodes, name)
            if node is None:
                self.skipped_paths.add("%s.%s" % (prefix, name) if prefix else name)
            elif raw is None:
                continue
            elif node.is_container:
                # an object or nested field can be a single object or an array of objects
                for child in raw if isinstance(raw, list) else [raw]:
                    self._flatten_object(child, node.children, node.path, values)
            elif node.type in ATOMIC_TYPES:
                values.setdefault(node.path, []).extend(self._atomic_values(node.type, raw))
            else:
                for value in raw if isinstance(raw, list) else [raw]:
                    if value is not None:
                        values.setdefault(node.path, []).append(value)


def log_skipped_fields(logger: logging.Logger, skipped_paths: Iterable[str]):
    """Logs how many fields were skipped because the mapping does not define them."""
    skipped = sorted(skipped_paths)
    logger.info("Skipped [%d] fields that are in the samples but not in the mapping.", len(skipped))
    if skipped:
        logger.debug("Skipped fields (first %d): %s", MAX_LOGGED_SKIPPED_FIELDS, skipped[:MAX_LOGGED_SKIPPED_FIELDS])
