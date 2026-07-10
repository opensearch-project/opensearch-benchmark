# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""Pure helpers for the ClickHouse backend.

Everything here (except wait_for_clickhouse) is I/O-free and unit-testable.
"""

import json
import logging
import re
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import requests

from osbenchmark import exceptions

logger = logging.getLogger(__name__)

# Actions supported by ClickHouseDatabaseClient.bulk. Non-index
# actions ("update", "delete") are not currently routable to ClickHouse's
# insert-only path and MUST be rejected at parse time.
_SUPPORTED_BULK_ACTIONS = frozenset({"index", "create"})
_ALL_BULK_ACTIONS = frozenset({"index", "create", "update", "delete"})

# Semver validator used by info()
_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


def _ns_to_ms(summary_dict: Optional[Mapping[str, Any]]) -> int:
    """Coerce elapsed_ns (a string from the X-ClickHouse-Summary header) to ms.

    Handles the string->int conversion BEFORE floor-division, avoiding the
    ``TypeError: unsupported operand type(s) for //: 'str' and 'int'`` bug
    that would otherwise occur on every ClickHouse RPC.
    """
    if not summary_dict:
        return 0
    raw = summary_dict.get("elapsed_ns", 0)
    try:
        return int(raw) // 1_000_000
    except (TypeError, ValueError):
        return 0


def quote_identifier(name: str) -> str:
    """Return `name` as a backticked ClickHouse identifier.

    Escapes embedded backticks by doubling. Rejects None, empty strings, and
    strings containing NUL/CR/LF, which ClickHouse rejects even when backticked.
    This is used for table and column names originating from workload YAML.
    Do NOT use it for values (use parameterized queries for values).
    """
    if name is None:
        raise ValueError("Cannot quote a None identifier")
    if not isinstance(name, str) or not name:
        raise ValueError(f"Cannot quote empty or non-string identifier: {name!r}")
    if "\x00" in name or "\r" in name or "\n" in name:
        raise ValueError(f"Identifier contains disallowed control character: {name!r}")
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def parse_bulk_body(body: Any) -> List[Dict[str, Any]]:
    """Normalize an OSB bulk body into a list of {"_id","_source","_action"} dicts.

    Accepts bytes (NDJSON), string (NDJSON), or a Python list of alternating
    action/doc dicts. Rejects any 'update' or 'delete' action with a
    BenchmarkError, since ClickHouse's insert-only bulk path cannot route
    these actions.

    Fails LOUD on excessive corruption: if more than 1% of lines could not be
    parsed as JSON, raises BenchmarkError. Prevents silent throughput fraud
    from a truncated bulk feed.
    """
    # bytes -> str
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8")

    docs: List[Dict[str, Any]] = []

    if isinstance(body, str):
        raw_lines = [line for line in body.split("\n") if line.strip()]
        parsed: List[Dict[str, Any]] = []
        skipped = 0
        for line in raw_lines:
            try:
                parsed.append(json.loads(line))
            except json.JSONDecodeError as exc:
                logger.warning("Skipping invalid bulk NDJSON: %s", exc)
                skipped += 1
        if raw_lines and skipped / len(raw_lines) > 0.01:
            raise exceptions.BenchmarkError(
                f"parse_bulk_body: {skipped}/{len(raw_lines)} lines could not be parsed as JSON "
                f"(>1% corruption). Bulk feed is likely truncated or malformed."
            )
        # Reject delete actions BEFORE pair-consumption to prevent mis-pairing
        i = 0
        while i < len(parsed):
            action_meta = parsed[i]
            action = _extract_action(action_meta)
            _reject_non_index_actions(action)
            # index/create actions must have a source line
            if i + 1 >= len(parsed):
                raise exceptions.BenchmarkError(
                    f"parse_bulk_body: action '{action}' at position {i} has no source line"
                )
            source = parsed[i + 1]
            doc_id = _extract_doc_id(action_meta)
            docs.append({"_id": doc_id, "_source": source, "_action": action})
            i += 2
        return docs

    if isinstance(body, list):
        if body and isinstance(body[0], dict) and "_source" in body[0]:
            return body  # already normalized
        i = 0
        while i < len(body):
            action_meta = body[i]
            action = _extract_action(action_meta)
            _reject_non_index_actions(action)
            if i + 1 >= len(body):
                raise exceptions.BenchmarkError(
                    f"parse_bulk_body: action '{action}' at position {i} has no source dict"
                )
            source = body[i + 1]
            doc_id = _extract_doc_id(action_meta)
            docs.append({"_id": doc_id, "_source": source, "_action": action})
            i += 2
        return docs

    raise ValueError(f"Unsupported bulk body type: {type(body).__name__}")


def _reject_non_index_actions(action: str) -> None:
    if action in ("update", "delete"):
        raise exceptions.BenchmarkError(
            f"ClickHouse bulk does not support '{action}' actions in v1. "
            f"Only 'index' and 'create' actions are routed to client.insert(). "
            f"See Follow-ups / v2 for ReplacingMergeTree-based upsert support."
        )
    if action not in _SUPPORTED_BULK_ACTIONS:
        raise exceptions.BenchmarkError(f"Unknown bulk action: {action}")


def _extract_action(action_meta: Dict[str, Any]) -> str:
    for key in _ALL_BULK_ACTIONS:
        if key in action_meta:
            return key
    return "index"


def _extract_doc_id(action_meta: Dict[str, Any]) -> Optional[str]:
    for key in _ALL_BULK_ACTIONS:
        if key in action_meta:
            meta = action_meta[key] or {}
            return meta.get("_id")
    return None


def rows_from_docs(docs: List[Dict[str, Any]], columns: List[str], strict: bool = False) -> List[Tuple]:
    """Project docs into positional row tuples aligned with `columns`.

    Missing columns become None. If any doc contains keys NOT in `columns`,
    they are silently dropped from the fast path - the caller is expected to
    detect this and switch to the JSONEachRow fallback.

    When strict=True, raises BenchmarkError on any missing column instead of
    substituting None.
    """
    rows: List[Tuple] = []
    columns_set = set(columns)
    for i, doc in enumerate(docs):
        source = doc.get("_source", {}) or {}
        if strict:
            missing = columns_set - set(source.keys())
            if missing:
                raise exceptions.BenchmarkError(
                    f"rows_from_docs: doc {i} (_id={doc.get('_id')}) is missing "
                    f"columns {sorted(missing)} required by column-names."
                )
        rows.append(tuple(source.get(col) for col in columns))
    return rows


def docs_have_extra_keys(docs: List[Dict[str, Any]], columns: List[str]) -> bool:
    """Return True if any doc contains keys not present in `columns`.

    Used by client.bulk() to trigger the JSONEachRow fallback path.
    """
    columns_set = set(columns)
    for doc in docs:
        source = doc.get("_source", {}) or {}
        if set(source.keys()) - columns_set:
            return True
    return False


def coerce_parameters(params: Any) -> Any:
    """Recursively convert numpy scalars and arrays to Python built-ins.

    clickhouse-connect's ``parameters`` argument expects Python lists (for
    Array-typed parameters) and Python scalars. numpy types serialize as
    ``str(x)`` which produces ``'[1.0 2.0 3.0]'`` - no commas, ClickHouse
    parses as a text-cast failure. This helper handles the coercion.
    Safe to call on non-numpy inputs (returns them unchanged).
    """
    if params is None:
        return None
    # numpy is optional at import time - only coerce when it is available.
    try:
        import numpy as np  # type: ignore  # pylint: disable=import-outside-toplevel
    except ImportError:
        return params

    def _coerce(value: Any) -> Any:
        if isinstance(value, np.ndarray):
            return value.tolist()
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, dict):
            return {k: _coerce(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return type(value)(_coerce(v) for v in value)
        return value

    return _coerce(params)


def convert_query_result_to_search_response(
    result_rows: List[Tuple],
    column_names: Tuple[str, ...],
    elapsed_ns: Optional[Any] = None,
    total_hits: Optional[int] = None,
) -> Dict[str, Any]:
    """Convert a clickhouse_connect QueryResult into an OpenSearch-shaped dict.

    Note on ``hits.total.value``: unless the caller supplies ``total_hits`` from
    a companion COUNT query, this value equals ``len(hits)`` - i.e. the number
    of returned rows, NOT the number of matched rows. Workloads that make
    assertions on total match count MUST supply ``total_hits`` from a separate
    COUNT query.
    """
    hits = []
    for row in result_rows:
        source = dict(zip(column_names, row))
        hit_id = source.pop("_id", "") if "_id" in source else ""
        hits.append({
            "_index": "",
            "_id": str(hit_id) if hit_id != "" else "",
            "_score": None,
            "_source": source,
        })

    took_ms = _ns_to_ms({"elapsed_ns": elapsed_ns}) if elapsed_ns is not None else 0
    hits_count = total_hits if total_hits is not None else len(hits)
    return {
        "took": took_ms,
        "timed_out": False,
        "hits": {
            "total": {"value": hits_count, "relation": "eq"},
            "max_score": None,
            "hits": hits,
        },
    }


def convert_query_result_for_vector_search(
    result_rows: List[Tuple],
    column_names: Tuple[str, ...],
    score_column: str = "score",
    id_column: str = "id",
    elapsed_ns: Optional[Any] = None,
) -> Dict[str, Any]:
    """Convert a k-NN QueryResult to OpenSearch-shaped hits with _score populated."""
    hits = []
    max_score = None
    for row in result_rows:
        source = dict(zip(column_names, row))
        score = source.pop(score_column, None) if score_column in source else None
        hit_id = source.pop(id_column, "") if id_column in source else ""
        if score is not None:
            if max_score is None or score > max_score:
                max_score = score
        hits.append({
            "_index": "",
            "_id": str(hit_id) if hit_id != "" else "",
            "_score": score,
            "_source": source,
        })
    took_ms = _ns_to_ms({"elapsed_ns": elapsed_ns}) if elapsed_ns is not None else 0
    return {
        "took": took_ms,
        "timed_out": False,
        "hits": {
            "total": {"value": len(hits), "relation": "eq"},
            "max_score": max_score,
            "hits": hits,
        },
    }


def build_stats_response(rows: int, bytes_on_disk: int, index_name: Optional[str]) -> Dict[str, Any]:
    """Fabricate an OS-shaped indices/stats response from ClickHouse system.parts row.

    Caveat: on ReplicatedMergeTree tables, ``system.parts`` reflects only the local
    node - cluster-wide byte totals are 2-3x higher. Callers should log a warning
    on first use and users should multiply by replica count.
    """
    stats = {
        "docs": {"count": rows, "deleted": 0},
        "store": {"size_in_bytes": bytes_on_disk},
    }
    envelope = {
        "_all": {"primaries": stats, "total": stats},
        "indices": {},
    }
    if index_name:
        envelope["indices"][index_name] = {"primaries": stats, "total": stats}
    return envelope


def parse_version(version_str: str) -> str:
    """Return a strict semver (e.g. '24.8.1') from a ClickHouse version string.

    Handles ``'24.8.1.2684'``, ``'25.5.1.11-stable'``, ``'24.8.1.2684-cloud'``,
    and other well-formed builds. Returns the '24.8.0' fallback for
    non-semver-parseable inputs like ``'head-fcbd7a4'`` or ``'24.8'``.
    """
    if not version_str or not isinstance(version_str, str):
        return "24.8.0"
    parts = version_str.split(".")
    if len(parts) < 3:
        return "24.8.0"
    # Take first three parts, strip any build suffix from the third.
    third = parts[2].split("-")[0]
    candidate = f"{parts[0]}.{parts[1]}.{third}"
    if not _SEMVER_RE.match(candidate):
        return "24.8.0"
    return candidate


def wait_for_clickhouse(
    ch_client: Any,
    max_attempts: int = 40,
    sleep_seconds: float = 3.0,
) -> bool:
    """Poll GET {ch_client.endpoint}/ping until the server returns 200 or attempts exhausted.

    Mirrors wait_for_vespa: same retry loop, INFO on ready, DEBUG on retry,
    WARNING for SSL errors.

    Accepts any 200 response (not just body == 'Ok.'), because ClickHouse cloud
    proxies can return JSON like '{"status":"ok"}' at /ping.
    """
    # Duck-typed endpoint resolution.
    endpoint = getattr(ch_client, "endpoint", None)
    verify: Any = True
    if endpoint is None:
        # Derive from the client's hosts/options as a fallback.
        hosts = getattr(ch_client, "_hosts", None) or []
        options = getattr(ch_client, "client_options", {}) or {}
        host, port, secure = parse_hosts(hosts) if hosts else ("localhost", 8123, False)
        scheme = "https" if secure else "http"
        endpoint = f"{scheme}://{host}:{port}"
        verify = options.get("ssl_verify", True)
    else:
        options = getattr(ch_client, "client_options", {}) or {}
        verify = options.get("ssl_verify", True)

    endpoint = endpoint.rstrip("/")
    url = f"{endpoint}/ping"
    for attempt in range(max_attempts):
        try:
            resp = requests.get(url, timeout=5, verify=verify)
            if resp.status_code == 200:
                logger.info("ClickHouse ready after %d attempt(s)", attempt + 1)
                return True
            logger.debug("ClickHouse not ready yet (status=%s, body=%r)", resp.status_code, resp.text[:64])
        except requests.exceptions.SSLError as exc:
            # Distinct WARNING for SSL - usually indicates a misconfigured verify=
            logger.warning("ClickHouse ping attempt %d failed with SSL error: %s", attempt + 1, exc)
        except requests.RequestException as exc:
            logger.debug("ClickHouse ping attempt %d failed: %s", attempt + 1, exc)
        time.sleep(sleep_seconds)
    return False


def parse_hosts(hosts: Any) -> Tuple[str, int, bool]:
    """Extract (host, port, secure) from OSB's hosts list.

    Validates port range 1-65535. Supports IPv6 by unwrapping brackets.
    Sets secure=True for both port 8443 (HTTPS) and 9440 (native TLS -
    though only 8443 works with clickhouse-connect's HTTP driver). Rejects
    port 9000 (native binary protocol) with a clear message.
    """
    if isinstance(hosts, dict):
        hosts = hosts.get("default", [])
    if not hosts:
        raise exceptions.SystemSetupError("No ClickHouse hosts configured")
    first = hosts[0]
    host = first.get("host", "localhost")
    # IPv6: strip brackets if present so clickhouse-connect can parse
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        port = int(first.get("port", 8123))
    except (TypeError, ValueError) as exc:
        raise exceptions.SystemSetupError(f"Invalid ClickHouse port: {first.get('port')!r}") from exc
    if not 1 <= port <= 65535:
        raise exceptions.SystemSetupError(f"ClickHouse port out of range: {port}")
    if port == 9000:
        raise exceptions.SystemSetupError(
            "Port 9000 is ClickHouse's native binary protocol. clickhouse-connect uses HTTP; "
            "use port 8123 (HTTP) or 8443 (HTTPS) instead."
        )
    # secure defaults to True for TLS ports; user can override with use_ssl.
    secure = bool(first.get("use_ssl", port in (8443, 9440)))
    return host, port, secure
