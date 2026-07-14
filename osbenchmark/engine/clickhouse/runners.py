# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""ClickHouse-native runners for OSB operations."""

import logging
import re

from osbenchmark import exceptions
from osbenchmark.worker_coordinator.runner import Runner, request_context_holder

logger = logging.getLogger(__name__)

# Match ORDER BY as a real SQL clause boundary. We require ORDER to be
# preceded by a non-word character (space/newline/paren) and followed by
# whitespace + BY + a non-word character. This matches real SQL clauses but
# NOT identifiers like 'ORDER_BY_ID' or truncated substrings inside a longer
# identifier like 'ORDER BYPASS_COL'.
_ORDER_BY_RE = re.compile(r"(?:^|[\s,()])ORDER\s+BY(?=[\s(]|$)", re.IGNORECASE)
# Line comment: `-- ...` to end of line. ClickHouse (and every ISO-compliant
# SQL dialect) requires whitespace after -- to distinguish a comment from
# arithmetic operators or identifier fragments. Matching the whitespace here
# avoids false-stripping legitimate SQL like `WHERE x = 'a--b'`.
_SQL_LINE_COMMENT_RE = re.compile(r"--[ \t\r\f\v][^\n]*")
_SQL_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
# Single-quoted string literal. Handles doubled '' escapes.
_SQL_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")


def _strip_sql_noise(sql: str) -> str:
    """Return `sql` with comments and single-quoted string literals removed.

    Used to make ``ORDER BY``-in-SQL detection robust against string contents
    and comments. NOT suitable as a SQL sanitizer for execution.

    Ordering: strip block comments first (they can contain -- or '), then
    string literals (they can contain -- or /* */), then line comments last
    (they can appear anywhere). This ordering prevents the classic bug where
    stripping line comments first eats content inside a string literal like
    ``WHERE label = 'first--second'``.
    """
    stripped = _SQL_BLOCK_COMMENT_RE.sub(" ", sql)
    stripped = _SQL_STRING_LITERAL_RE.sub(" ", stripped)
    stripped = _SQL_LINE_COMMENT_RE.sub(" ", stripped)
    return stripped


def _compute_recall(hits, neighbors, k):
    """Compute recall@k and recall@1 with integer-normalization when possible.

    If both retrieved and truth IDs parse as integers, compare as integers to
    handle zero-padded truth IDs (e.g. '00001' vs '1').
    """
    def _normalize(value):
        s = str(value)
        try:
            return int(s)
        except (TypeError, ValueError):
            return s
    retrieved_ids = {_normalize(h["_id"]) for h in hits}
    truth = {_normalize(n) for n in neighbors[:k]}
    if not truth:
        return 0.0, 0.0
    recall_k = len(retrieved_ids & truth) / len(truth)
    recall_1 = 1.0 if hits and _normalize(hits[0]["_id"]) in truth else 0.0
    return recall_k, recall_1


class ClickHouseBulkIndex(Runner):
    """Bulk-insert docs into ClickHouse.

    Params consumed:
      - index: str (table name)
      - body: list[dict] or bytes/str (NDJSON) - the OSB bulk body
      - bulk-size: int - count of docs
      - unit: str - "docs" typically
      - column-names: list[str] - REQUIRED for the fast Native path; if absent
        or if docs have extra keys, the client automatically falls back to
        JSONEachRow.
    """
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        # pylint: disable=import-outside-toplevel
        from osbenchmark.engine.clickhouse.helpers import parse_bulk_body
        index = params.get("index") or params.get("table")
        body = params.get("body")
        bulk_size = params.get("bulk-size", 0)
        unit = params.get("unit", "docs")

        # Zero-doc guard + one-shot parse. We parse the body once here and
        # thread the result to client.bulk() so it doesn't re-parse. This
        # avoids the double-parse hot-path cost on every bulk operation.
        #
        # parse_bulk_body raises BenchmarkError on data corruption (malformed
        # NDJSON, misaligned action/source pairs, unsupported action types).
        # These are per-sample failures, NOT benchmark-aborting configuration
        # errors, so catch here and return an error dict rather than letting
        # the exception propagate through the actor system.
        try:
            parsed_docs = parse_bulk_body(body) if body else []
        except exceptions.BenchmarkError as exc:
            self.logger.warning("Bulk body parse failed: %s", exc)
            return {"weight": bulk_size, "unit": unit, "success": False,
                    "error-count": 1, "error-type": "clickhouse",
                    "error-description": str(exc), "took": 0}
        except Exception:  # pylint: disable=broad-except
            # Programmer error inside parse_bulk_body: re-raise so the actor
            # log captures it. This is intentionally different from the
            # BenchmarkError branch above.
            self.logger.exception("Unexpected parse error")
            raise
        if not parsed_docs:
            # Empty body IS a workload configuration bug (non-zero bulk-size
            # with nothing to insert), NOT a data corruption issue. Raise so
            # operators fix their workload before continuing.
            raise exceptions.BenchmarkError(
                f"Bulk operation on {index!r} produced 0 parsed docs "
                f"(bulk-size={bulk_size}). Workload params-source is likely "
                f"misconfigured - non-zero bulk-size with an empty body is a bug."
            )

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            # Pass pre-parsed docs to skip re-parsing inside client.bulk().
            response = await clickhouse_client.bulk(
                body=body, index=index, params=params, parsed_docs=parsed_docs)
        except Exception as exc:  # pylint: disable=broad-except
            # Surface the failure to the sample stream. We deliberately do NOT
            # mutate clickhouse_client._client here: forcing a re-init on any
            # transient error is a race against _ensure_client's lock, and if
            # the underlying client is genuinely broken the next _ensure_client
            # call won't detect that anyway. Retry semantics belong in a retry
            # decorator, not ad-hoc private-field mutation.
            # Follow-up: wire engine.clickhouse.on_execute_error into the sample
            #   error path so transient httpx errors get consistent handling.
            self.logger.warning("Bulk RPC raised: %s", exc)
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
            return {"weight": bulk_size, "unit": unit, "success": False,
                    "error-count": 1, "error-type": "clickhouse",
                    "error-description": str(exc), "took": 0}
        request_context_holder.on_request_end()
        request_context_holder.on_client_request_end()

        errored = response.get("errors", False)
        # ClickHouse INSERT is all-or-nothing. On failure, count as 1 error, not bulk_size.
        error_count = 1 if errored else 0
        return {
            "weight": bulk_size,
            "unit": unit,
            "success": not errored,
            "error-count": error_count,
            "took": response.get("took", 0),
        }

    def __repr__(self):
        return "clickhouse-bulk-index"


class ClickHouseQuery(Runner):
    """Executes a SQL query.

    Params consumed:
      - body: {"sql": "SELECT ...", "parameters": {...}, "settings": {...}} - REQUIRED
      - detailed-results: bool (currently ignored)
    """
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        body = params.get("body") or {}
        if "sql" not in body:
            raise exceptions.BenchmarkError(
                "ClickHouse runner received a body without a 'sql' key. Workloads must "
                "supply a ClickHouse-native param source that produces {'sql': 'SELECT ...'}."
            )
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            try:
                response = await clickhouse_client.search(body=body)
            except Exception as exc:  # pylint: disable=broad-except
                # Do NOT mutate clickhouse_client._client (race with _ensure_client's lock).
                self.logger.warning("Search RPC raised: %s", exc)
                return {"weight": 1, "unit": "ops", "success": False,
                        "error-count": 1, "took": 0}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        hits = response.get("hits", {})
        return {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "hits": hits.get("total", {}).get("value", 0),
            "hits_relation": hits.get("total", {}).get("relation", "eq"),
            "timed_out": response.get("timed_out", False),
            "took": response.get("took", 0),
        }

    def __repr__(self):
        return "clickhouse-query"


class ClickHouseVectorSearch(Runner):
    """k-NN search using ClickHouse vector_similarity index.

    Uses the client's public ``execute_query``.

    Recall computation caveat: ID type mismatches between the workload's
    ground-truth ``neighbors`` and the returned rows silently produce
    recall=0.0. This runner attempts integer normalization when both sides
    parse as integers; workloads with non-integer IDs must ensure their
    param source emits string IDs matching the ClickHouse output format.
    """
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        # pylint: disable=import-outside-toplevel
        from osbenchmark.engine.clickhouse.helpers import (
            convert_query_result_for_vector_search, coerce_parameters,
        )
        body = params.get("body") or {}
        if "sql" not in body:
            raise exceptions.BenchmarkError(
                "ClickHouseVectorSearch requires a 'sql' key in the body."
            )
        k = params.get("k", 10)
        ef_search = params.get("hnsw_ef_search")
        id_field = params.get("id-field", "id")
        score_field = params.get("score-field", "score")

        settings = dict(body.get("settings") or {})
        # Use `is not None` so an explicit hnsw_ef_search=0 reaches ClickHouse
        # unchanged (0 disables candidate expansion in HNSW-style queries).
        if ef_search is not None:
            settings["hnsw_candidate_list_size_for_search"] = int(ef_search)
            settings.setdefault("max_limit_for_vector_search_queries", max(1000, k))

        parameters = coerce_parameters(body.get("parameters"))

        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            try:
                result = await clickhouse_client.execute_query(
                    body["sql"],
                    parameters=parameters,
                    settings=settings or None,
                )
            except Exception as exc:  # pylint: disable=broad-except
                # Do NOT mutate clickhouse_client._client (race with _ensure_client's lock).
                self.logger.warning("Vector search RPC raised: %s", exc)
                return {"weight": 1, "unit": "ops", "success": False,
                        "error-count": 1, "took": 0}
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()

        elapsed_ns = 0
        if result.summary:
            try:
                elapsed_ns = int(result.summary.get("elapsed_ns", 0))
            except (TypeError, ValueError):
                elapsed_ns = 0
        response = convert_query_result_for_vector_search(
            result.result_rows, result.column_names,
            score_column=score_field, id_column=id_field, elapsed_ns=elapsed_ns,
        )
        hits = response["hits"]["hits"]
        out = {
            "weight": 1, "unit": "ops", "success": True,
            "hits": len(hits), "hits_relation": "eq",
            "timed_out": False, "took": response.get("took", 0),
        }
        if params.get("calculate-recall") and params.get("neighbors"):
            recall_k, recall_1 = _compute_recall(hits, params["neighbors"], k)
            out["recall@k"] = recall_k
            out["recall@1"] = recall_1
        return out

    def __repr__(self):
        return "clickhouse-vector-search"


class ClickHouseScrollQuery(Runner):
    """Simulates OS scroll via workload-supplied {limit:UInt32}/{offset:UInt32} parameters.

    IMPORTANT: the workload SQL MUST contain ClickHouse-style
    ``{limit:UInt32}`` and ``{offset:UInt32}`` parameter placeholders and MUST
    include an ``ORDER BY`` on a deterministic key. The runner refuses to
    proceed if either constraint is violated; it does NOT rewrite user SQL.

    Cost warning: OFFSET-based pagination in ClickHouse is O(N*pages). For
    non-trivial datasets, prefer keyset pagination in the workload SQL rather
    than this runner. This runner exists for OS-parity smoke tests.
    """
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        body = params.get("body") or {}
        if "sql" not in body:
            raise exceptions.BenchmarkError("ClickHouseScrollQuery requires a 'sql' key.")
        base_sql = body["sql"]
        # Reject workload SQL that lacks the required parameter placeholders.
        if "{limit:" not in base_sql or "{offset:" not in base_sql:
            raise exceptions.BenchmarkError(
                "ClickHouseScrollQuery requires the workload SQL to contain "
                "{limit:UInt32} and {offset:UInt32} placeholders. Naive LIMIT/OFFSET "
                "injection is unsafe because it corrupts SQL with existing LIMIT, "
                "ORDER BY, or FORMAT clauses."
            )
        # Strip comments and string literals BEFORE checking for ORDER BY so
        # e.g. `WHERE label = 'ORDER BYPASS'` doesn't false-match.
        if not _ORDER_BY_RE.search(_strip_sql_noise(base_sql)):
            raise exceptions.BenchmarkError(
                "ClickHouseScrollQuery requires the workload SQL to include ORDER BY on a "
                "deterministic key; pagination without ordering returns different rows per page."
            )

        pages_requested = params.get("pages", 10)
        per_page = params.get("results-per-page", 1000)
        total_hits = 0
        pages_actual = 0
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            for page in range(pages_requested):
                base_params = dict(body.get("parameters") or {})
                base_params.update({"limit": per_page, "offset": page * per_page})
                try:
                    result = await clickhouse_client.execute_query(
                        base_sql, parameters=base_params, settings=body.get("settings"),
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    # Do NOT mutate clickhouse_client._client (race with _ensure_client's lock).
                    self.logger.warning("Scroll RPC raised: %s", exc)
                    return {
                        "weight": 1, "unit": "ops", "success": False,
                        "error-count": 1, "pages_actual": pages_actual, "took": 0,
                    }
                pages_actual += 1
                got = len(result.result_rows)
                total_hits += got
                if got < per_page:
                    break
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        return {
            "weight": 1, "unit": "ops", "success": True,
            "pages": pages_requested, "pages_actual": pages_actual,
            "hits": total_hits, "hits_relation": "eq",
            "timed_out": False, "took": 0,
        }

    def __repr__(self):
        return "clickhouse-scroll-query"


class ClickHouseBulkVectorDataSet(Runner):
    """Bulk-ingest vector datasets, pairing alternating action/vector dicts.

    NOTE: the workload param source MUST include ``column-names`` for this
    runner, same as ClickHouseBulkIndex. The runner delegates to
    ``clickhouse_client.bulk`` which requires ``column-names`` for the fast
    Native path (or triggers the JSONEachRow fallback if absent).

    RETURN SHAPE: returns a 2-tuple ``(size, "docs")`` on success and a dict on
    failure. The tuple return matches OSB's BulkVectorDataSet sampling contract
    (execute_single treats a 2-tuple as ``(weight, unit)`` with success=True);
    a dict return with ``success: False`` short-circuits the success path so
    ClickHouse errors don't fabricate throughput.
    """
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        size = params.get("size", 0)
        body = params.get("body", [])
        docs = []
        vec_field = params.get("vector-field", "vec")
        for i in range(0, len(body) - 1, 2):
            action = body[i]
            vector_doc = body[i + 1]
            # Ensure numpy arrays are converted to Python lists
            candidate = vector_doc.get(vec_field, None)
            if hasattr(candidate, "tolist"):
                vector_doc = {**vector_doc, vec_field: candidate.tolist()}
            docs.extend([action, vector_doc])
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            try:
                await clickhouse_client.bulk(body=docs, index=params.get("index"), params=params)
            except Exception as exc:  # pylint: disable=broad-except
                # Do NOT swallow: return an error dict so execute_single records
                # success=False and does NOT fabricate throughput on ClickHouse crash.
                # Do NOT mutate clickhouse_client._client (race with _ensure_client).
                self.logger.warning("BulkVectorDataSet RPC raised: %s", exc)
                return {
                    "weight": 0, "unit": "docs", "success": False,
                    "error-count": 1, "error-type": "clickhouse",
                    "error-description": str(exc),
                }
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        return size, "docs"

    def __repr__(self):
        return "clickhouse-bulk-vector-data-set"


class ClickHouseCreateTable(Runner):
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        indices = params.get("indices", [])
        if not indices and params.get("index"):
            indices = [(params["index"], params.get("body", {}))]
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            for name, body in indices:
                await clickhouse_client.indices.create(index=name, body=body, params=params)
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        return {"weight": len(indices), "unit": "ops", "success": True}

    def __repr__(self):
        return "clickhouse-create-table"


class ClickHouseDropTable(Runner):
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        indices = params.get("indices", [])
        if not indices and params.get("index"):
            indices = [params["index"]]
        # Split any comma-separated entries so `indices=['a,b']` (OSB's typical
        # DeleteIndex shape) drops both tables individually. Without this, the
        # exists()/delete() calls would receive the literal string 'a,b' and
        # fail to match either table.
        expanded = []
        for entry in indices:
            if isinstance(entry, str) and "," in entry:
                expanded.extend(part.strip() for part in entry.split(",") if part.strip())
            else:
                expanded.append(entry)
        indices = expanded
        only_if_exists = params.get("only-if-exists", False)
        deleted = 0
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            for name in indices:
                if only_if_exists and not await clickhouse_client.indices.exists(index=name):
                    continue
                await clickhouse_client.indices.delete(index=name)
                deleted += 1
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        return {"weight": deleted, "unit": "ops", "success": True}

    def __repr__(self):
        return "clickhouse-drop-table"


class ClickHouseSystemParts(Runner):
    """Return shape matches the OS IndicesStats runner.

    OS returns: {"weight":1,"unit":"ops","success":True,"stats":stats,
                 "index":str,"primaries":dict}. This mirror populates each key
    so downstream sample consumers work unchanged.
    """
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        index = params.get("index")
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            stats = await clickhouse_client.indices.stats(index=index)
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        primaries = stats.get("_all", {}).get("primaries", {})
        return {
            "weight": 1, "unit": "ops", "success": True,
            "stats": stats, "index": index or "_all",
            "primaries": primaries,
        }

    def __repr__(self):
        return "clickhouse-system-parts"


class ClickHouseClusterHealth(Runner):
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            health = await clickhouse_client.cluster.health()
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        status = health.get("status", "unknown")
        return {
            "weight": 1, "unit": "ops",
            "success": status in ("green", "yellow"),
            "cluster-status": status,
            "relocating-shards": health.get("relocating_shards", 0),
        }

    def __repr__(self):
        return "clickhouse-cluster-health"


class ClickHouseOptimizeTable(Runner):
    multi_cluster = False

    async def __call__(self, clickhouse_client, params):
        index = params.get("index")
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            shards = await clickhouse_client.indices.forcemerge(index=index)
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        return {"weight": 1, "unit": "ops", "success": True,
                "shards": shards.get("_shards", {})}

    def __repr__(self):
        return "clickhouse-optimize-table"


class ClickHouseNoOp(Runner):
    """Logs a skip and returns success. Used for OpenSearch-specific admin ops."""
    multi_cluster = False

    def __init__(self, name):
        super().__init__()
        self._name = name

    async def __call__(self, clickhouse_client, params):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            self.logger.debug("Skipping unsupported ClickHouse operation: %s", self._name)
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
        return {"weight": 1, "unit": "ops", "success": True}

    def __repr__(self):
        return f"clickhouse-noop({self._name})"
