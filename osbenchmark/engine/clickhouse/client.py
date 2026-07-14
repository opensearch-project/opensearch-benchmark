# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

"""ClickHouse database client for OpenSearch Benchmark.

See the sibling engine/vespa/client.py for the pattern this file follows.
"""

from __future__ import annotations  # ensure all type hints are stringified

import asyncio
import json as _json
import logging
import os
from typing import Any, Dict, List, Optional

from osbenchmark import exceptions
from osbenchmark.context import RequestContextHolder

try:
    import clickhouse_connect  # type: ignore
    CLICKHOUSE_CONNECT_AVAILABLE = True
except ImportError:
    clickhouse_connect = None  # type: ignore
    CLICKHOUSE_CONNECT_AVAILABLE = False


class _NodesProxy:
    """Async proxy for the ``client.nodes`` namespace.

    Both ``stats`` and ``info`` are async because OSB's default NodeStats runner
    does ``await opensearch.nodes.stats(...)``. Returning a coroutine from a sync
    method (or awaiting a sync method) would raise TypeError when telemetry
    fires. Both return OS-shaped envelopes (with a populated ``nodes`` key) so
    downstream iterators don't KeyError.
    """

    def __init__(self, parent: "ClickHouseDatabaseClient") -> None:
        self._parent = parent

    async def stats(self, *args: Any, **kwargs: Any) -> Dict:
        return self._parent.nodes_stats(*args, **kwargs)

    async def info(self, *_args: Any, **_kwargs: Any) -> Dict:
        return {
            "nodes": {},
            "cluster_name": self._parent.client_options.get("cluster_name", "clickhouse"),
            "cluster_uuid": "clickhouse",
        }


class ClickHouseDatabaseClient(RequestContextHolder):
    """Async ClickHouse client that duck-types the OSB engine client interface.

    Follows the Vespa pattern: self.indices = self, self.cluster = self, etc.,
    so runners can call client.indices.create(...) and resolve to methods on
    this class directly. Inherits from RequestContextHolder (same as
    VespaDatabaseClient) so OSB can attach request lifecycle hooks; multi-engine
    does NOT have a DatabaseClient ABC to satisfy, so duck-typing is sufficient
    for the runner interface.
    """

    def __init__(self, hosts: List[Dict], client_options: Dict) -> None:
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self._hosts = hosts
        self.client_options = client_options or {}
        self._client = None  # clickhouse_connect.AsyncClient, initialized lazily
        self._sync_client = None  # clickhouse_connect.Client, for info()/wait_for_client
        self._client_lock = asyncio.Lock()  # prevents duplicate init under contention
        self._database: Optional[str] = None
        self._stats_caveat_logged = False

        # namespace proxies (Vespa pattern). nodes uses a sync proxy so telemetry
        # devices can call it synchronously.
        self.indices = self
        self.cluster = self
        self.transport = self
        self.nodes = _NodesProxy(self)

    # -----------------------------------------------------------------
    # Client init / lifecycle
    # -----------------------------------------------------------------

    async def _ensure_client(self):
        # Fast path: no lock needed if client already exists
        if self._client is not None:
            return self._client
        async with self._client_lock:
            # Double-check inside lock - another task may have initialized while we waited
            if self._client is not None:
                return self._client
            if not CLICKHOUSE_CONNECT_AVAILABLE:
                raise exceptions.SystemSetupError(
                    "clickhouse_connect not installed. Install with: "
                    "pip install 'opensearch-benchmark[clickhouse]'"
                )
            # Defensive API check (pyvespa lesson): fail loud if SDK is missing get_async_client
            if not hasattr(clickhouse_connect, "get_async_client"):
                raise exceptions.SystemSetupError(
                    "Installed clickhouse-connect is missing get_async_client - expected >=0.9.2"
                )
            from osbenchmark.engine.clickhouse.helpers import parse_hosts  # pylint: disable=import-outside-toplevel
            host, port, secure = parse_hosts(self._hosts)
            username = self.client_options.get(
                "basic_auth_user", self.client_options.get("username", "default"))
            password = self.client_options.get(
                "basic_auth_password", self.client_options.get("password", ""))
            database = self.client_options.get("database", "default")
            # Default settings ensure durable writes. Users can opt into fire-and-forget
            # via client_options["async_insert_fire_and_forget"] = True.
            default_settings = {"async_insert": 1, "wait_for_async_insert": 1}
            if self.client_options.get("async_insert_fire_and_forget"):
                default_settings["wait_for_async_insert"] = 0
                self.logger.warning(
                    "async_insert_fire_and_forget=True: bulk throughput numbers will not "
                    "reflect durable writes; data may not be queryable immediately after "
                    "bulk completes."
                )
            self._client = await clickhouse_connect.get_async_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=secure,
                connect_timeout=self.client_options.get("connect_timeout", 10),
                send_receive_timeout=self.client_options.get("request_timeout", 300),
                compress=self.client_options.get("compress", True),
                verify=self.client_options.get("ssl_verify", True),
                settings=default_settings,
            )
            self._database = database
            return self._client

    def _ensure_sync_client(self):
        if self._sync_client is not None:
            return self._sync_client
        if not CLICKHOUSE_CONNECT_AVAILABLE:
            raise exceptions.SystemSetupError(
                "clickhouse_connect not installed. Install with: "
                "pip install 'opensearch-benchmark[clickhouse]'"
            )
        from osbenchmark.engine.clickhouse.helpers import parse_hosts  # pylint: disable=import-outside-toplevel
        host, port, secure = parse_hosts(self._hosts)
        self._sync_client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=self.client_options.get(
                "basic_auth_user", self.client_options.get("username", "default")),
            password=self.client_options.get(
                "basic_auth_password", self.client_options.get("password", "")),
            database=self.client_options.get("database", "default"),
            secure=secure,
            connect_timeout=self.client_options.get("connect_timeout", 10),
            send_receive_timeout=self.client_options.get("request_timeout", 300),
            compress=self.client_options.get("compress", True),
            verify=self.client_options.get("ssl_verify", True),
        )
        return self._sync_client

    @property
    def endpoint(self) -> str:
        from osbenchmark.engine.clickhouse.helpers import parse_hosts  # pylint: disable=import-outside-toplevel
        host, port, secure = parse_hosts(self._hosts)
        scheme = "https" if secure else "http"
        # IPv6 addresses contain colons; wrap in brackets so the URL is unambiguous.
        if ":" in host:
            host = f"[{host}]"
        return f"{scheme}://{host}:{port}"

    # -----------------------------------------------------------------
    # Public escape hatch for runners
    # -----------------------------------------------------------------

    async def execute_query(self, sql: str, parameters: Optional[Dict] = None,
                            settings: Optional[Dict] = None) -> Any:
        """Public wrapper around AsyncClient.query.

        Runners that need raw QueryResult objects (VectorSearch for scoring,
        ScrollSearch for pagination) call this instead of reaching into
        _ensure_client(). Enables instrumentation subclasses to intercept
        query execution.
        """
        from osbenchmark.engine.clickhouse.helpers import coerce_parameters  # pylint: disable=import-outside-toplevel
        client = await self._ensure_client()
        if parameters is not None:
            parameters = coerce_parameters(parameters)
        if settings:
            return await client.query(sql, parameters=parameters, settings=settings)
        if parameters is not None:
            return await client.query(sql, parameters=parameters)
        return await client.query(sql)

    # -----------------------------------------------------------------
    # Sync stub for telemetry
    # -----------------------------------------------------------------

    def nodes_stats(self, **_kwargs: Any) -> Dict:
        """Sync stub. Real Node telemetry is deferred to v2.

        Called synchronously by OSB telemetry devices (NodeStats). Returns an
        empty envelope with an OS-shaped 'nodes' key so callers that iterate
        over it don't KeyError.
        """
        return {"nodes": {}, "cluster_name": self.client_options.get("cluster_name", "clickhouse")}

    # -----------------------------------------------------------------
    # Top-level engine client interface
    # -----------------------------------------------------------------

    async def bulk(self, body: Any, index: Optional[str] = None,
                   doc_type: Any = None, params: Optional[Dict] = None,
                   **kwargs: Any) -> Dict:
        from osbenchmark.engine.clickhouse.helpers import (  # pylint: disable=import-outside-toplevel
            parse_bulk_body, rows_from_docs, docs_have_extra_keys, _ns_to_ms
        )
        client = await self._ensure_client()
        docs = parse_bulk_body(body)
        if not docs:
            return {"took": 0, "errors": False, "items": []}

        columns = (params or {}).get("column-names") or kwargs.get("column_names")
        insert_mode = (params or {}).get("insert-mode", "auto")  # "native" | "json" | "auto"
        use_json_fallback = (
            insert_mode == "json"
            or not columns
            or docs_have_extra_keys(docs, columns)
        )

        if use_json_fallback:
            # JSONEachRow fallback - schema-flexible, ignores missing/extra columns
            ndjson = "\n".join(_json.dumps(d.get("_source", {})) for d in docs).encode("utf-8")
            try:
                table_expr = index  # workload passes the table name; may include db.table
                summary = await client.command(
                    f"INSERT INTO {table_expr} FORMAT JSONEachRow",
                    data=ndjson,
                    settings={
                        "input_format_skip_unknown_fields": 1,
                        "input_format_null_as_default": 1,
                    },
                )
            except Exception as exc:  # pylint: disable=broad-except
                # ClickHouse INSERT is all-or-nothing; DO NOT fabricate per-doc errors.
                self.logger.warning("ClickHouse bulk (JSONEachRow) failed: %s", exc)
                return {"took": 0, "errors": True, "items": [], "_bulk_error": str(exc)}
        else:
            # Fast Native-format path (strict - errors if any doc missing a declared column)
            try:
                rows = rows_from_docs(docs, columns, strict=True)
                summary = await client.insert(index, rows, column_names=columns)
            except exceptions.BenchmarkError:
                raise  # strict mismatch - surface the clean column-name error
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.warning("ClickHouse bulk (Native) failed: %s", exc)
                return {"took": 0, "errors": True, "items": [], "_bulk_error": str(exc)}

        # Success path: summary is QuerySummary; summary.summary is Dict[str, str]
        summary_dict = getattr(summary, "summary", {}) if summary else {}
        took = _ns_to_ms(summary_dict)
        items = [{"index": {"_index": index, "_id": d.get("_id"), "status": 201}} for d in docs]
        return {"took": took, "errors": False, "items": items}

    # pylint: disable-next=redefined-builtin,too-many-positional-arguments
    async def index(self, index: str, body: Dict, id: Any = None,
                    doc_type: Any = None, params: Optional[Dict] = None,
                    **_kwargs: Any) -> Dict:
        client = await self._ensure_client()
        # Standardized convention: read column names from params dict, same as bulk
        columns = (params or {}).get("column-names") or list(body.keys())
        row = tuple(body.get(col) for col in columns)
        await client.insert(index, [row], column_names=columns)
        return {"_index": index, "_id": id or "", "result": "created", "_version": 1}

    async def search(self, index: Optional[str] = None, body: Optional[Dict] = None,
                     doc_type: Any = None, **_kwargs: Any) -> Dict:
        from osbenchmark.engine.clickhouse.helpers import (  # pylint: disable=import-outside-toplevel
            convert_query_result_to_search_response, coerce_parameters,
        )
        client = await self._ensure_client()
        body = body or {}
        sql = body.get("sql")
        if not sql:
            raise exceptions.BenchmarkError(
                "ClickHouse search body missing 'sql' key - see "
                "docs/user-guides/clickhouse-support.md"
            )
        # Warn about unknown top-level keys (commonly happens when copy-pasting OS bodies)
        unknown = set(body.keys()) - {"sql", "parameters", "settings"}
        if unknown:
            self.logger.warning("Ignoring unknown ClickHouse search body keys: %s", sorted(unknown))

        query_params = coerce_parameters(body.get("parameters"))
        settings = body.get("settings") or None
        if settings and query_params is not None:
            result = await client.query(sql, parameters=query_params, settings=settings)
        elif query_params is not None:
            result = await client.query(sql, parameters=query_params)
        elif settings:
            result = await client.query(sql, settings=settings)
        else:
            result = await client.query(sql)
        elapsed_ns = 0
        if result.summary:
            try:
                elapsed_ns = int(result.summary.get("elapsed_ns", 0))
            except (TypeError, ValueError):
                elapsed_ns = 0
        return convert_query_result_to_search_response(
            result.result_rows, result.column_names, elapsed_ns=elapsed_ns
        )

    # -----------------------------------------------------------------
    # Indices namespace (via self.indices = self)
    # -----------------------------------------------------------------

    async def create(self, index: Optional[str] = None, body: Optional[Dict] = None,
                     params: Optional[Dict] = None, **_kwargs: Any) -> Dict:
        client = await self._ensure_client()
        body = body or {}
        ddl = body.get("ddl")
        if not ddl:
            ddl_file = body.get("ddl-file")
            if ddl_file:
                # Resolve relative paths against params["workload-path"] (OSB standard).
                # Also accept the legacy underscore variant and fall back to cwd when
                # neither is present, emitting a WARNING so users aren't surprised.
                if not os.path.isabs(ddl_file):
                    workload_path = None
                    if params:
                        workload_path = (params.get("workload-path")
                                         or params.get("workload_path"))
                    if not workload_path:
                        workload_path = os.getcwd()
                        self.logger.warning(
                            "ddl-file %r is relative but no 'workload-path' param was "
                            "supplied; resolving against cwd %r", ddl_file, workload_path
                        )
                    ddl_file = os.path.join(workload_path, ddl_file)
                with open(ddl_file, "r", encoding="utf-8") as fp:
                    ddl = fp.read()
        if not ddl:
            raise exceptions.BenchmarkError(
                f"ClickHouse CREATE TABLE for '{index}' requires a 'ddl' or 'ddl-file' key "
                f"in workload.indices[i].body"
            )
        await client.command(ddl)
        return {"acknowledged": True, "shards_acknowledged": True, "index": index}

    async def delete(self, index: Optional[str] = None, **_kwargs: Any) -> Dict:
        from osbenchmark.engine.clickhouse.helpers import quote_identifier  # pylint: disable=import-outside-toplevel
        client = await self._ensure_client()
        # Support db.table syntax
        if "." in (index or ""):
            db, tbl = index.split(".", 1)
            target = f"{quote_identifier(db)}.{quote_identifier(tbl)}"
        else:
            target = quote_identifier(index)
        await client.command(f"DROP TABLE IF EXISTS {target}")
        return {"acknowledged": True}

    async def exists(self, index: Optional[str] = None,
                     database: Optional[str] = None, **_kwargs: Any) -> bool:
        """Check table existence. Accepts explicit database kwarg or db.table syntax."""
        client = await self._ensure_client()
        # Support db.table syntax as an alternative to the database kwarg
        if database is None and "." in (index or ""):
            database, index = index.split(".", 1)
        if database is not None:
            result = await client.query(
                "SELECT count() FROM system.tables "
                "WHERE database = {db:String} AND name = {name:String}",
                parameters={"db": database, "name": index},
            )
        else:
            result = await client.query(
                "SELECT count() FROM system.tables "
                "WHERE database = currentDatabase() AND name = {name:String}",
                parameters={"name": index},
            )
        return bool(result.result_rows and result.result_rows[0][0] > 0)

    async def refresh(self, index: Optional[str] = None, **_kwargs: Any) -> Dict:
        return {"_shards": {"total": 0, "successful": 0, "failed": 0}}

    async def stats(self, index: Optional[str] = None, metric: Optional[str] = None,
                    database: Optional[str] = None, **_kwargs: Any) -> Dict:
        from osbenchmark.engine.clickhouse.helpers import build_stats_response  # pylint: disable=import-outside-toplevel
        client = await self._ensure_client()
        # Support db.table syntax
        if database is None and index and "." in index:
            database, index = index.split(".", 1)

        # Log the per-node caveat once per client
        if not self._stats_caveat_logged:
            self.logger.warning(
                "ClickHouse stats reflect single-node values from system.parts. "
                "For ReplicatedMergeTree clusters, multiply by replica count for cluster totals."
            )
            self._stats_caveat_logged = True

        if database is not None:
            base_where = "database = {db:String}"
            base_params: Dict[str, Any] = {"db": database}
        else:
            base_where = "database = currentDatabase()"
            base_params = {}
        sql = (f"SELECT ifNull(sum(rows), 0) AS rows, "
               f"ifNull(sum(bytes_on_disk), 0) AS bytes "
               f"FROM system.parts WHERE active AND {base_where}")
        if index:
            sql += " AND table = {name:String}"
            base_params["name"] = index
        if base_params:
            result = await client.query(sql, parameters=base_params)
        else:
            result = await client.query(sql)
        rows_count, bytes_on_disk = (result.result_rows[0] if result.result_rows else (0, 0))
        return build_stats_response(
            rows=int(rows_count), bytes_on_disk=int(bytes_on_disk), index_name=index
        )

    async def forcemerge(self, index: Optional[Any] = None, **_kwargs: Any) -> Dict:
        from osbenchmark.engine.clickhouse.helpers import quote_identifier  # pylint: disable=import-outside-toplevel
        client = await self._ensure_client()
        if index:
            names = index.split(",") if isinstance(index, str) else index
            for name in names:
                name = name.strip()
                if "." in name:
                    db, tbl = name.split(".", 1)
                    target = f"{quote_identifier(db)}.{quote_identifier(tbl)}"
                else:
                    target = quote_identifier(name)
                await client.command(f"OPTIMIZE TABLE {target} FINAL")
        return {"_shards": {"total": 1, "successful": 1, "failed": 0}}

    # -----------------------------------------------------------------
    # Cluster namespace
    # -----------------------------------------------------------------

    async def health(self, **_kwargs: Any) -> Dict:
        client = await self._ensure_client()
        ok = await client.ping()
        status = "green" if ok else "red"
        return {
            "cluster_name": self.client_options.get("cluster_name", "clickhouse"),
            "status": status,
            "timed_out": False,
            "number_of_nodes": 1,
            "number_of_data_nodes": 1,
            "active_primary_shards": 0,
            "active_shards": 0,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
        }

    async def put_settings(self, body: Optional[Dict] = None, **_kwargs: Any) -> Dict:
        return {"acknowledged": True}

    # -----------------------------------------------------------------
    # Transport namespace
    # -----------------------------------------------------------------

    # pylint: disable-next=too-many-positional-arguments
    async def perform_request(self, method: str, url: str, params: Any = None,
                              body: Any = None, headers: Any = None) -> Any:
        """Unsupported for ClickHouse.

        OSB's default runners (SubmitAsyncSearch, ML ops, RawRequest,
        CreatePointInTime, ...) call ``client.transport.perform_request`` with a
        REST-DSL URL. ClickHouse has no OS-shaped REST API, so a silent stub
        would fabricate success. Raise loudly instead so workloads using these
        runners fail fast and switch to a ClickHouse-native operation.
        """
        # Reference params/headers so pylint doesn't complain about unused args
        # while keeping the OSB-compatible signature.
        _ = (params, headers)
        raise exceptions.BenchmarkError(
            f"ClickHouseDatabaseClient.transport.perform_request is not supported; "
            f"the {method} {url} operation has no ClickHouse equivalent. "
            f"Use a ClickHouse-native operation."
        )

    # -----------------------------------------------------------------
    # Sync helpers
    # -----------------------------------------------------------------

    def info(self, **_kwargs: Any) -> Dict:
        # pylint: disable=import-outside-toplevel
        from osbenchmark.engine.clickhouse.helpers import parse_version
        # Narrow the expected-exception set. Anything outside these is unexpected
        # and should log at WARNING (with the exception type) so it surfaces in
        # operator logs rather than being silently masked.
        expected_exc: tuple = ()
        try:
            import clickhouse_connect.driver.exceptions as ch_exc
            expected_exc = expected_exc + (ch_exc.ClickHouseError,)
        except ImportError:
            pass
        try:
            import httpx
            expected_exc = expected_exc + (httpx.HTTPError,)
        except ImportError:
            pass
        try:
            sync = self._ensure_sync_client()
            result = sync.query("SELECT version()")
            version_str = str(result.result_rows[0][0]) if result.result_rows else ""
            semver = parse_version(version_str)
            if semver == "24.8.0" and version_str and not version_str.startswith("24.8."):
                # Distinct WARNING when fallback is triggered
                self.logger.warning(
                    "Could not parse ClickHouse version %r; using fallback %s. "
                    "Metrics store version field may be misleading.", version_str, semver
                )
        except expected_exc as exc:
            self.logger.warning(
                "ClickHouse info() failed: %s - using fallback version 24.8.0", exc
            )
            semver = "24.8.0"
        except Exception as exc:  # pylint: disable=broad-except
            # Unexpected exception - still return a fallback so metrics store
            # doesn't crash, but WARN with the exception type so it's visible.
            self.logger.warning(
                "ClickHouse info() raised unexpected %s: %s - using fallback version 24.8.0",
                type(exc).__name__, exc
            )
            semver = "24.8.0"
        return {
            "name": "clickhouse",
            "cluster_name": self.client_options.get("cluster_name", "clickhouse"),
            "cluster_uuid": "clickhouse-benchmark",
            "version": {
                "number": semver,
                "distribution": "clickhouse",
                "build_type": "release",
                "build_hash": "unknown",
            },
            "tagline": "You Know, for Analytics",
        }

    def return_raw_response(self) -> None:  # pylint: disable=arguments-differ
        return None

    # -----------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------

    async def __aenter__(self) -> "ClickHouseDatabaseClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._client is not None:
            close_method = getattr(self._client, "aclose", None) or getattr(self._client, "close", None)
            if close_method:
                try:
                    result = close_method()
                    if hasattr(result, "__await__"):
                        # Bounded timeout - clickhouse-connect close can hang on pending requests
                        await asyncio.wait_for(result, timeout=10)
                except asyncio.TimeoutError:
                    self.logger.warning("Timeout closing async ClickHouse client; forcing null")
                except Exception as exc:  # pylint: disable=broad-except
                    self.logger.warning("Error closing async ClickHouse client: %s", exc)
            self._client = None
        if self._sync_client is not None:
            try:
                self._sync_client.close()
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.warning("Error closing sync ClickHouse client: %s", exc)
            finally:
                self._sync_client = None


class ClickHouseClientFactory:
    """Factory called by osbenchmark.engine.clickhouse.create_client_factory."""

    def __init__(self, hosts: List[Dict], client_options: Dict) -> None:
        self.hosts = hosts
        self.client_options = client_options or {}

    def create(self) -> "ClickHouseDatabaseClient":
        return ClickHouseDatabaseClient(self.hosts, self.client_options)

    def create_async(self) -> "ClickHouseDatabaseClient":
        return ClickHouseDatabaseClient(self.hosts, self.client_options)

    def wait_for_rest_layer(self, max_attempts: int = 40) -> bool:
        from osbenchmark.engine.clickhouse.helpers import wait_for_clickhouse  # pylint: disable=import-outside-toplevel
        # Build a lightweight duck-typed object that wait_for_clickhouse accepts.
        ephemeral = ClickHouseDatabaseClient(self.hosts, self.client_options)
        return wait_for_clickhouse(ephemeral, max_attempts=max_attempts)
