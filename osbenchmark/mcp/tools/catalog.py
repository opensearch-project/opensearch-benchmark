# SPDX-License-Identifier: Apache-2.0
"""
Catalog tools: enumerate workloads OSB knows about and operation types OSB
supports. Useful for LLM-driven exploration of "what can I run?" and
"what operation types could I use when authoring a workload?"
"""

import json
import subprocess
from typing import List, Optional

from osbenchmark.workload.workload import OperationType


def register(app) -> None:
    """Register catalog tools on the FastMCP app."""

    @app.tool()
    def osb_list_operations() -> List[dict]:
        """
        List every OSB operation type, with its hyphenated name (what
        appears in a workload's `operation-type` field) and whether it is
        an administrative operation (admin ops are not normally part of
        measured throughput).

        Use this when authoring a workload to discover which operation
        types are available. The returned list reflects the OSB version
        currently installed; new operations show up here as soon as they
        are added to OSB.
        """
        return [
            {
                "name": op.name,
                "hyphenated_name": op.to_hyphenated_string(),
                "admin_op": op.admin_op,
            }
            for op in OperationType
        ]

    @app.tool()
    def osb_list_workloads(timeout_seconds: int = 30) -> List[dict]:
        """
        List workloads OSB can run. Returns each workload's name and a
        short description.

        This wraps `opensearch-benchmark list workloads` and inherits its
        view of which workloads are available locally (anything in
        `~/.benchmark/workloads/` plus the published default set).

        timeout_seconds: how long to wait for the underlying CLI before
        giving up. Increase if your workloads repo is on slow storage.
        """
        try:
            result = subprocess.run(
                ["opensearch-benchmark", "list", "workloads", "--format=json"],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except FileNotFoundError as e:
            raise RuntimeError(
                "Could not invoke `opensearch-benchmark`. Make sure it is on "
                "PATH and that you have run `pip install opensearch-benchmark[mcp]`."
            ) from e
        except subprocess.TimeoutExpired as e:
            raise RuntimeError(
                f"`opensearch-benchmark list workloads` timed out after "
                f"{timeout_seconds}s."
            ) from e

        if result.returncode != 0:
            # OSB doesn't ship --format=json on list yet; fall back to
            # text parsing.
            return _parse_text_workload_list(_run_text_workloads(timeout_seconds))

        try:
            payload = json.loads(result.stdout)
            return _normalize_workloads(payload)
        except json.JSONDecodeError:
            return _parse_text_workload_list(result.stdout)


def _run_text_workloads(timeout_seconds: int) -> str:
    result = subprocess.run(
        ["opensearch-benchmark", "list", "workloads"],
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"`opensearch-benchmark list workloads` failed (exit "
            f"{result.returncode}): {result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout


def _parse_text_workload_list(text: str) -> List[dict]:
    """
    Parse `osb list workloads` text output. The CLI prints a tabulated
    table with at least a Name column and optionally a Description
    column; we extract those.

    Defensive: skips header/divider lines, ignores blanks, and never
    raises on a format change. If columns can't be located, falls back
    to one name per data row.
    """
    rows: List[dict] = []
    seen_header = False
    name_col_start: Optional[int] = None
    desc_col_start: Optional[int] = None

    def is_divider(line: str) -> bool:
        # Divider rows are non-empty and contain only dashes, equals,
        # or spaces (the spaces are gaps between columns).
        s = line.strip()
        return bool(s) and set(s) <= {"-", "=", " "}

    for line in text.splitlines():
        if not line.strip():
            continue
        if is_divider(line):
            continue
        if not seen_header:
            lower = line.lower()
            if "name" in lower:
                seen_header = True
                name_col_start = lower.find("name")
                desc_col_start = lower.find("description")
                if desc_col_start == -1:
                    desc_col_start = None
            continue
        if name_col_start is None:
            rows.append({"name": line.strip(), "description": None})
            continue
        if desc_col_start is None:
            name = line[name_col_start:].strip()
            description = None
        else:
            name = line[name_col_start:desc_col_start].strip()
            description = line[desc_col_start:].strip() or None
        if name:
            rows.append({"name": name, "description": description})
    return rows


def _normalize_workloads(payload) -> List[dict]:
    """Normalize whatever shape `osb list workloads --format=json` returns
    into the simple {name, description} contract this tool promises."""
    if isinstance(payload, list):
        return [_normalize_one(item) for item in payload]
    if isinstance(payload, dict) and "workloads" in payload:
        return [_normalize_one(item) for item in payload["workloads"]]
    return []


def _normalize_one(item) -> dict:
    if isinstance(item, str):
        return {"name": item, "description": None}
    if not isinstance(item, dict):
        return {"name": str(item), "description": None}
    return {
        "name": item.get("name") or item.get("workload"),
        "description": item.get("description"),
    }
