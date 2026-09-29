# SPDX-License-Identifier: Apache-2.0
"""
Run-inspection tools: list completed OSB test runs and fetch the
metrics summary of any one of them.

Reads from `~/.benchmark/benchmarks/test-runs/<run-id>/test_run.json`,
which is where OSB persists every run's final report. `~/.osb` is a
symlink to `~/.benchmark` so both paths resolve to the same files.
"""

import json
import os
from pathlib import Path
from typing import List, Optional


def _test_runs_dir() -> Path:
    """Resolve the test-runs directory, honoring OSB_CONFIG_DIR overrides
    used by some advanced setups."""
    override = os.environ.get("OSB_TEST_RUNS_DIR")
    if override:
        return Path(override).expanduser()
    return Path("~/.benchmark/benchmarks/test-runs").expanduser()


def register(app) -> None:
    """Register run tools on the FastMCP app."""

    @app.tool()
    def osb_list_runs(
        limit: int = 20,
        workload: Optional[str] = None,
    ) -> List[dict]:
        """
        List recent OSB test runs in reverse-chronological order, newest
        first. Each row gives enough info to identify and compare runs:
        run id, timestamp, workload, test procedure, pipeline, and any
        user tags applied at run time.

        limit: maximum number of runs to return. Default 20.
        workload: filter to a single workload (e.g., "big5", "geonames").
            Omit to return runs across all workloads.
        """
        runs = _load_all_runs()
        if workload:
            runs = [r for r in runs if r.get("workload") == workload]
        runs.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
        return runs[: max(0, limit)]

    @app.tool()
    def osb_get_run(run_id: str) -> dict:
        """
        Return a structured summary of one OSB test run by id. Includes
        per-operation throughput, latency percentiles (p50/p99/p100),
        service time, error rate, and duration.

        The raw test_run.json file is large (100KB+); this tool extracts
        the LLM-useful subset. For the full file, read the path returned
        in the result's `source_path` field.

        run_id: UUID of the run, as printed by OSB at run completion and
            shown by osb_list_runs.
        """
        run_dir = _test_runs_dir() / run_id
        path = run_dir / "test_run.json"
        if not path.exists():
            raise FileNotFoundError(
                f"No test_run.json found for run id {run_id}. "
                f"Looked under {run_dir}. "
                f"Use osb_list_runs to see available run ids."
            )
        with path.open() as f:
            raw = json.load(f)
        return _summarize_run(raw, source_path=str(path))


def _load_all_runs() -> List[dict]:
    """Walk the test-runs directory and return a summary row per run."""
    root = _test_runs_dir()
    if not root.exists():
        return []
    rows: List[dict] = []
    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        path = entry / "test_run.json"
        if not path.exists():
            continue
        try:
            with path.open() as f:
                raw = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        rows.append(_summarize_row(raw, run_id=entry.name))
    return rows


def _summarize_row(raw: dict, run_id: str) -> dict:
    """Shape one test_run.json into a listing row."""
    cluster = raw.get("cluster") or {}
    return {
        "run_id": raw.get("test-run-id") or run_id,
        "timestamp": raw.get("test-run-timestamp"),
        "workload": raw.get("workload"),
        "test_procedure": raw.get("test_procedure"),
        "pipeline": raw.get("pipeline"),
        "user_tags": raw.get("user-tags") or {},
        "distribution_version": cluster.get("distribution-version"),
        "distribution_flavor": cluster.get("distribution-flavor"),
    }


def _summarize_run(raw: dict, source_path: str) -> dict:
    """Shape one test_run.json into a full-detail summary."""
    base = _summarize_row(raw, run_id=raw.get("test-run-id") or "")
    base["source_path"] = source_path
    base["operations"] = _summarize_op_metrics(
        raw.get("results", {}).get("op_metrics") or []
    )
    return base


def _summarize_op_metrics(op_metrics: list) -> List[dict]:
    """Extract the LLM-useful metric fields per operation."""
    out: List[dict] = []
    for op in op_metrics:
        if not isinstance(op, dict):
            continue
        out.append(
            {
                "operation": op.get("operation"),
                "task": op.get("task"),
                "throughput": _scalar_metric(op.get("throughput")),
                "latency": _percentile_metric(op.get("latency")),
                "service_time": _percentile_metric(op.get("service_time")),
                "error_rate": op.get("error_rate"),
                "duration_seconds": op.get("duration"),
            }
        )
    return out


def _scalar_metric(metric) -> Optional[dict]:
    """Throughput-style metric: min/mean/median/max/unit."""
    if not isinstance(metric, dict):
        return None
    return {
        "min": metric.get("min"),
        "mean": metric.get("mean"),
        "median": metric.get("median"),
        "max": metric.get("max"),
        "unit": metric.get("unit"),
    }


def _percentile_metric(metric) -> Optional[dict]:
    """Latency/service-time metric. Keep mean + commonly-used percentiles."""
    if not isinstance(metric, dict):
        return None
    return {
        "mean": metric.get("mean"),
        "p50": metric.get("50_0"),
        "p90": metric.get("90_0"),
        "p99": metric.get("99_0"),
        "p99_9": metric.get("99_9"),
        "p100": metric.get("100_0"),
        "unit": metric.get("unit"),
    }
