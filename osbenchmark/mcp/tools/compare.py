# SPDX-License-Identifier: Apache-2.0
"""
Comparison and aggregation tools. Wraps `osb compare` and `osb aggregate`
as subprocesses, then parses their CSV output into structured rows.

CSV is used in preference to the default markdown so we don't have to
parse a tabulated table; OSB's CSV output has stable columns:
Metric, Task, Baseline, Contender, Diff, Unit.
"""

import csv
import io
import re
import subprocess
import tempfile
from pathlib import Path
from typing import List


_RUN_ID_RE = re.compile(r"\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b")


def register(app) -> None:
    """Register compare/aggregate tools on the FastMCP app."""

    @app.tool()
    def osb_compare_runs(
        baseline: str,
        contender: str,
        timeout_seconds: int = 60,
    ) -> List[dict]:
        """
        Compare two OSB test runs and return per-operation deltas.

        Each returned row is {metric, task, baseline, contender, diff,
        unit}, matching the columns of `osb compare --results-format=csv`.
        For throughput metrics, contender > baseline is an improvement;
        for latency / service-time / error-rate metrics, contender <
        baseline is an improvement.

        baseline: run id of the baseline (the run you are comparing
            against).
        contender: run id of the contender (the run you are evaluating).
        timeout_seconds: how long to wait for `osb compare` before giving
            up. Comparisons should be fast; bump only if needed.
        """
        with tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False
        ) as tmp:
            results_path = Path(tmp.name)
        try:
            cmd = [
                "opensearch-benchmark",
                "compare",
                f"--baseline={baseline}",
                f"--contender={contender}",
                "--results-format=csv",
                f"--results-file={results_path}",
            ]
            _run_subprocess(cmd, timeout_seconds)
            return _parse_compare_csv(results_path.read_text())
        finally:
            try:
                results_path.unlink()
            except FileNotFoundError:
                pass

    @app.tool()
    def osb_aggregate_runs(
        run_ids: List[str],
        timeout_seconds: int = 120,
    ) -> dict:
        """
        Aggregate multiple OSB test runs into a single synthetic run.
        Returns the new aggregated run id, which you can then pass to
        osb_get_run or osb_compare_runs to smooth out run-to-run noise.

        run_ids: list of UUIDs to aggregate. All runs should target the
            same workload and test procedure; mixing across workloads is
            not meaningful.
        timeout_seconds: how long to wait for `osb aggregate`. Default
            120s covers most cases.
        """
        if not run_ids:
            raise ValueError("osb_aggregate_runs requires at least one run id.")
        joined = ",".join(run_ids)
        cmd = ["opensearch-benchmark", "aggregate", f"--test-runs={joined}"]
        output = _run_subprocess(cmd, timeout_seconds)

        # `osb aggregate` prints the new run id somewhere in stdout. Be
        # tolerant of phrasing changes: grep for any UUID that is NOT one
        # of the inputs.
        candidates = set(_RUN_ID_RE.findall(output)) - set(run_ids)
        new_run_id = next(iter(candidates), None)
        return {
            "new_run_id": new_run_id,
            "source_runs": run_ids,
            "raw_output": output,
        }


def _run_subprocess(cmd: list, timeout_seconds: int) -> str:
    """Run an OSB CLI subcommand. Returns combined stdout/stderr text on
    success; raises RuntimeError on failure."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            "Could not invoke `opensearch-benchmark`. Make sure it is on "
            "PATH and that the [mcp] extra is installed."
        ) from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(
            f"`{' '.join(cmd)}` timed out after {timeout_seconds}s."
        ) from e

    if result.returncode != 0:
        raise RuntimeError(
            f"`{' '.join(cmd)}` failed (exit {result.returncode}). "
            f"stderr: {result.stderr.strip()}"
        )
    return result.stdout + result.stderr


def _parse_compare_csv(text: str) -> List[dict]:
    """
    Parse the CSV that `osb compare --results-format=csv` writes. The
    file has a header row: Metric, Task, Baseline, Contender, Diff, Unit.
    """
    rows: List[dict] = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        rows.append(
            {
                "metric": row.get("Metric"),
                "task": row.get("Task"),
                "baseline": _maybe_number(row.get("Baseline")),
                "contender": _maybe_number(row.get("Contender")),
                "diff": _maybe_number(row.get("Diff")),
                "unit": row.get("Unit"),
            }
        )
    return rows


def _maybe_number(value):
    """Convert numeric strings to int/float; leave non-numeric alone.
    OSB sometimes emits empty strings, 'N/A', or formatted numbers; pass
    those through as-is rather than crash."""
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        if "." in s or "e" in s.lower():
            return float(s)
        return int(s)
    except ValueError:
        return s
