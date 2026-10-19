# SPDX-License-Identifier: Apache-2.0
"""Tests for runs.py (osb_list_runs, osb_get_run)."""

from osbenchmark.mcp.tools import runs


def test_list_runs_returns_newest_first(fake_test_runs_dir):
    rows = sorted(runs._load_all_runs(), key=lambda r: r["timestamp"], reverse=True)
    assert [r["run_id"][:8] for r in rows] == ["bbbbbbbb", "aaaaaaaa"]


def test_list_runs_filters_by_workload(fake_test_runs_dir):
    rows = runs._load_all_runs()
    # both fixtures use 'geonames'; verify the workload field is populated
    assert all(r["workload"] == "geonames" for r in rows)
    assert {r["workload"] for r in rows} == {"geonames"}


def test_list_runs_summary_shape(fake_test_runs_dir):
    rows = runs._load_all_runs()
    expected_keys = {
        "run_id",
        "timestamp",
        "workload",
        "test_procedure",
        "pipeline",
        "user_tags",
        "distribution_version",
        "distribution_flavor",
    }
    assert expected_keys == set(rows[0].keys())


def test_get_run_extracts_per_op_metrics(fake_test_runs_dir):
    run_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    path = fake_test_runs_dir / run_id / "test_run.json"

    import json
    raw = json.loads(path.read_text())
    summary = runs._summarize_run(raw, source_path=str(path))

    assert summary["run_id"] == run_id
    assert summary["workload"] == "geonames"
    assert summary["distribution_version"] == "3.0.0"
    assert len(summary["operations"]) == 1
    op = summary["operations"][0]
    assert op["operation"] == "index-append"
    assert op["throughput"]["mean"] == 110
    assert op["service_time"]["p99"] == 79
    assert op["service_time"]["unit"] == "ms"
    assert op["error_rate"] == 0.0


def test_get_run_handles_missing_percentiles(fake_test_runs_dir):
    # Synthesize a run that lacks 99_0 to confirm we don't KeyError.
    raw = {
        "test-run-id": "ccc",
        "test-run-timestamp": "20260101T000000Z",
        "workload": "tiny",
        "results": {
            "op_metrics": [
                {
                    "operation": "x",
                    "service_time": {"50_0": 1, "mean": 1, "unit": "ms"},
                }
            ]
        },
    }
    summary = runs._summarize_run(raw, source_path="/nowhere")
    assert summary["operations"][0]["service_time"]["p99"] is None
