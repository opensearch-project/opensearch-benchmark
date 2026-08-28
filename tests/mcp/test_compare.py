# SPDX-License-Identifier: Apache-2.0
"""Tests for compare.py (osb_compare_runs, osb_aggregate_runs)."""

from osbenchmark.mcp.tools import compare


def test_parse_compare_csv_round_trips():
    csv_text = (
        "Metric,Task,Baseline,Contender,Diff,Unit\n"
        "Min Throughput,index-append,100,95,-5,docs/s\n"
        "Mean Throughput,index-append,110.5,105.2,-5.3,docs/s\n"
        "50th percentile latency,index-append,50,52,2,ms\n"
        "Error rate,index-append,0.0,0.001,0.001,%\n"
    )
    rows = compare._parse_compare_csv(csv_text)
    assert len(rows) == 4
    assert rows[0] == {
        "metric": "Min Throughput",
        "task": "index-append",
        "baseline": 100,
        "contender": 95,
        "diff": -5,
        "unit": "docs/s",
    }
    # float parsing for fractional values
    assert rows[1]["baseline"] == 110.5
    assert rows[1]["diff"] == -5.3
    # tiny floats survive
    assert rows[3]["diff"] == 0.001


def test_parse_compare_csv_handles_empty_diff():
    csv_text = (
        "Metric,Task,Baseline,Contender,Diff,Unit\n"
        "Some metric,index-append,N/A,N/A,,docs/s\n"
    )
    rows = compare._parse_compare_csv(csv_text)
    assert rows[0]["baseline"] == "N/A"
    assert rows[0]["diff"] is None


def test_maybe_number_parses_ints_and_floats():
    assert compare._maybe_number("42") == 42
    assert compare._maybe_number("3.14") == 3.14
    assert compare._maybe_number("1e-3") == 0.001
    assert compare._maybe_number("not-a-number") == "not-a-number"
    assert compare._maybe_number("") is None
    assert compare._maybe_number(None) is None


def test_aggregate_extracts_new_run_id_from_output(monkeypatch):
    # Simulate `osb aggregate` stdout that includes the new run id.
    fake_stdout = (
        "[INFO] Aggregating runs\n"
        "[INFO] New aggregated test_run: 11111111-2222-3333-4444-555555555555\n"
    )
    def fake_run(cmd, timeout_seconds):
        return fake_stdout

    monkeypatch.setattr(compare, "_run_subprocess", fake_run)

    # Drive the registered tool through a minimal app stub.
    class _App:
        def __init__(self):
            self.tools = {}

        def tool(self):
            def wrap(fn):
                self.tools[fn.__name__] = fn
                return fn
            return wrap

    app = _App()
    compare.register(app)
    result = app.tools["osb_aggregate_runs"](["aaa", "bbb"])
    assert result["new_run_id"] == "11111111-2222-3333-4444-555555555555"
    assert result["source_runs"] == ["aaa", "bbb"]


def test_aggregate_rejects_empty_run_list(monkeypatch):
    monkeypatch.setattr(compare, "_run_subprocess", lambda *a, **kw: "")

    class _App:
        def __init__(self):
            self.tools = {}

        def tool(self):
            def wrap(fn):
                self.tools[fn.__name__] = fn
                return fn
            return wrap

    app = _App()
    compare.register(app)
    try:
        app.tools["osb_aggregate_runs"]([])
    except ValueError as e:
        assert "at least one run id" in str(e)
    else:
        raise AssertionError("expected ValueError for empty run_ids")
