# SPDX-License-Identifier: Apache-2.0
"""Shared fixtures for MCP tool tests."""

import json
import shutil
from pathlib import Path

import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fake_test_runs_dir(tmp_path, monkeypatch):
    """
    Build a temporary directory shaped like ~/.benchmark/benchmarks/test-runs/
    with the two fixture runs in it. Point the MCP tools at this dir via
    OSB_TEST_RUNS_DIR.
    """
    runs_root = tmp_path / "test-runs"
    runs_root.mkdir()

    for name in ("test_run_a.json", "test_run_b.json"):
        raw = json.loads((FIXTURES_DIR / name).read_text())
        run_id = raw["test-run-id"]
        run_dir = runs_root / run_id
        run_dir.mkdir()
        (run_dir / "test_run.json").write_text(json.dumps(raw))

    monkeypatch.setenv("OSB_TEST_RUNS_DIR", str(runs_root))
    return runs_root
