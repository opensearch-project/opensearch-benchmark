# SPDX-License-Identifier: Apache-2.0
"""Tests for catalog.py (osb_list_operations, osb_list_workloads)."""

from osbenchmark.mcp.tools import catalog
from osbenchmark.workload.workload import OperationType


def test_list_operations_text_parser_extracts_rows():
    text = """
Available workloads:

Name              Description
----              -----------
big5              Big5 mixed query/index workload
geonames          Geonames workload from rally
http_logs         HTTP logs workload
"""
    rows = catalog._parse_text_workload_list(text)
    names = [r["name"] for r in rows]
    assert names == ["big5", "geonames", "http_logs"]
    assert rows[1]["description"] == "Geonames workload from rally"


def test_list_operations_text_parser_tolerates_no_description_column():
    text = """
Name
----
big5
geonames
"""
    rows = catalog._parse_text_workload_list(text)
    assert {r["name"] for r in rows} == {"big5", "geonames"}


def test_normalize_workloads_handles_listed_dicts():
    payload = [
        {"name": "big5", "description": "mixed"},
        {"name": "geonames", "description": None},
    ]
    out = catalog._normalize_workloads(payload)
    assert out == payload


def test_normalize_workloads_handles_wrapped_payload():
    payload = {"workloads": [{"name": "big5"}, "geonames"]}
    out = catalog._normalize_workloads(payload)
    assert out == [
        {"name": "big5", "description": None},
        {"name": "geonames", "description": None},
    ]


def test_operations_enum_to_hyphenated():
    # Smoke test: every operation type produces a hyphenated name without
    # crashing. This is the data that osb_list_operations returns.
    rows = [
        {
            "name": op.name,
            "hyphenated_name": op.to_hyphenated_string(),
            "admin_op": op.admin_op,
        }
        for op in OperationType
    ]
    assert len(rows) > 50  # OSB has ~59 operation types
    assert any(r["hyphenated_name"] == "bulk" for r in rows)
    assert any(r["hyphenated_name"] == "create-snapshot" and r["admin_op"] for r in rows)
