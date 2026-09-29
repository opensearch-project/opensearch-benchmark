# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
"""
MCP (Model Context Protocol) server for OpenSearch Benchmark.

Exposes OSB capability as typed tools that any MCP-capable client
(Claude Desktop, Claude Code, Cursor, Cline, Codex CLI, etc.) can call.

Phase 1 is read-only: list and inspect test runs, compare and aggregate
runs, list available workloads and operation types. Running benchmarks
is intentionally not included in Phase 1.

Install with: pip install opensearch-benchmark[mcp]
Run with: opensearch-benchmark-mcp
"""
