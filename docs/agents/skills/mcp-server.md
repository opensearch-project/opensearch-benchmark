# Skill: use the OSB MCP server

Goal: use the OSB MCP tools (instead of shelling out to `opensearch-benchmark` directly) to inspect, compare, and aggregate benchmark runs.

## Prerequisites

- The MCP server is installed and registered with the client running you. If the tools below aren't available in your tool list, the user needs to install it. See [`osbenchmark/mcp/README.md`](../../../osbenchmark/mcp/README.md).
- Read access to `~/.benchmark/benchmarks/test-runs/` on the machine where the MCP server runs.
- The `opensearch-benchmark` CLI must be on PATH where the MCP server runs (compare and aggregate shell out to it).

## When to use MCP tools vs. the CLI

Prefer MCP tools when:
- You need structured data back (typed dicts, not parsed CLI text)
- You're inside an LLM tool with MCP support
- You'd otherwise be reading run JSON files by hand

Prefer the CLI when:
- You need to actually run a benchmark (Phase 1 of the MCP server is read-only)
- You're scripting from outside an LLM context
- You need a flag the MCP tool doesn't expose

## Tool catalog

| Tool | Use when |
|---|---|
| `osb_list_runs(limit, workload)` | "what did I run yesterday?" or "show me all my big5 runs" |
| `osb_get_run(run_id)` | "what were the p99 latencies for run X?" |
| `osb_compare_runs(baseline, contender)` | "did this run regress vs. last week's?" |
| `osb_aggregate_runs(run_ids)` | "smooth out noise across 3 runs before comparing" |
| `osb_list_workloads()` | "what workloads are available?" |
| `osb_list_operations()` | "what operation types could I use in a workload?" |

## Typical interaction

User asks: *"Did the 3.5 vectorsearch run regress vs. 3.4?"*

1. `osb_list_runs(limit=20, workload="vectorsearch")` to find candidate run IDs.
2. Ask the user which one is baseline and which is contender (or infer from `distribution_version` and `timestamp` fields if obvious).
3. `osb_compare_runs(baseline=<id>, contender=<id>)` to get the delta table.
4. Triage and report per [`compare-runs.md`](compare-runs.md) (±5% noise threshold, mean-vs-tail-latency framing).

User asks: *"Smooth out noise — I have three runs on each side."*

1. `osb_aggregate_runs(run_ids=[baseline-1, baseline-2, baseline-3])` → captures the new aggregated baseline ID.
2. `osb_aggregate_runs(run_ids=[contender-1, contender-2, contender-3])` → new aggregated contender ID.
3. `osb_compare_runs(baseline=<agg-baseline>, contender=<agg-contender>)` and triage as usual.

## Things to know

- Tool results are JSON-friendly Python dicts. Don't expect dot-notation; use bracket access.
- All numeric values come back as their original type (`int` for whole numbers, `float` for fractional). Unparseable strings are passed through as strings; check before doing arithmetic.
- The `service_time` and `latency` fields in `osb_get_run` use the keys `mean`, `p50`, `p90`, `p99`, `p99_9`, `p100` (not OSB's internal `50_0` / `100_0` notation).
- `osb_get_run` includes a `source_path` field. If the user wants the raw test_run.json (e.g., for a custom analysis), read from there.
- `osb_compare_runs` writes a temporary CSV file to read OSB's output. The temp file is cleaned up before the tool returns.

## Failure modes

| Symptom | Cause | Action |
|---|---|---|
| Tool returns "Could not invoke `opensearch-benchmark`" | CLI not on PATH where the MCP server runs | Ask user to verify their venv / PATH |
| `osb_get_run` raises FileNotFoundError | Run is on a different machine | Ask user where the run was produced; tools only see local runs |
| `osb_compare_runs` returns empty list | OSB CLI emitted no comparable metrics (e.g., workloads differ) | Use `osb_list_runs` to confirm workloads match |
| `osb_aggregate_runs` returns `new_run_id: None` | OSB output format changed; we couldn't extract the new ID | Check `raw_output` field for the actual stdout |
