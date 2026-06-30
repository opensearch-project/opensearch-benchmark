# OSB MCP server

This is an optional [Model Context Protocol](https://modelcontextprotocol.io) server that exposes OpenSearch Benchmark as typed tools. Once installed, any MCP-capable client (Claude Code, Claude Desktop, Cursor, Cline, Codex CLI) can list runs, fetch run details, compare runs, and explore the OSB operation catalog by calling tools instead of shelling out to the CLI.

Phase 1 is read-only. Running benchmarks via MCP is planned for a follow-up.

## Install

```
pip install opensearch-benchmark[mcp]
```

## Configure your MCP client

Easiest path, if you use Claude Code:

```
claude mcp add opensearch-benchmark opensearch-benchmark-mcp
```

For other clients, run the installer:

```
opensearch-benchmark-mcp install                    # auto-detect
opensearch-benchmark-mcp install --client=cursor
opensearch-benchmark-mcp install --client=cline
opensearch-benchmark-mcp install --client=claude-desktop
opensearch-benchmark-mcp install --print            # just print the config snippet
```

The installer backs up the existing config to `<path>.bak-<timestamp>` before merging the new server entry, so re-running it is safe.

Restart the client after install for the tools to load.

## Tools

| Tool | Purpose |
|---|---|
| `osb_list_runs` | List recent test runs, newest first. Optionally filter by workload. |
| `osb_get_run` | Fetch a structured per-operation summary of one run (throughput, latency p50/p90/p99/p100, service time, error rate, duration). |
| `osb_compare_runs` | Compare two runs and return per-operation deltas. |
| `osb_aggregate_runs` | Aggregate multiple runs into one synthetic run for noise smoothing. |
| `osb_list_workloads` | List workloads OSB can run. |
| `osb_list_operations` | List every OSB operation type with its hyphenated name. |

All tools are read-only. They never mutate clusters, never submit requests, and never spawn benchmark runs.

## How it works

The server is a local Python process launched as a subprocess by your MCP client. It speaks JSON-RPC over stdio (per the MCP spec) and routes tool calls to the implementations in `osbenchmark/mcp/tools/`.

Read tools read from your local `~/.benchmark/benchmarks/test-runs/` directory directly. Compare and aggregate tools shell out to the `opensearch-benchmark` CLI you already have installed; they don't reimplement that logic.

## Troubleshooting

**Tools don't appear after install.** Restart your MCP client. Most clients only load servers at startup.

**"Could not invoke `opensearch-benchmark`".** The CLI must be on PATH from wherever the MCP server runs. If you installed OSB into a venv, the MCP client also has to launch from that venv.

**Existing config has comments and the installer refuses to touch it.** The installer requires valid JSON. Either strip comments first, or use `--print` to get the snippet and add it manually.

**Config path differs on Windows / Linux.** The installer handles macOS for Claude Desktop. For other OSes use `--print` and add the snippet manually.

## For agent-side usage

If you're an AI assistant being asked to call these tools, see [`docs/agents/skills/mcp-server.md`](../../docs/agents/skills/mcp-server.md) for tool-selection guidance and example interactions.
