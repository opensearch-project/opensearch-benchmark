# Agent playbooks

This directory contains playbooks for AI coding assistants (Claude, Codex, Cursor, Aider, local models, etc.) that need to run OpenSearch Benchmark workflows end-to-end: not just edit the OSB codebase.

For repository conventions (build, test, code style, PR guidelines), see [AGENTS.md](../../AGENTS.md) at the repo root.

## When to use what

- **Editing OSB source code:** read [AGENTS.md](../../AGENTS.md). You usually don't need anything here.
- **Running a benchmark against a cluster:** start with [`skills/run-benchmark.md`](skills/run-benchmark.md).
- **Comparing two test runs:** [`skills/compare-runs.md`](skills/compare-runs.md).
- **Spinning up a target cluster on AWS:** [`skills/provision-target.md`](skills/provision-target.md).
- **Using OSB through MCP tools** (when available in your LLM client): [`skills/mcp-server.md`](skills/mcp-server.md).
- **Connecting to EC2 instances or refreshing AWS credentials:** [`reference/aws-access.md`](reference/aws-access.md).
- **Diagnosing a benchmark that failed or produced weird results:** [`reference/troubleshooting.md`](reference/troubleshooting.md).

## Structure

```
docs/agents/
├── README.md              # this file
├── skills/                # task-oriented playbooks ("how do I do X end-to-end")
│   ├── run-benchmark.md
│   ├── compare-runs.md
│   └── provision-target.md
└── reference/             # cross-cutting context for multiple skills
    ├── aws-access.md
    └── troubleshooting.md
```

Each skill is a self-contained markdown file. It states the goal, lists prerequisites, gives the exact commands to run, and explains what success looks like. Anything account-specific (instance IDs, profile names, IP addresses) is left as a placeholder: the user invoking the agent supplies their own values.

## Contributing

These playbooks are most useful when they reflect what actually works today. If a skill is wrong or stale, fix it in the same PR as the code change. If you discover a new pattern worth keeping, add a skill rather than burying it in a commit message.

Keep skills short. If a playbook grows past ~150 lines, it's probably trying to do two things: split it.
