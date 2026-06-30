# AGENTS.md

This file is for AI coding assistants (Claude, Codex, Cursor, Aider, etc.) working in this repository. It contains facts an agent needs every session: paths, commands, conventions. Human-oriented context lives in [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) and [CONTRIBUTING.md](CONTRIBUTING.md).

## Concepts

Terms used throughout the codebase and these docs:

- **Workload**: a benchmarking scenario (e.g., `geonames`, `big5`). Defines indexes, corpora, and test procedures. Workloads live in the separate [opensearch-benchmark-workloads](https://github.com/opensearch-project/opensearch-benchmark-workloads) repo.
- **Test procedure**: a named sequence of operations within a workload (e.g., `append-no-conflicts`, `query-only`). One workload can ship multiple procedures.
- **Operation**: one logical action (bulk-index, search, refresh, etc.). Each operation has a type defined in `OperationType` (`osbenchmark/workload/workload.py`).
- **Runner**: the Python class that executes one operation. Subclass of `Runner` in `osbenchmark/worker_coordinator/runner.py`.
- **Param source**: produces the parameter dict each runner invocation receives. Subclass of `ParamSource` in `osbenchmark/workload/params.py`.
- **Corpora**: the document datasets a workload ingests, declared in the workload's `corpora` section.
- **Pipeline**: top-level mode controlling whether OSB provisions a cluster (`from-sources`, `from-distribution`) or only drives an existing one (`benchmark-only`).
- **Telemetry**: optional pluggable cluster-side data collectors (`node-stats`, `recovery-stats`, `jfr`, etc.). See `osbenchmark/telemetry.py`.
- **Test run**: one end-to-end invocation of `opensearch-benchmark run`. Results land in `~/.benchmark/benchmarks/test-runs/<run-id>/test_run.json` (also reachable via the `~/.osb` symlink).

## Repository structure

OpenSearch Benchmark (OSB) is a Python macrobenchmarking framework. Key directories:

- `osbenchmark/`: the package. Major subsystems:
  - `worker_coordinator/`: orchestrates benchmark workers. `runner.py` contains the runner class for every operation type.
  - `workload/`: workload loading and parameter generation. `workload.py` defines the `OperationType` enum; `params.py` is the parameter source registry.
  - `builder/`: cluster provisioning logic. Used by `from-sources` and `from-distribution` pipelines; skipped by `benchmark-only`.
  - `client.py`: async OpenSearch client wrapper and request context manager. Read this before adding a new runner: it defines the `on_client_request_start/end` and `on_request_start/end` hooks every runner uses.
  - `test_run_orchestrator.py`: top-level orchestration entry point.
- `tests/`: unit tests. Run with `make test` or `python -m pytest tests/`.
- `it/`: integration tests. Long-running; require Docker + JDK 17/21. Run with `make it310`.
- `benchmarks/`: micro-benchmarks for OSB internals (not user-facing benchmarks).
- `scripts/`: release and CI helpers.
- `docs/agents/`: agent playbooks (running benchmarks, comparing runs, provisioning targets).

## Build and test

Python 3.10–3.13 supported, minimum 3.10.6 (see `.ci/variables.json`). `pyenv` is required.

```
make develop           # pyenv install + pip install -e .[develop]
make test              # unit tests
make lint              # pylint
make it310             # full integration suite on Python 3.10 (slow: ~20–30 min, needs Docker + JDK)
```

`make develop` installs OSB and its dev dependencies into the active Python environment via `pip install -e .`. It does not create a virtualenv. If you want isolation, create one yourself before running `make develop`:

```
python3 -m venv .venv
source .venv/bin/activate
make develop
```

To run a single test file:

```
python -m pytest tests/worker_coordinator/runner_test.py -q
python -m pytest tests/worker_coordinator/runner_test.py::TestQuery -q
```

Use `python -m pytest`, not the bare `pytest` from `pyenv` shims. The shim version often points at a Python without `cbor2` installed and produces misleading `ModuleNotFoundError`.

## Coding conventions

- Match existing style. Don't reformat unrelated code.
- 4-space indent. snake_case for functions/variables, CapCase for classes.
- Type hints: add them to new public functions and class attributes you create. When modifying inside an existing untyped function, match the surrounding style: don't leave a function half-typed. Don't sweep through unrelated modules adding hints.
- Async I/O uses `asyncio`. Runners are `async def __call__(self, opensearch, params)` on a subclass of `Runner`.
- Logging: `self.logger = logging.getLogger(__name__)`. Don't `print()`.
- Tests: `unittest.TestCase` subclasses with `async def test_*` methods using `IsolatedAsyncioTestCase` where needed. Test file naming: `<module>_test.py`. Test methods start with `test_`.

## Operation types and runners

Adding a new operation type is a 3-file change:

1. `osbenchmark/workload/workload.py`: add an `OperationType` enum entry (numeric ID must be unique). Update `from_hyphenated_string()`.
2. `osbenchmark/workload/params.py`: add a `ParamSource` subclass and register it with `register_param_source_for_operation(OperationType.YourOp, YourParamSource)` near the bottom of the file.
3. `osbenchmark/worker_coordinator/runner.py`: add a `Runner` subclass and register it inside `register_default_runners()`.

Canonical examples to copy from:

- Runner: `Query` in `runner.py` (search-style) or `BulkIndex` (bulk-style).
- ParamSource: `SearchParamSource` or `BulkIndexParamSource` in `params.py`.
- Tests: `tests/worker_coordinator/runner_test.py::TestQuery` shows the `AsyncMock` + transport-mock pattern.

Each operation must have:
- A unique hyphenated name (`OperationType.to_hyphenated_string()` derives it from the enum name)
- A 1:1 mapping in `register_default_runners()`
- Documentation in the documentation-website repo at `_benchmark/reference/workloads/operations.md`

### Runner contract

A `Runner` subclass implements `async def __call__(self, opensearch, params)`. Return shape options:

- `None` (or no return statement): framework defaults `weight=1`, `unit="ops"`, `success=True`. Most simple runners do this.
- A `dict`: must include `weight: int` and `unit: str`. Bulk-style runners also return `success: bool`, `success-count`, `error-count`, and other metadata that flows into the metrics store. See `BulkIndex` for the canonical shape.
- A `(weight, unit)` tuple is also accepted but is legacy; prefer returning a dict or `None`.

Timing hooks (from `osbenchmark/client.py`): for any runner that issues network requests outside the `opensearch` client (raw `transport.perform_request`, gRPC, custom HTTP), call `request_context_holder.on_client_request_start()` / `on_client_request_end()` around the call so OSB can measure client-observed latency. For most runners that just call `opensearch.search()` / `opensearch.bulk()` etc., the wrapper in `client.py` already handles this.

Exceptions raised from `__call__` are treated as operation failures; OSB records them and continues with the next operation. Don't catch and swallow.

## Database backends

`osbenchmark/database/` defines the pluggable-backend registry. The `DatabaseType` enum (`database/registry.py`) currently includes `OPENSEARCH`, `MILVUS`, and `VESPA`. To add a new backend:

1. Add an entry to `DatabaseType`.
2. Implement a client factory under `osbenchmark/database/clients/<name>/` following the OpenSearch factory's shape.
3. Call `register_database(DatabaseType.YOUR_BACKEND, YourClientFactory)` at module-import time.

Per-backend Python deps go in `setup.py` `extras_require`, not the base install: keep the core lean.

## Pull request conventions

- Sign-off required (DCO): `git commit -s`.
- Reference the issue in the PR body: `Closes #1234`.
- Title format: short imperative, no period. Match recent commits in `git log --oneline`.
- One commit per logical change. Squash before opening if you have noise.
- Tests required for behavior changes. Doc updates required for user-visible changes (link to the matching `documentation-website` PR).

## Common gotchas

- `~/.osb` is a symlink to `~/.benchmark` (set up the first time OSB runs). Either path resolves to the same files. Tools and docs use them interchangeably. Stale config from a previous OSB version produces confusing errors like `No value for mandatory configuration: section='reporting'`. `rm -rf ~/.osb ~/.benchmark` is safe.
- Forks: `origin` should be your fork, `upstream` should be `opensearch-project/opensearch-benchmark`. Always `git fetch upstream` before checking branch status; `git log upstream/main..HEAD` shows what's actually unmerged.
- Tests using a real cluster must use the integration test harness (`it/`). Mock OpenSearch with `unittest.mock.AsyncMock`; don't ship tests that hit real network endpoints from `tests/`.
- The `min-os-version.txt` file gates which OpenSearch versions OSB supports. Update it together with version-specific code paths.
- Workload changes don't live here: they're in `opensearch-project/opensearch-benchmark-workloads`.

## For benchmark workflows

To actually run benchmarks (locally or against AWS), see `docs/agents/`:

- [`docs/agents/skills/run-benchmark.md`](docs/agents/skills/run-benchmark.md)
- [`docs/agents/skills/compare-runs.md`](docs/agents/skills/compare-runs.md)
- [`docs/agents/skills/provision-target.md`](docs/agents/skills/provision-target.md)
- [`docs/agents/reference/aws-access.md`](docs/agents/reference/aws-access.md)
- [`docs/agents/reference/troubleshooting.md`](docs/agents/reference/troubleshooting.md)
