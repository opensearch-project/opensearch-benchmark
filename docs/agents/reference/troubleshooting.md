# Reference: troubleshooting benchmarks

When a run fails, produces wildly wrong numbers, or won't start, work through these in order. Most problems are in the first three categories.

## Logs to check first

| File | What it contains |
|---|---|
| `~/.benchmark/logs/benchmark.log` | OSB's main log. First place to look for any failure. (`~/.osb/logs/benchmark.log` is the same file via symlink.) |
| `~/.benchmark/benchmarks/test-runs/<run-id>/test_run.json` | Final results + metadata for one run. |
| stdout/stderr of the `opensearch-benchmark run` invocation | Live progress, summary table. |

If you piped output to a file (`tee ~/osb-run.log`), search that for `[ERROR]` and `Traceback` first.

## Common failure modes

### Won't start

| Symptom | Cause | Fix |
|---|---|---|
| `No value for mandatory configuration: section='reporting', key='datastore.type'` | Stale config in `~/.osb/` from a previous OSB version | `rm -rf ~/.osb ~/.benchmark` and rerun |
| `Cannot connect to <host>:<port>` | Network / security group / wrong endpoint | `curl <host>:<port>` from the OSB host first |
| `ImportError: No module named '...'` | OSB installed via pyenv shim, not venv | `source .venv/bin/activate` and verify with `which opensearch-benchmark` |
| `Could not find workload [X]` | Workload name typo, or workload repo not synced | `opensearch-benchmark list workloads` to see what's available |
| `another OSB instance is running` | Stale lock file from a previous crash | Pass `--kill-running-processes` |

### Runs but produces garbage

| Symptom | Cause | Fix |
|---|---|---|
| Throughput is zero or near-zero | Cluster overloaded, no warmup convergence, error rate spike | Check error count in summary; tail `benchmark.log` for the run; verify cluster health (`_cluster/health?pretty`) |
| Latency p99 wildly higher than p50 | GC pauses, throttling, contention from concurrent runs | Run alone on the cluster; add `--telemetry=jfr,node-stats` for next run |
| Numbers don't match a prior run | Different cluster topology, different OSB version, run-to-run noise | Compare cluster configs; use `opensearch-benchmark aggregate` over multiple runs to smooth noise |
| Operation skipped (zero count in summary) | Bad workload parameter, missing index | Grep `benchmark.log` for the operation name; look for the skip reason |
| `429 Too Many Requests` repeated | Client load exceeds cluster capacity | Lower `clients` in the workload; or scale up the cluster |

### Run completes but no results saved

| Symptom | Cause | Fix |
|---|---|---|
| Empty `test_run.json` | Run crashed during write-out (e.g., disk full) | `df -h` on the OSB host; clean `~/.osb/benchmarks/` if needed |
| Results in OSB but not in metrics datastore | Datastore unreachable or misconfigured | Check `benchmark.ini` reporting section; verify datastore credentials |
| Datastore writes succeed but values look off | Time zone / unit mismatch between datastore and viewer | Confirm both are using UTC; verify metric units (`ops/s` vs `ops/min`) |

## Diagnosing slow/stuck benchmarks

A benchmark that "isn't doing anything" is usually one of:

1. **In long warmup.** Some workloads (vectorsearch with large datasets) warm up for many minutes. Tail `benchmark.log`; look for `[INFO] Executing warmup` followed by `[INFO] Executing test`.
2. **Stuck on bulk-ingest.** Check `_cat/indices?v` on the target: is doc count climbing? If yes, just slow ingest. If no, ingest is wedged. Check cluster `_nodes/hot_threads`.
3. **Network stall.** `tcpdump` or `ss -i` between OSB host and target. If the connection is idle, OSB isn't sending requests.
4. **Client side bottleneck.** OSB workers waiting on each other or on disk I/O. Check OSB host CPU/disk usage with `top`/`iostat`.

If a run has been stuck for >5x its expected duration, kill it and dig into the log rather than waiting longer.

```
# Find the specific PID first; never blanket-pkill on a shared host
pgrep -af opensearch-benchmark
kill <pid>

# Or, if it's in a screen session
screen -S <session-name> -X quit
```

## Capturing more signal for the next run

Telemetry devices are the OSB-built-in way to grab cluster-side metrics during a run:

```
opensearch-benchmark run \
  --workload=<...> \
  --target-hosts=<...> \
  --telemetry=node-stats,recovery-stats,jfr \
  --telemetry-params='{"node-stats-include-indices":true}'
```

Full list: `opensearch-benchmark list telemetry`.

For deeper JVM analysis, `jfr` produces flight-recorder dumps you can open in JMC.

## When to escalate

- The cluster itself is misbehaving (OOM, repeated leader election, slow log noise): that's an OpenSearch issue, not OSB. Hand off to whoever owns the cluster.
- Reproducible OSB bug (specific workload + parameters always crashes): file at https://github.com/opensearch-project/opensearch-benchmark/issues with the workload, command, and log excerpt.
- Performance regression on the upstream code path: file with `git bisect` results if possible.
