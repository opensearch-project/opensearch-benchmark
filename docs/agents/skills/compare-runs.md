# Skill: compare benchmark runs

Goal: compare two completed OSB test runs to identify regressions or improvements.

## Prerequisites

- Two completed runs whose results are accessible by OSB (same machine, or both pulled into the same `~/.osb/benchmarks/test-runs/` directory).
- The two run IDs (UUIDs). If you don't have them, list available runs with:

  ```
  opensearch-benchmark list test-runs
  ```

## The command

```
opensearch-benchmark compare \
  --baseline=<baseline-run-id> \
  --contender=<contender-run-id>
```

- `--baseline` is "what we're measuring against" (e.g., last week's run, the version on `main`).
- `--contender` is "the run we want to evaluate" (e.g., today's run, a feature branch).

OSB prints a delta table per operation: baseline value, contender value, and absolute and percent change.

## Reading the output

Each row is one metric × one operation. Columns:

| Column | Meaning |
|---|---|
| `Metric` | Throughput, service-time (latency), error-rate, etc. |
| `Operation` | The workload operation (e.g., `default`, `query-string`, `term-100`) |
| `Baseline` | Value from the baseline run |
| `Contender` | Value from the contender run |
| `Diff` | Contender minus baseline |
| `Unit` | `ops/s`, `ms`, `%` |

For throughput, higher contender is improvement. For latency, service-time, and error-rate, lower contender is improvement.

## Aggregating multiple runs

OSB benchmarks have run-to-run variance. A single comparison can mislead. To smooth noise, aggregate several runs into one synthetic run:

```
opensearch-benchmark aggregate --test-runs=<id1>,<id2>,<id3>
```

This emits a new test_run ID whose metrics are the median/mean of the input runs. Use that aggregated ID as the baseline or contender for `compare`.

## What to report back to the user

Don't dump the raw delta table. Triage it first using a single noise threshold of ±5% (OSB has real run-to-run variance, so smaller deltas are usually unreliable from a single comparison):

1. **Regressions**: metrics that got worse by more than 5% on operations the user cares about. Surface these first.
2. **Improvements**: metrics that got better by more than 5%. Useful if the user is validating a change they expected to be a win.
3. **No-ops**: operations within ±5% of baseline. One sentence is enough: "the remaining N operations are within ±5%."
4. **Anomalies**: error-rate changes, missing operations in one run but not the other, mean and p99 moving in different directions on the same operation (see "Mean vs. tail latency" below).

Thresholds are noise-floor heuristics, not statistical significance. If the user is making a ship decision, aggregate 3+ runs per side (see below) before quoting a delta.

A useful report shape:

```
Regressions vs baseline <id-prefix>:
- query-string service-time p99: 145ms → 198ms (+37%)
- term-100 throughput: 4,820 ops/s → 4,350 ops/s (-10%)

Improvements:
- range latency p50: 22ms → 18ms (-18%)

Other 23 metrics are within ±5%.
```

## Mean vs. tail latency

If mean latency and p99 move in different directions on the same operation (e.g., mean -5% but p99 +30%), report it as a tail-latency regression: the typical request is unchanged or slightly better, but the slow tail got materially worse. Common causes: GC pauses, throttling, lock contention, or other-tenant load on the contender cluster. Don't average mean and p99 into a single "this regressed / improved" verdict: flag both directions separately.

## Failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `Test run with id X not found` | Run doesn't exist or is on a different machine | Verify with `opensearch-benchmark list test-runs` |
| Two runs used different workloads/test-procedures | Comparison is meaningless | Re-run one with matching `--workload` and `--test-procedure` |
| Wildly different cluster topologies in the two runs | Comparison reflects topology, not code | Use the same cluster (or identically-provisioned ones) for both runs |
| Throughput moved but latency didn't (or vice versa) | Possible duty-cycle artifact (warmup, throttling, queueing) | Check `~/.benchmark/logs/benchmark.log` for both runs; aggregate multiple runs to confirm |
