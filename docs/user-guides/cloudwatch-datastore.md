---
layout: default
title: CloudWatch reporting datastore
parent: OSB Use Cases
nav_order: 30
---

# CloudWatch reporting datastore

OpenSearch Benchmark can ship benchmark metrics, telemetry, and test-run
documents to **Amazon CloudWatch** as an alternative to the existing
OpenSearch reporting datastore. Configuration is the same `benchmark.ini`
flow you already use — change `datastore.type = opensearch` to
`datastore.type = cloudwatch` and OSB writes EMF log events to CloudWatch
Logs, which auto-extracts the numeric values as CloudWatch Metrics.

## When to use this

- You want benchmark metrics in the **CloudWatch console** for dashboards
  and alarms.
- You don't want to operate an OpenSearch cluster solely to receive OSB
  metrics.
- Your benchmark host already has an IAM identity (EC2 instance role,
  ECS / EKS task role, SSO) and you want OSB to inherit it.

## Quick start

1. Make sure `aws sts get-caller-identity` works in your shell or on
   your benchmark host. OSB uses boto3's default credential chain.
2. Add the CloudWatch keys to `~/.osb/benchmark.ini`:

   ```ini
   [reporting]
   datastore.type     = cloudwatch
   datastore.region   = us-east-1
   datastore.namespace = OSB
   ```

3. Run a benchmark as usual:

   ```bash
   opensearch-benchmark run --workload=geonames --target-hosts=...
   ```

At the start of the run you'll see a line like:

```
[INFO] CloudWatch datastore: writing to account 123456789012 as
       arn:aws:iam::123456789012:role/OSBBenchmarkRunner in region us-east-1
```

— that's your sanity check on which AWS identity is actually being used.

After the run, open the CloudWatch console:
- **Metrics** → namespace `OSB` → metrics dimensioned by `Workload`,
  `Task`, `OperationType`, `SampleType`.
- **Logs Insights** → log group `benchmark-metrics` → query individual
  samples (e.g. `filter Workload = "geonames" | stats pct(service_time, 99)
  by Task`).

## Configuration reference

All keys live in the `[reporting]` section of `benchmark.ini`.

| Key | Default | Description |
|---|---|---|
| `datastore.type` | `opensearch` | Set to `cloudwatch` to enable this backend. |
| `datastore.region` | (from boto3 chain) | AWS region. Optional — boto3 also reads `AWS_REGION`, `AWS_DEFAULT_REGION`, or the active profile's region. |
| `datastore.namespace` | `OSB` | CloudWatch Metrics namespace. |
| `datastore.log_group.metrics` | `benchmark-metrics` | Per-sample EMF log group. |
| `datastore.log_group.test_runs` | `benchmark-test-runs` | Test-run document log group. |
| `datastore.log_group.results` | `benchmark-results` | Aggregated results log group. |
| `datastore.log_retention_days` | (none — never expires) | CloudWatch Logs retention. Must be one of CW's accepted values (1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653). |
| `datastore.profile` | (none) | Named AWS profile (`~/.aws/credentials` or `~/.aws/config`). |
| `datastore.role_arn` | (none) | If set, OSB calls `sts:AssumeRole` and uses the assumed-role creds for all subsequent calls. boto3 handles auto-refresh. |
| `datastore.cloudwatch.spool.enabled` | `true` | Spool to disk when credentials fail mid-run; replay later via `cw-replay`. Set to `false` if you'd rather fail the run on auth errors. |
| `datastore.cloudwatch.spool.dir` | `~/.osb/cw-spool` | Spool root directory. |
| `datastore.cloudwatch.spool.trigger_failures` | `3` | Consecutive auth failures before the spool engages. |
| `datastore.cloudwatch.spool.recheck_seconds` | `60` | Background `sts:GetCallerIdentity` probe interval while spooled. |

## IAM permissions

Minimum policy for write-only operation:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:PutRetentionPolicy",
      "logs:DescribeLogGroups"
    ],
    "Resource": "arn:aws:logs:*:*:log-group:benchmark-*"
  }, {
    "Effect": "Allow",
    "Action": "sts:GetCallerIdentity",
    "Resource": "*"
  }]
}
```

Add these permissions if the same role also runs `osbenchmark compare`,
`osbenchmark list test-runs`, or `osbenchmark aggregate`:

```json
{
  "Effect": "Allow",
  "Action": [
    "logs:StartQuery",
    "logs:GetQueryResults",
    "logs:StopQuery",
    "cloudwatch:GetMetricData",
    "cloudwatch:GetMetricStatistics",
    "cloudwatch:ListMetrics"
  ],
  "Resource": "*"
}
```

## How it works

**Per-request samples** become Embedded Metric Format (EMF) log events.
A single `PutLogEvents` call writes both a queryable log event (in
CloudWatch Logs) and one or more auto-extracted CloudWatch metrics. The
metric name is pivoted to a top-level JSON key; dimensions are
deliberately kept small (`Workload`, `Task`, `OperationType`,
`SampleType`) to keep CloudWatch's custom-metric count — and your bill —
bounded. Run-identity fields (`TestRunId`, `NodeName`, user tags) are
emitted as plain top-level log fields so they're queryable via Logs
Insights but do not multiply the custom-metric count.

**Telemetry payloads** (NodeStats, ShardStats, etc.) take a slightly
different path: each device emits a flattened dict with many numeric
fields, which OSB groups by prefix (`indices_*`, `jvm_*`, `os_*`, ...)
into multiple `CloudWatchMetrics` directives within a single EMF event
so each directive stays under EMF's 100-metric-per-directive cap.

**Test-run and results documents** are written as plain JSON log events
(no EMF metric extraction) to dedicated log groups, mirroring the way
the OpenSearch backend writes whole documents to dedicated indices.

**Reads** (the `osbenchmark compare`, `aggregate`, `list test-runs`
paths) are backed by CloudWatch Logs Insights queries. Note that
Insights queries take a few seconds even for small result sets — slower
than the OpenSearch backend's sub-second aggregations.

## Long-running benchmarks

For benchmarks that outlast their credential session (typically 1–12
hours), OSB has two levers:

1. **Use a renewable credential source**. Best option: run OSB on EC2
   with an IAM instance role attached. boto3 auto-rotates STS
   credentials in the background; a 24-hour benchmark needs no
   intervention.
2. **Trust the disk spool**. If credentials *do* die mid-run (e.g. an
   SSO session on a laptop expired), OSB does not stop the benchmark
   and does not lose data:
   - After 3 consecutive auth failures (configurable) the writer
     switches to spool mode: events go to
     `~/.osb/cw-spool/<test-run-id>/<log-group>.jsonl` instead of
     CloudWatch.
   - A background thread probes `sts:GetCallerIdentity` every 60s; if
     credentials come back the spool drains automatically and live
     shipping resumes.
   - If credentials stay dead until the benchmark ends, OSB finishes
     successfully (exit 0, local `test_run.json` written) and prints:

     ```
     [WARN] CloudWatch datastore: credentials expired at 14:23:01 UTC.
            Buffered 47,832 events (12.4 MB) to ~/.osb/cw-spool/<run-id>/
            After refreshing credentials, replay with:
              opensearch-benchmark cw-replay --test-run-id=<run-id>
     ```

`cw-replay` is idempotent: a `.cursor` file tracks the byte offset of
the next un-shipped record, so a crash-and-restart picks up where it
left off.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Unable to resolve AWS credentials` | boto3 can't find creds in any chain step | Run `aws sts get-caller-identity` to confirm and pick a credential source (env vars, profile, role) |
| `AccessDenied` on PutLogEvents | Role lacks `logs:PutLogEvents` on the configured log group | Attach the minimum IAM policy above |
| Empty `compare` / `list` results | Logs Insights ingest lag (events take seconds to be queryable) | Wait 10–30 s after the run finishes |
| `Multiple test runs in ...` from `cw-replay` | Spool root has more than one run directory | Pass `--test-run-id=<id>` explicitly |
| Spool file remains after `cw-replay` | At least one batch failed mid-drain | Re-run `cw-replay`; the cursor resumes from the failed record |

## Limitations vs. the OpenSearch backend

- **Read latency:** Logs Insights queries are seconds, not sub-second.
  `osbenchmark compare` feels slower.
- **Per-run dashboard filtering:** with `TestRunId` kept out of CW
  Metrics dimensions to control cardinality, console charts filter by
  time window rather than by `TestRunId` directly. Use Logs Insights
  for per-run drill-down.
- **EMF unit fidelity:** the CloudWatch Unit enum stops at
  `Microseconds`. Nanosecond and other unmapped units appear as
  `None` in CloudWatch Metrics but the original OSB unit string is
  preserved at the top-level `Unit` field for Logs Insights queries.
