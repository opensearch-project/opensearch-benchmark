# Skill: run a benchmark

Goal: run an OSB benchmark against a target cluster and capture the results.

## Prerequisites

- An OSB checkout with `make develop` already run. See `AGENTS.md` "Build and test" if not.
- A reachable OpenSearch cluster (HTTP, with auth credentials if it's secured).
- For long runs on a remote host: ability to reach that host (SSH, SSM, or other; see [`reference/aws-access.md`](../reference/aws-access.md) for AWS patterns).

Values like `<host>`, `<port>`, `<workload-name>`, and `<run-id>` are placeholders. Get them from the user. Do not invent IP addresses, run IDs, or credentials.

## Preflight

Before launching a run, confirm the cluster is reachable from wherever OSB will execute. From the OSB host:

```
curl -s <host>:<port>/_cluster/health?pretty
```

A healthy response is JSON containing `"cluster_name"` and a `"status"` of `"green"` or `"yellow"` (`"red"` means the cluster is unhealthy and a benchmark will be misleading). If the curl fails or times out, fix reachability before burning a run: see [`reference/aws-access.md`](../reference/aws-access.md) "Security groups and reachability".

## The minimum command

```
opensearch-benchmark run \
  --workload=<workload-name> \
  --target-hosts=<host>:<port> \
  --pipeline=benchmark-only
```

- `--workload`: which workload to run (`geonames`, `big5`, `nyc_taxis`, `pmc`, `vectorsearch`, `http_logs`, `so`, etc.). See [opensearch-benchmark-workloads](https://github.com/opensearch-project/opensearch-benchmark-workloads) for the full set.
- `--target-hosts`: cluster endpoint. Multiple hosts: comma-separated.
- `--pipeline=benchmark-only`: skip cluster provisioning. The cluster already exists.

For a smoke test (small dataset, fast finish), add `--test-mode`.

If the run fails with "another OSB instance is running", check first with `pgrep -af opensearch-benchmark`. If the PID belongs to someone else's work, leave it alone. Only add `--kill-running-processes` once you've confirmed the lock is stale (no live PID, or a PID you own).

## Common additions

- **Auth, basic over HTTP** (security disabled or proxy-terminated TLS): `--client-options="basic_auth_user:admin,basic_auth_password:<pw>"`
- **Auth, basic over HTTPS with self-signed cert** (typical secured cluster): `--client-options="basic_auth_user:admin,basic_auth_password:<pw>,use_ssl:true,verify_certs:false"`
- **Auth, sigv4** (AWS-managed OpenSearch): `--client-options="amazon_aws_log_in:environment,region:us-west-2,service:es"`
- **Workload params:** `--workload-params='{"number_of_shards":"1","number_of_replicas":"0"}'`
- **Local workload checkout** (e.g., to test changes to a workload before they're published): `--workload-path=/path/to/opensearch-benchmark-workloads/<workload-name>`. By default OSB clones the workloads repo to `~/.benchmark/workloads/` and uses that.
- **Specific test procedure:** `--test-procedure=<name>`. List available: `opensearch-benchmark info --workload=<workload-name>`.
- **Tag the run:** `--user-tag="purpose:baseline,branch:main"`. Tags are searchable when comparing runs later.
- **Telemetry:** `--telemetry=node-stats,recovery-stats` (full list: `opensearch-benchmark list telemetry`).

## Long-running runs

Long runs must detach from the invoking shell so the agent doesn't sit blocked for an hour. Pick one of `screen`, `tmux`, or `systemd-run --user --scope`. Plain `nohup ... &` does not survive an SSM `send-command` exit (see [`reference/aws-access.md`](../reference/aws-access.md) SSM gotchas).

Example with `screen`:

```
screen -dmS osb-run bash -c 'opensearch-benchmark run --workload=big5 --target-hosts=... --pipeline=benchmark-only 2>&1 | tee ~/osb-run.log'
```

- Tail without attaching: `tail -f ~/osb-run.log`
- Check status: `screen -ls`
- Attach to interact: `screen -r osb-run` (detach again with `Ctrl-a d`)
- Kill: `screen -S osb-run -X quit`

Pick a unique session name per run so concurrent benchmarks don't collide.

## What success looks like

A successful run ends with a summary table printed to stdout and a `test_run.json` written to:

```
~/.benchmark/benchmarks/test-runs/<run-id>/test_run.json
```

(`~/.osb` is a symlink to `~/.benchmark` and resolves to the same file.)

`<run-id>` is a UUID printed near the top of the OSB output. Save it: it's what `compare-runs.md` needs.

A few signals that a run is healthy mid-flight (when tailing the log):
- "[INFO] Executing test on" appears after warmup
- Throughput and latency numbers are non-zero and not strictly decreasing
- No repeated `[ERROR]` lines about connection refused, timeouts, or 429s

## Failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `Cannot connect to <host>:<port>` | Network/SG blocks OSB host from target | Verify with `curl <host>:<port>` from the OSB host first |
| `No value for mandatory configuration: section='reporting'` | Stale config in `~/.osb/` or `~/.benchmark/` | `rm -rf ~/.osb ~/.benchmark` and rerun |
| 401/403 from target | Missing or wrong auth | Add `--client-options` with the right credentials |
| Run completes but result table is empty | Workload skipped due to a bad parameter | Check `~/.benchmark/logs/benchmark.log` for skipped operations |

For deeper triage see [`reference/troubleshooting.md`](../reference/troubleshooting.md).

## What to report back to the user

When the run finishes, tell the user:
1. The run ID (UUID from the summary)
2. Top-line metrics (mean throughput, p50/p90/p99 latencies for the main op)
3. Anything anomalous (warm-up that never converged, error count >0, throughput collapsed mid-run)
4. Where the raw results are saved (`~/.osb/benchmarks/test-runs/<run-id>/`)
