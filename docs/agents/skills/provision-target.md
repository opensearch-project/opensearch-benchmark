# Skill: provision a target cluster

Goal: stand up an OpenSearch cluster on AWS to benchmark against. This is for ephemeral test clusters: production clusters are out of scope.

## When to use this

- You need a fresh cluster for a one-off benchmark.
- You want to test against a specific OpenSearch version or build that isn't already deployed.
- Existing clusters are too small, too busy, or have data you don't want to disturb.

If a cluster already exists and you can reach it, use it. Provisioning takes 10-20 minutes and you'll usually want to tear it down after.

## Two approaches

### Option A: opensearch-cluster-cdk (recommended for AWS)

The [opensearch-cluster-cdk](https://github.com/opensearch-project/opensearch-cluster-cdk) repo defines CDK stacks for spinning up OpenSearch on EC2.

```
git clone https://github.com/opensearch-project/opensearch-cluster-cdk
cd opensearch-cluster-cdk
npm install
cdk deploy \
  -c distVersion=<x.y.z> \
  -c serverAccessType=ipv4 \
  -c restrictServerAccessTo=<your-ip>/32 \
  -c singleNodeCluster=true \
  -c securityDisabled=true
```

Key context flags:

| Flag | Purpose |
|---|---|
| `distVersion` | OpenSearch version (e.g., `3.0.0`) |
| `singleNodeCluster` | `true` for single-node, `false` for cluster with separate data/manager nodes |
| `dataInstanceType` | Default is `r6g.large`. For perf work, `m5.4xlarge` or larger. |
| `securityDisabled` | `true` to skip auth. Easier for benchmarks, but never use against shared infra. |
| `serverAccessType` | `ipv4` or `prefix` (PrefixList) |
| `restrictServerAccessTo` | CIDR allowed to reach the cluster |
| `enableRemoteStore` | `true` to enable segment replication / remote store |

Read the repo's README before deploying: flags evolve. After `cdk deploy`, the CloudFormation outputs include the cluster endpoint.

### Option B: tarball install on a single EC2

Faster than CDK if you just need one node:

1. Launch an EC2 instance (e.g., `r7g.4xlarge` running Amazon Linux 2023).
2. SSH/SSM in.
3. Download and untar the OpenSearch release:
   ```
   wget https://artifacts.opensearch.org/releases/bundle/opensearch/3.0.0/opensearch-3.0.0-linux-arm64.tar.gz
   tar -xf opensearch-3.0.0-linux-arm64.tar.gz
   cd opensearch-3.0.0
   ```
4. Edit `config/opensearch.yml`:
   ```
   discovery.type: single-node
   plugins.security.disabled: true
   network.host: 0.0.0.0
   ```
5. Increase `vm.max_map_count`:
   ```
   sudo sysctl -w vm.max_map_count=262144
   ```
6. Start:
   ```
   ./opensearch-tar-install.sh
   ```
7. Verify from the OSB host:
   ```
   curl <ec2-private-ip>:9200/_cluster/health?pretty
   ```

This is faster to iterate on but you own the lifecycle (start/stop, SG rules, instance termination).

## Sizing guidance

For most workload smoke tests:
- `r6g.large` is enough: single-node, ~15GB heap, fits most test-mode runs
- `r7g.4xlarge` for moderate workloads (big5, vectorsearch with small datasets)
- `r7g.8xlarge` or `m5.8xlarge` for full-scale runs (100GB+ corpora, nyc_taxis)

Match the workload to the cluster. A 250GB workload on a 32GB-RAM node will spend the run thrashing disk.

## Connecting OSB to the new cluster

Once the cluster is up, the OSB invocation is just:

```
opensearch-benchmark run \
  --workload=<workload> \
  --target-hosts=<cluster-ip>:9200 \
  --pipeline=benchmark-only \
  --kill-running-processes
```

If you disabled security, no `--client-options` needed. If you didn't:

```
--client-options="basic_auth_user:admin,basic_auth_password:<pw>,use_ssl:true,verify_certs:false"
```

## Teardown

**Don't forget this.** Idle EC2 + EBS volumes cost money.

- CDK: `cdk destroy` from the same directory.
- Manual: `aws ec2 terminate-instances --instance-ids <id>` and delete any associated EBS volumes that don't auto-delete-on-terminate.

Confirm there are no leftover resources:

```
aws ec2 describe-instances --filters "Name=instance-state-name,Values=running,stopped" --query 'Reservations[].Instances[].[InstanceId,Tags,LaunchTime]' --output table
```

If you're operating in someone else's account, **always** confirm before tearing down: instances you didn't create may not be yours to delete. See the production safety notes in [`reference/aws-access.md`](../reference/aws-access.md).
