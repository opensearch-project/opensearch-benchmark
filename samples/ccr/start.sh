#!/usr/bin/env bash

# This script can be used for creating a cross cluster replication setup between 2 domains and executing a benchmark run.
# Simply execute the script using ./start.sh under the python virtual environment(Refer DEVELOPER_GUIDE.md)
#
# Prerequisite: Docker installed locally.
# Steps:
# 1. Sets up 2 single node (leader and follower) clusters.
# 2. Starts a single node cluster for metrics store. We can use Kibana attached to the metric store cluster to see the metrics..
# 3. Configures the seed nodes on the follower cluster and starts replication using autofollow pattern.
# 4. Runs the eventdata benchmark on the replication setup. OSB metrics can be seen on the Kiabana.
# 5. To tear down everything, execute ./stop.sh.
set -e


# Start Opensearch
docker-compose up -d --remove-orphans

# Start metrics store
docker-compose -f ./docker-compose-metricstore.yml up -d

printf "Waiting for clusters to get ready "

# Wait until OS is up
ALL_CLUSTERS_READY=false

while ! $ALL_CLUSTERS_READY; do
    (curl -ks -u admin:admin https://localhost:9200 -o /dev/null && curl -ks -u admin:admin https://localhost:9201 -o /dev/null && ALL_CLUSTERS_READY=true) || (printf "." && sleep 5)
done

echo

# Configure the seed nodes on follower cluster
# TODO: Update the seed node to private IP.
echo "Configure remotes on follower"
curl -o /dev/null -H 'Content-Type: application/json' -k -u admin:admin -X PUT https://localhost:9201/_cluster/settings -d @- <<-EOF
    {
    "persistent" : {
        "cluster" : {
        "remote" : {
            "source" : {
            "seeds" : [
                "127.0.0.1:9300"
            ]
            }
        }
        }
    }
    }
EOF

echo "Set auto-follow pattern on follower for every index on leader"
curl -H 'Content-Type: application/json' -k -u admin:admin https://localhost:9201/_plugins/_replication/_autofollow -d @- <<-EOF
{
  "leader_alias": "source",
  "name": "all",
  "pattern": "eventdata*",
  "use_roles": {
    "leader_cluster_role": "all_access",
    "follower_cluster_role": "all_access"
   }
}
EOF


# Create target-hosts file for OSB.
cat >ccr-target-hosts.json <<'EOF'
{
  "default": [
    "https://127.0.0.1:9200"
  ],
  "follower": [
    "https://127.0.0.1:9201"
  ]
}
EOF

cat >ccr-telemetry-param.json <<'EOF'
{
  "ccr-stats-sample-interval": 1,
  "ccr-stats-indices": {
	  "follower": ["eventdata"]
  },
  "ccr-max-replication-lag-seconds": 36000
}
EOF

cat >ccr-client-options.json <<'EOF'
{
  "default": {
    "use_ssl":"true",
    "basic_auth_user":"admin",
    "basic_auth_password":"admin",
    "verify_certs":"false"
  },
  "follower": {
    "use_ssl":"true",
    "basic_auth_user":"admin",
    "basic_auth_password":"admin",
    "verify_certs":"false"
  }
}
EOF


# Create metricstore ini file
cat >${HOME}/.benchmark/benchmark.ini <<EOF
[meta]
config.version = 17

[system]
env.name = local

[node]
root.dir = ${HOME}/.benchmark/benchmarks
src.root.dir = ${HOME}/.benchmark/benchmarks/src

[source]
remote.repo.url = https://github.com/opensearch-project/OpenSearch.git
opensearch.src.subdir = opensearch

[benchmarks]
local.dataset.cache = ${HOME}/.benchmark/benchmarks/data

[results_publishing]
datastore.type = opensearch
datastore.host = 127.0.0.1
datastore.port = 9209
datastore.secure = True
datastore.user = admin
datastore.password = admin


[workloads]
default.url = https://github.com/opensearch-project/opensearch-benchmark-workloads

[provision_configs]
default.dir = default-provision-config

[defaults]
preserve_benchmark_candidate = false

[distributions]
release.cache = true

EOF


# Start OpenSearch Benchmark
opensearch-benchmark execute-test --configuration-name=metricstore --workload=geonames --target-hosts=./ccr-target-hosts.json --pipeline=benchmark-only --workload-params="number_of_replicas:1" --client-options=./ccr-client-options.json --kill-running-processes --telemetry="ccr-stats" --telemetry-params=./ccr-telemetry-param.json