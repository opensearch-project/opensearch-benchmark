---
layout: default
title: Execute Test
parent: Benchmark API
nav_order: 10
---

The `execute-test` command of OpenSearch Benchmark executes tests against your OpenSearch cluster.


## Syntax

```bash
opensearch-benchmark execute-test <arguments>
```


### Common Arguments

See [All Arguments](#all-arguments) for an exhaustive list of arguments

Argument | Description | Required
:--- | :--- |:---
`workload` | The dataset and operations that execute during a test. See [OpenSearch Benchmark Workloads repository](https://github.com/opensearch-project/opensearch-benchmark-workloads) for more details on workloads. | Yes
`workload-params` | Parameters defined within each workload that can be overwritten. These parameters are outlined in the README of each workload. You can find an example of the parameters for the eventdata workload [here](https://github.com/opensearch-project/opensearch-benchmark-workloads/tree/main/eventdata#parameters).  | No
`test_procedure` | Test Procedures define the sequence of operations and parameters for a specific workload. When no `test_procedure` is specified, Benchmark selects the default for the workload. You can find an example test procedure [here](https://github.com/opensearch-project/opensearch-benchmark-workloads/blob/main/eventdata/test_procedures/default.json). | No
`client-options` | Options for the [OpenSearch Python client](https://opensearch.org/docs/latest/clients/python/). Required if testing against a cluster with security enabled. | No
`pipeline` | Steps required to execute a test, including provisioning an OpenSearch from source code or a specified distribution. Defaults to `from-sources` which provisions an OpenSearch cluster from source code. | No
`distribution-version` | The OpenSearch version to use for a given test. Defining a version can be useful when using a `pipeline` that includes provisioning. When using a `pipeline` without provisioning, Benchmark will automatically determine the version | No
`target-hosts` | The OpenSearch endpoint(s) to execute a test against. This should only be specified with  `--pipeline=benchmark-only`  | No
`test-mode` | Run a single iteration of each operation in the test procedure. The test provides a quick way for sanity checking a testing configuration. Therefore, do not use `test-mode` for actual benchmarking. | No
`kill-running-processes` | Kill any running OpenSearch Benchmark processes on the local machine before the test executes. | No

*Example 1*

```
opensearch-benchmark execute-test --workload eventdata --test-mode
```

Provision an OpenSearch node on the local machine based on the latest source code in Github and execute the `eventdata` workload in test mode.

*Example 2*

```
opensearch-benchmark execute-test --workload http_logs --pipeline benchmark-only --target-hosts <endpoint> --workload-params "bulk_indexing_clients:1,ingest_percentage:10"
```

Execute the `http_logs` workload against an existing OpenSearch cluster but only use one client for indexing and only ingest 10% of the total data corpus.

*Example 3*

```
opensearch-benchmark execute-test --workload nyc_taxis --pipeline benchmark-only --target-hosts <endpoint> --client-options "verify_certs:false,use_ssl:true,basic_auth_user:admin,basic_auth_password:admin"
```

Execute the `nyc_taxis` workload against an existing OpenSearch cluster with the security plugin enabled.


### All Arguments

Argument | Description | Required
:--- | :--- |:---
`distribution-version` | Define the version of the OpenSearch distribution to download. Check https://opensearch.org/docs/version-history/ for released versions. | No
`provision-config-path` | Define the path to the provision_config_instance and plugin configurations to use. | No
`provision-config-repository` | Define repository from where Benchmark will load provision_configs and provision_config_instances (default: `default`). | No
`provision-config-revision` | Define a specific revision in the provision_config repository that Benchmark should use. | No
`test-execution-id` | Define a unique id for this test_execution. | No
`pipeline` | Select the pipeline to run. | No
`revision` | Define the source code revision for building the benchmark candidate. 'current' uses the source tree as is, 'latest' fetches the latest version on main. It is also possible to specify a commit id or an ISO timestamp. The timestamp must be specified as: "@ts" where "ts" must be a valid ISO 8601 timestamp, e.g. "@2013-07-27T10:37:00Z" (default: `current`). | No
`workload-repository` | Define the repository from where Benchmark will load workloads (default: `default`). | No
`workload-path` | Define the path to a workload. | No
`workload-revision` | Define a specific revision in the workload repository that Benchmark should use. | No
`workload` | Define the workload to use. List possible workloads with `opensearch-benchmark list workloads`. | No
`workload-params` | Define a comma-separated list of key:value pairs that are injected verbatim to the workload as variables. | No
`test-procedure` | Define the test_procedure to use. List possible test_procedures for workloads with `opensearch-benchmark list workloads`. | No
`provision-config-instance` | Define the provision_config_instance to use. List possible provision_config_instances with `opensearch-benchmark list provision_config_instances` (default: `defaults`). | No
`provision-config-instance-params` | Define a comma-separated list of key:value pairs that are injected verbatim as variables for the provision_config_instance. | No
`runtime-jdk` | The major version of the runtime JDK to use. | No
`opensearch-plugins` | Define the OpenSearch plugins to install. (default: install no plugins). | No
`plugin-params` | Define a comma-separated list of key:value pairs that are injected verbatim to all plugins as variables. | No
`target-hosts` | Define a comma-separated list of host:port pairs which should be targeted if using the pipeline 'benchmark-only' (default: `localhost:9200`). | No
`load-worker-coordinator-hosts` | Define a comma-separated list of hosts which should generate load (default: `localhost`). | No
`client-options` | Define a comma-separated list of client options to use. The options will be passed to the OpenSearch Python client (default: `timeout:60`). | No
`on-error` | Controls how Benchmark behaves on response errors. Options are `continue` and `abort` (default: `continue`). | No
`telemetry` | Enable the provided telemetry devices, provided as a comma-separated list. List possible telemetry devices with `opensearch-benchmark list telemetry`. | No
`telemetry-params` | Define a comma-separated list of key:value pairs that are injected verbatim to the telemetry devices as parameters. | No
`distribution-repository` | Define the repository from where the OpenSearch distribution should be downloaded (default: `release`). | No
`include-tasks` | Defines a comma-separated list of tasks to run. By default all tasks of a test_procedure are run. | No
`exclude-tasks` | Defines a comma-separated list of tasks not to run. By default all tasks of a test_procedure are run. | No
`user-tag` | Define a user-specific key-value pair (separated by ':'). It is added to each metric record as meta info. Example: intention:baseline-ticket-12345 | No
`results-format` | Define the output format for the command line results. Options are `markdown` and `csv` (default: `markdown`). | No
`results-numbers-align` | Define the output column number alignment for the command line results. Options are `right`, `center`, `left` and `decimal` (default: right). | No
`show-in-results` | Define which values are shown in the summary publish. Options are `available`, `all-percentiles` and `all` (default: `available`). | No
`results-file` | Write the command line results also to the provided file. | No
`preserve-install` | Keep the benchmark candidate and its index. (default: false). | No
`test-mode` | Runs the given workload in 'test mode'. Meant to check a workload for errors but not for real benchmarks (default: false). | No
`enable-worker-coordinator-profiling` | Enables a profiler for analyzing the performance of calls in Benchmark's worker coordinator (default: false). | No
`enable-assertions` | Enables assertion checks for tasks (default: false). | No
`kill-running-processes` | If any processes is running, it is going to kill them and allow Benchmark to continue to run. | No
`quiet` | Suppress as much as output as possible (default: false). | No
`offline` | Assume that Benchmark has no connection to the Internet (default: false). | No

