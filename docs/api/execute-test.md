---
layout: default
title: Execute Test
parent: Benchmark API
nav_order: 10
---

The `execute_test` command is the primary usage of OpenSearch Benchmark. This command is used to execute a test.


## Syntax 

```bash
opensearch-benchmark execute_test <args>
```


### Arguments

**Note**: This is not an exhaustive list of arguments. 

Argument | Description | Required
:--- | :--- |:---
`workload` | The dataset and operations that will be executed during a test. Workloads are defined in the [OpenSearch Benchmark Workloads repository](https://github.com/opensearch-project/opensearch-benchmark-workloads). | Yes
`workload-params` | Parameters defined within each workload that can be overwritten. These are defined within the README of each workload. An example for the eventdata workload can be found [here](https://github.com/opensearch-project/opensearch-benchmark-workloads/tree/main/eventdata#parameters).  | No
`test_procedure` | Test Procedures define the sequence of operations and parameters for a specific workload. If no `test_procedure` is specified the default will be selected. Test Procedures are defined within each workload directory. An example for the eventdata workload can be found [here](https://github.com/opensearch-project/opensearch-benchmark-workloads/blob/main/eventdata/test_procedures/default.json). | No
`client-options` | Options for the [OpenSearch Python client](https://opensearch.org/docs/latest/clients/python/). Required if testing against a cluster with security enabled. | No
`pipeline` | Steps required to execute a test. This can including provisioning an OpenSearch from source code or from a specified distribution.  | No
`distribution-version` | The OpenSearch version to use for a given test. This can be useful when using a `pipeline` which includes provisioning. When using a `pipeline` without provisioning Benchmark will automatically determine the version | No
`target-hosts` | The OpenSearch endpoint(s) to execute a test against. This should only be specified with  `--pipeline=benchmark-only`  | No
`test-mode` | Run a single iteration of each operation in the Test Procedure. This is meant to provide a quick way for sanity checking a testing configuration and should not be used for actual benchmarking. | No
`kill-running-processes` | Kill any running OpenSearch Benchmark processes on the local machine before the test is executed.  | No

*Example 1*

```
opensearch-benchmark execute_test --workload eventdata --test-mode
```

Provision an OpenSearch node on the local machine based on the latest source code in Github and execute the `eventdata` workload in test mode. 

*Example 2*

```
opensearch-benchmark execute_test --workload http_logs --pipeline benchmark-only --target-hosts <endpoint> --workload-params "bulk_indexing_clients:1,ingest_percentage:10"
```

Execute the `http_logs` workload against an existing OpenSearch cluster but only use 1 client for indexing and only ingest 10% of the total data corpus . 

*Example 3*

```
opensearch-benchmark execute_test --workload nyc_taxis --pipeline benchmark-only --target-hosts <endpoint> --client-options "verify_certs:false,use_ssl:true,basic_auth_user:admin,basic_auth_password:admin"
```

Execute the `nyc_taxis` workload against an existing OpenSearch cluster with the security plugin enabled. 

