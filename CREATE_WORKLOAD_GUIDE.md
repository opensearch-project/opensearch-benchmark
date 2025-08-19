# Create Workload Guide

This guide explores how users can use the `create-workload` subcommand in OpenSearch Benchmark to create a workload based on pre-existing data in a cluster.

### Create a Workload from Pre-Existing Indices in a Cluster

**Prerequisites:**
* OpenSearch cluster with data ingested into it in an index. Ensure that index has 1000+ docs. If not, a workload will be created but users cannot run the workload with `--test-mode`.
* Ensure that your cluster is permissive.

Create a workload with the following command:
```
$ opensearch-benchmark create-workload \
--workload="<WORKLOAD NAME>" \
--target-hosts="<CLUSTER ENDPOINT>" \
--client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'" \
--indices="<INDICES TO GENERATE WORKLOAD FROM>" \
--output-path="<LOCAL DIRECTORY PATH TO STORE WORKLOAD>"
```
Note that:
* `--indices` can be 1+ indices specified in a comma-separated list.
* If the cluster uses basic authentication and has TLS enabled, users will need to provide them through `--client-options`.

The following is an example output of when a user creates a workload from an index called movies that contains 2000 docs.

```
   ____                  _____                      __       ____                  __                         __
  / __ \____  ___  ____ / ___/___  ____ ___________/ /_     / __ )___  ____  _____/ /_  ____ ___  ____ ______/ /__
 / / / / __ \/ _ \/ __ \\__ \/ _ \/ __ `/ ___/ ___/ __ \   / __  / _ \/ __ \/ ___/ __ \/ __ `__ \/ __ `/ ___/ //_/
/ /_/ / /_/ /  __/ / / /__/ /  __/ /_/ / /  / /__/ / / /  / /_/ /  __/ / / / /__/ / / / / / / / / /_/ / /  / ,<
\____/ .___/\___/_/ /_/____/\___/\__,_/_/   \___/_/ /_/  /_____/\___/_/ /_/\___/_/ /_/_/ /_/ /_/\__,_/_/  /_/|_|
    /_/

[INFO] You did not provide an explicit timeout in the client options. Assuming default of 10 seconds.
[INFO] Connected to OpenSearch cluster [380d8fd64dd85b5f77c0ad81b0799e1e] version [1.1.0].

Extracting documents for index [movies] for test mode...      1000/1000 docs [100.0% done]
Extracting documents for index [movies]...                    2000/2000 docs [100.0% done]

[INFO] Workload movies has been created. Run it with: opensearch-benchmark --workload-path=/Users/hoangia/Desktop/workloads/movies

-------------------------------
[INFO] SUCCESS (took 2 seconds)
-------------------------------
```

By default, workloads created will come with the following operations run in the following order:
* **delete-index**: Deletes any pre-existing indices with the same name(s) as the indices provided in `--indices`
* **create-index**: Creates the index with the same name(s) as the indices provided in `--indices`
* **cluster-health**: Verifies that cluster health is green before proceeding with the ingestion
* **bulk**: Ingests documents collected from the indices specified in `--indices`
* **default**: Runs a match-all query on the index for a number of iterations

To invoke the newly created workload, run the following:
```
$ opensearch-benchmark run \
--pipeline="benchmark-only" \
--workload-path="<PATH OUTPUTTED IN THE OUTPUT OF THE CREATE-WORKLOAD COMMAND>" \
--target-host="<CLUSTER ENDPOINT>" \
--client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

Users have the options to specify a subset of documents from the index or override the default match_all query. See the following sections for more information on how.

### Adding Custom Queries
Add `--custom-queries` to the `create-workload` command. This parameter takes in a JSON filepath. This overrides the default match_all query with the queries present in the input file.

Requirements:
* Ensure that queries are properly formatted and adhere to JSON schema
* Ensure that all queries are contained within a list. Exception: If providing only a single query, it does not have to be in a list.

Adding to the previous example, a user wants to override default query with the following two custom queries in a JSON file.
```
[
  {
    "name": "default",
    "operation-type": "search",
    "body": {
      "query": {
        "match_all": {}
      }
    }
  },
  {
    "name": "term",
    "operation-type": "search",
    "body": {
      "query": {
        "term": {
          "director": "Ian"
        }
      }
    }
  }
]
```

To do this, the user can provide the JSON filepath to `--custom-queries` parameter:
```
$ opensearch-benchmark create-workload \
--workload="<WORKLOAD NAME>" \
--target-hosts="<CLUSTER ENDPOINT>" \
--client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'" \
--indices="<INDICES TO GENERATE WORKLOAD FROM>" \
--output-path="<LOCAL DIRECTORY PATH TO STORE WORKLOAD>" \
--custom-queries="<JSON filepath containing queries>"
```

### Common Errors
When adding custom queries, users might experience the following error will occur if the queries do not adhere to JSON schema standards or are not in a list.
```
[INFO] You did not provide an explicit timeout in the client options. Assuming default of 10 seconds.
[ERROR] Cannot create-workload. Ensure JSON schema is valid and queries are contained in a list: Extra data: line 9 column 2 (char 113)
```