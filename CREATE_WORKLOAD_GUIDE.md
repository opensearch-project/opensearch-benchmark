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
--number-of-docs="<INDEX NAME>:<NUMBER OF DOCUMENTS>" 
--output-path="<LOCAL DIRECTORY PATH TO STORE WORKLOAD>"
--custom-queries="<FILE PATH OF CUSTOM QUERIES TO ADD FOR THE WORKLOAD EXECUTION>"
--concurrent
--threads="<NUMBER OF THREADS TO RUN THE INDEX EXTRACTION>"
--bsize="<BATCH SIZE OF EVERY INDEX EXTRACTION QUERY WITHIN THREADS>"
--custom_dump_query="<FILE PATH OF THE CUSTOM DOCUMENT SEARCH QUERY FOR INDEX EXTRACTION>"
```
Note that:
* `--indices` can be 1+ indices specified in a comma-separated list in the format `index1,index2`
* If the cluster uses basic authentication and has TLS enabled, users will need to provide them through `--client-options`.
* `--number-of-docs`, `--concurrent`, `--threads`, `--bsize`, `--custom_dump_query` are OPTIONAL
* `--number-of-docs` can be 1+ `index:num_docs` pairs in the format `index1:num_docs, index2:num_docs`
* `--concurrent` is a flag, and does not require a value 
* `--bsize` and `--custom_dump_query` can be set even without concurrency, but `--threads` needs the `--concurrent` flag to be set

Below, we provide examples of how to use `create-workload`. We will be using the sample `flights` and `ecommerce` datasets that opensearch-dashboards provides. The commands listed below can be used with a running opensearch instance created using this sample [docker-compose.yml](https://opensearch.org/docs/latest/install-and-configure/install-opensearch/docker/#sample-docker-composeyml) file

The following is an example output of when a user creates a workload from a single index called `opensearch_dashboards_sample_data_flights` that contains 1500 docs (without concurrency).

```
$ opensearch-benchmark create-workload \
--workload="flights" \
--target-host="https://127.0.0.1:9200" \
--client-options="basic_auth_user:'admin',basic_auth_password:'admin'" \
--output-path="~/workloads" \
--indices="opensearch_dashboards_sample_data_flights" \                                            
--number-of-docs="opensearch_dashboards_sample_data_flights:2500" \                                                 
--client-options="timeout:300,use_ssl:true,verify_certs:false,basic_auth_user:'admin',basic_auth_password:'admin'"

   ____                  _____                      __       ____                  __                         __
  / __ \____  ___  ____ / ___/___  ____ ___________/ /_     / __ )___  ____  _____/ /_  ____ ___  ____ ______/ /__
 / / / / __ \/ _ \/ __ \\__ \/ _ \/ __ `/ ___/ ___/ __ \   / __  / _ \/ __ \/ ___/ __ \/ __ `__ \/ __ `/ ___/ //_/
/ /_/ / /_/ /  __/ / / /__/ /  __/ /_/ / /  / /__/ / / /  / /_/ /  __/ / / / /__/ / / / / / / / / /_/ / /  / ,<
\____/ .___/\___/_/ /_/____/\___/\__,_/_/   \___/_/ /_/  /_____/\___/_/ /_/\___/_/ /_/_/ /_/ /_/\__,_/_/  /_/|_|
    /_/

[INFO] Connected to OpenSearch cluster [opensearch-node1] version [2.11.0].

Extracting documents from opensearch_dashboards_sample_data_flights [for test mode]: 100%|███████████████████████████████████████████████████████████████████████| 1000/1000 [00:00<00:00, 4484.50doc/s]
Extracting documents from opensearch_dashboards_sample_data_flights: 100%|███████████████████████████████████████████████████████████████████████████████████████| 2500/2500 [00:00<00:00, 4878.57doc/s]


[INFO] Workload flights has been created. Run it with: opensearch-benchmark --workload-path=/home/aksha/Workbench/Workloads/flights

-------------------------------
[INFO] SUCCESS (took 1 seconds)
-------------------------------
```

The following is an example output of when a user creates a workload from indices called `opensearch_dashboards_sample_data_flights` and `opensearch_dashboards_sample_data_flights_ecommerce` which contains 2500 and 1500 docs respectively, with concurrency and other parameters provided.

```
$ opensearch-benchmark create-workload --workload="flights" --target-host="https://127.0.0.1:9200" \
--client-options="basic_auth_user:'admin',basic_auth_password:'admin'" \
--output-path="~/workloads" \
--indices="opensearch_dashboards_sample_data_flights,opensearch_dashboards_sample_data_ecommerce" \
--number-of-docs="opensearch_dashboards_sample_data_flights:2500,opensearch_dashboards_sample_data_ecommerce:1500" \
--client-options="timeout:300,use_ssl:true,verify_certs:false,basic_auth_user:'admin',basic_auth_password:'admin'" \
--concurrent --threads=8 --bsize=50 --custom_dump_query="../custom_query.json"


   ____                  _____                      __       ____                  __                         __
  / __ \____  ___  ____ / ___/___  ____ ___________/ /_     / __ )___  ____  _____/ /_  ____ ___  ____ ______/ /__
 / / / / __ \/ _ \/ __ \\__ \/ _ \/ __ `/ ___/ ___/ __ \   / __  / _ \/ __ \/ ___/ __ \/ __ `__ \/ __ `/ ___/ //_/
/ /_/ / /_/ /  __/ / / /__/ /  __/ /_/ / /  / /__/ / / /  / /_/ /  __/ / / / /__/ / / / / / / / / /_/ / /  / ,<
\____/ .___/\___/_/ /_/____/\___/\__,_/_/   \___/_/ /_/  /_____/\___/_/ /_/\___/_/ /_/_/ /_/ /_/\__,_/_/  /_/|_|
    /_/

[INFO] Connected to OpenSearch cluster [opensearch-node1] version [2.11.0].

Extracting documents from opensearch_dashboards_sample_data_flights [for test mode]: 100%|███████████████████████████████████████████████████████████████████████████████████████| 1000/1000 [00:00<00:00, 6777.74doc/s]
Extracting documents from opensearch_dashboards_sample_data_flights: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████| 2500/2500 [00:00<00:00, 9417.50doc/s]
Extracting documents from opensearch_dashboards_sample_data_ecommerce [for test mode]: 100%|█████████████████████████████████████████████████████████████████████████████████████| 1000/1000 [00:00<00:00, 5065.93doc/s]
Extracting documents from opensearch_dashboards_sample_data_ecommerce: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████| 1500/1500 [00:00<00:00, 5928.33doc/s]


[INFO] Workload flights has been created. Run it with: opensearch-benchmark --workload-path=/home/aksha/Workbench/Workloads/flights

-------------------------------
[INFO] SUCCESS (took 1 seconds)
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
$ opensearch-benchmark execute_test \
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