This directory contains the OpenSearch base configuration.

### Parameters

This configuration allows to set the following parameters with Benchmark 0.10.0 using `--provision-config-instance-params`:

* `data_paths` (default: "data" (relative to the OpenSearch root directory)): A string specifying the OpenSearch data path.
* `indexing_pressure_memory_limit` (default: not set): A percentage value defining the cluster setting `indexing_pressure.memory.limit`.
* `additional_cluster_settings` (default: empty): A dictionary of key-value pairs with additional settings in `opensearch.yml`.
* `additional_java_settings` (default: empty): A list of additional settings in `jvm.options`. Each entry in the list will end up as one line in `jvm.options`.

It is recommended to store those settings in a JSON file that can be specified as well with `--provision-config-instance-params`.

Example:

```json
{
  "data_paths": "/mnt/data",
  "additional_cluster_settings": {
    "indices.queries.cache.size": "5%",
    "transport.tcp.compress": true
  },
  "additional_java_settings": [
    "-XX:+PreserveFramePointer"
  ]
}
```

Save it as `params.json` and provide it to Benchmark with `--provision-config-instance-params="/path/to/params.json"`.
