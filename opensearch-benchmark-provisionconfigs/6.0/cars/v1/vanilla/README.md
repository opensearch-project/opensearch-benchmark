This directory contains the Elasticsearch base configuration.

### Parameters

This configuration allows to set the following parameters with Rally 0.10.0 using `--car-params`:

* `data_paths` (default: "data" (relative to the Elasticsearch root directory)): A string specifying the Elasticsearch data path.
* `additional_cluster_settings` (default: empty): A dictionary of key-value pairs with additional settings in `elasticsearch.yml`.
* `additional_java_settings` (default: empty): A list of additional settings in `jvm.options`. Each entry in the list will end up as one line in `jvm.options`.

It is recommended to store those settings in a JSON file that can be specified as well with `--car-params`.

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

Save it as `params.json` and provide it to Rally with `--car-params="/path/to/params.json"`.
