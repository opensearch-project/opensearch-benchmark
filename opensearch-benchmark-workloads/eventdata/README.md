## EventData track

This track is based on 20 million Apache access log entries generated based on statistics from sample 
elastic.co access logs using the generator avilable here: https://github.com/elastic/rally-eventdata-track

The size of the data file is around 15GB, which gives an average JSON record size of 822 bytes. Mappings have been optimized and some of the fields added through `geoip` and `user-agent` enrichment has been removed to achieve a more compact format.

The purpose of this track is to provide an efficient way to benchmark indexing of this data type as the generator built into the rally-eventdata-track can be CPU intensive.

### Example Document

```json
{
	"agent": "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36\"",
	"useragent": {
		"os": "Mac OS X 10.10.2",
		"os_name": "Mac OS X",
		"name": "Chrome"
	},
	"geoip": {
		"country_name": "India",
		"location": [80.2833, 13.083300000000008]
	},
	"clientip": "122.178.238.140",
	"referrer": "\"-\"",
	"request": "/apple-touch-icon-144x144.png",
	"bytes": 0,
	"verb": "GET",
	"response": 304,
	"httpversion": "1.1",
	"@timestamp": "2017-07-03T07:51:49.995Z",
	"message": "122.178.238.140 - - [2017-07-03T07:51:49.995Z] \"GET /apple-touch-icon-144x144.png HTTP/1.1\" 304 0 \"-\" \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36\""
}
```

### Parameters

This track allows to overwrite the following parameters with Rally 0.8.0+ using `--track-params`:

* `bulk_size` (default: 5000)
* `bulk_indexing_clients` (default: 8): Number of clients that issue bulk indexing requests.
* `ingest_percentage` (default: 100): A number between 0 and 100 that defines how much of the document corpus should be ingested.
* `number_of_replicas` (default: 0)
* `number_of_shards` (default: 5)
* `source_enabled` (default: true): A boolean defining whether the `_source` field is stored in the index. 
* `index_settings`: A list of index settings. Index settings defined elsewhere (e.g. `number_of_replicas`) need to be overridden explicitly.
* `cluster_health` (default: "green"): The minimum required cluster health.
* `error_level` (default: "non-fatal"): Available for bulk operations only to specify ignore-response-error-level.

### License

This is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Copyright 2015-2018 Elasticsearch https://www.elastic.co

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
