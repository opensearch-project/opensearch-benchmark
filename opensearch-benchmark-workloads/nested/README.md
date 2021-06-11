## Nested track

This track is based on a [dump of StackOverflow posts](https://ia800500.us.archive.org/22/items/stackexchange/stackoverflow.com-Posts.7z) retrieved as of June 10, 2016.

Each question and related answers have been assembled into a single JSON doc containing:

* qid: a unique ID for a question 
* title: a free-text field with the question title
* creationDate:	The date the questions was asked 
* user:	The user's screen name and unique ID combined into a single string
* tag: An array of tags describing the technologies.
* answers: An array of objects, one per answer, with the following fields:
    * date: Date of answer
    * user: Answerer's screen name and unique ID combined into a single string
		

Data preparation process:

* Question and answer entries in the original posts.XML were converted to slimmed-down rows in a CSV and enriched with user names from users.xml
* CSV was sorted by first two columns (questionID and answerID)
* The CSV was converted to the JSON file presented here, combining questions and answers into a single JSON doc.

These scripts are available in the raw_data_prep_scripts.zip file.

### Example Document

```json
{
  "title": "Are these LAMP permissions secure?",
  "qid": "10000023",
  "answers": [
    {
      "date": "2012-04-04T12:56:34.433",
      "user": "larsks (147356)"
    }
  ],
  "tag": [
    "linux",
    "apache",
    "security",
    "ubuntu",
    "permissions"
  ],
  "user": "Trent Scott (600873)",
  "creationDate": "2012-04-03T19:26:57.033"
}
```

### Parameters

This track allows to overwrite the following parameters with Rally 0.8.0+ using `--track-params`:

* `bulk_size` (default: 5000)
* `bulk_indexing_clients` (default: 4): Number of clients that issue bulk indexing requests.
* `ingest_percentage` (default: 100): A number between 0 and 100 that defines how much of the document corpus should be ingested.
* `number_of_replicas` (default: 0)
* `number_of_shards` (default: 1)
* `source_enabled` (default: true): A boolean defining whether the `_source` field is stored in the index.
* `index_settings`: A list of index settings. Index settings defined elsewhere (e.g. `number_of_replicas`) need to be overridden explicitly.
* `cluster_health` (default: "green"): The minimum required cluster health.
* `error_level` (default: "non-fatal"): Available for bulk operations only to specify ignore-response-error-level.

### License

We use the same license for the data as the original data: [CC-SA-3.0](http://creativecommons.org/licenses/by-sa/3.0/)

