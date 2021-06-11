## StackOverflow track

This dataset is derived from a dump of StackOverflow posts downloaded on June 10th 2016 from
https://ia800500.us.archive.org/22/items/stackexchange/stackoverflow.com-Posts.7z

Each question and answer have formatted into a JSON document with the following fields:
    
	questionId:	      a unique ID for a question
	answerId:         a unique ID for an answer
	acceptedAnswerId: the unique ID of the answer accepted for question
	title:	          a free-text field with the question title
	creationDate:	  The date the questions was asked 
	user:	          The user's unique ID
	tags:	          An array of tags describing the technologies.
    body:             Field contsaining the text of the question or answer.
    type:             Type of post. Either 'question' or 'answer'
	
Fields that do not have values have been left out. The body has had text extracted and been 
formatted to fit into JSON documents.

Data preparation process:
* Question and answer entries in the original posts.XML were converted to slimmed-down JSON 
  documents.
* No enrichment was performed.
These scripts are available in the raw_data_prep_script.zip file.

### Example Document

```json
{
	"user": "45",
	"tags": ["c#", "linq", ".net-3.5"],
	"questionId": "59",
	"creationDate": "2008-08-01T13:14:33.797",
	"title": "How do I get a distinct, ordered list of names from a DataTable using LINQ?",
	"acceptedAnswerId": "43110",
	"type": "question",
	"body": "Let's say I have a DataTable with a Name column. I want to have a collection of the unique names ordered alphabetically. The following query ignores the order by clause. var names = (from DataRow dr in dataTable.Rows orderby (string)dr[\"Name\"] select (string)dr[\"Name\"]).Distinct(); Why does the orderby not get enforced? "
}
```

### Parameters

This track allows to overwrite the following parameters with Rally 0.8.0+ using `--track-params`:

* `bulk_size` (default: 5000)
* `bulk_indexing_clients` (default: 4): Number of clients that issue bulk indexing requests.
* `ingest_percentage` (default: 100): A number between 0 and 100 that defines how much of the document corpus should be ingested.
* `number_of_replicas` (default: 0)
* `number_of_shards` (default: 5)
* `source_enabled` (default: true): A boolean defining whether the `_source` field is stored in the index.
* `index_settings`: A list of index settings. Index settings defined elsewhere (e.g. `number_of_replicas`) need to be overridden explicitly.
* `cluster_health` (default: "green"): The minimum required cluster health.
* `error_level` (default: "non-fatal"): Available for bulk operations only to specify ignore-response-error-level.

### License

We use the same license for the data as the original data: [CC-SA-3.0](http://creativecommons.org/licenses/by-sa/3.0/)
