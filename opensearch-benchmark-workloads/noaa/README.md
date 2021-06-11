## NOAA track

This track is based on a [daily weather measurement from NOAA](ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/).

To recreate the document corpus:

1. Download the following files:
    * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2014.csv.gz
    * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2015.csv.gz
    * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/2016.csv.gz
    * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
    * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt
    * ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-states.txt
2. Decompress measurement files. For example: `gunzip 2016.csv.gz`
3. Sort the files by station. For example: `sort --field-separator=',' --key=1,2 -o 2016-sorted.csv 2016.csv`
4. Execute a script like `_tools/process.py` to create json documents.
5. Make sure that the JSON documents are randomly ordered. (The script orders measurements of the same station next to each other). This can be achieved with `shuf documents.json > documents1.json`. 
6. Compress the documents json file: `bzip2 -9 -c documents1.json > documents.json.bz2`

### Example Document

```json
{
  "date": "2016-01-01T00:00:00",
  "TAVG": 22.9,
  "station": {
    "elevation": 34.0,
    "name": "SHARJAH INTER. AIRP",
    "country": "United",
    "gsn_flag": "GSN",
    "location": {
      "lat": 25.333,
      "lon": 55.517
    },
    "country_code": "AE",
    "wmo_id": "41196",
    "id": "AE000041196"
  },
  "TMIN": 15.5
}
```

### Parameters

This track allows to overwrite the following parameters with Rally 0.8.0+ using `--track-params`:

* `bulk_size` (default: 5000)
* `bulk_indexing_clients` (default: 8): Number of clients that issue bulk indexing requests.
* `ingest_percentage` (default: 100): A number between 0 and 100 that defines how much of the document corpus should be ingested.
* `number_of_replicas` (default: 0)
* `number_of_shards` (default: 1)
* `source_enabled` (default: true): A boolean defining whether the `_source` field is stored in the index.
* `index_settings`: A list of index settings. Index settings defined elsewhere (e.g. `number_of_replicas`) need to be overridden explicitly.
* `cluster_health` (default: "green"): The minimum required cluster health.
* `error_level` (default: "non-fatal"): Available for bulk operations only to specify ignore-response-error-level.

### License

[US Government Work data license](https://www.usa.gov/government-works)
