This directory contains the (optional) keystore configuration for the `repository-s3` plugin.

### Parameters

This plugin allows to set the following parameters with Benchmark using `--plugin-params` in combination with `--opensearch-plugins="repository-s3"`:

* `s3_client_name`: A string specifying the clientname to associate the above credentials with (mandatory).
* `s3_access_key`: A string specifying the AWS access key (mandatory).
* `s3_secret_key`: A string specifying the AWS secret key (mandatory).
* `s3_session_token`: A string specifying the AWS session token (optional).

Example:

`--opensearch-plugins="repository-s3" --plugin-params="s3_client_name:mys3client,s3_access_key:XXXXX,s3_secret_key:YYYYY,s3_session_token:ZZZZZ"`

Alternatively, the above settings can also be stored in a JSON file that can be specified via `--plugin-params`.

Example:

```json
{
  "s3_client_name": "mys3client",
  "s3_access_key": "XXXXX",
  "s3_secret_key": "YYYYY",
  "s3_session_token": "ZZZZZ"
}
```

Save it as `params.json` and provide it to Benchmark with `--opensearch-plugins="repository-s3" --plugin-params="/path/to/params.json"`.
