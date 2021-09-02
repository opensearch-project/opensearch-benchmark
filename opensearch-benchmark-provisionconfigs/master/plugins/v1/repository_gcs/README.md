This directory contains the (optional) keystore configuration for the `repository-gcs` plugin.
For more details on secure settings for the repository-gcs plugin please refer to the [repository-gcs-client](https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-gcs-client.html) documentation.

### Parameters

This configuration allows to set the following parameters with Rally using `--plugin-params` in combination with `--elasticsearch-plugins="repository-gcs"`:

* `gcs_credentials_file`: A string specifying the full path to the service account json file (mandatory).
* `gcs_client_name`: A string specifying the clientname to associate the service account file under (mandatory).

Example:

`--elasticsearch-plugins="repository-gcs" --plugin-params="gcs_client_name:internalgcsclient,gcs_credentials_file:'/home/user/service_account.json'"`

**IMPORTANT**: when provided in the inline format, the path to the credentials file needs to be enclosed in quotes.

The above settings can also be stored in a JSON file that can be specified as well with `--plugin-params`.

Example:

```json
{
  "gcs_client_name": "internalgcsclient",
  "gcs_credentials_file": "/home/user/service_account.json"
}
```   

Save it as `params.json` and provide it to Rally with `--elasticsearch-plugins="repository-gcs" --plugin-params="/path/to/params.json"`.
