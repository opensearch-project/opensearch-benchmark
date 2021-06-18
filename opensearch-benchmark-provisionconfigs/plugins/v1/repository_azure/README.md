This directory contains the (optional) keystore configuration for the `repository-azure` plugin.
For more details on secure settings for the repository-azure plugin please refer to the [repository-azure-client](https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-azure-client-settings.html) documentation.

### Parameters

This plugin allows to set the following parameters with Rally using `--plugin-params` in combination with `--elasticsearch-plugins="repository-azure"`:

* `azure_client_name`: A string specifying the clientname to associate the above credentials with (mandatory).
* `azure_account`: A string specifying the Azure account name (mandatory).
* `azure_key`: A string specifying the Azure key (mandatory).

Example:

`--elasticsearch-plugins="repository-azure" --plugin-params="azure_client_name:default,azure_account:XXXXX,azure_key:YYYYY"`

Alternatively, the above settings can also be stored in a JSON file that can be specified via `--plugin-params`.

Example:

```json
{
  "azure_client_name": "default",
  "azure_account": "XXXXX",
  "azure_key": "YYYYY"
}
```   

Save it as `params.json` and provide it to Rally with `--elasticsearch-plugins="repository-azure" --plugin-params="/path/to/params.json"`.
