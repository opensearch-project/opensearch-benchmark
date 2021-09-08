This directory contains example configurations for x-pack:

* `x-pack-security`: Configures TLS for all HTTP and transport communication using self-signed certificates.
* `x-pack-monitoring`: Enables x-pack monitoring.
* `x-pack-ml`: Enables x-pack Machine Learning.

The configurations have been implemented so that you can either only one of them or both together, i.e. all of the following combinations will work:

* `--provision-config-instance="defaults,x-pack-security"`
* `--provision-config-instance="defaults,x-pack-monitoring"`
* `--provision-config-instance="defaults,x-pack-security,x-pack-monitoring-local"`

## Parameters

### x-pack-security

The `x-pack-security` provision_config_instance will enable basic authentication and TLS for the HTTP and the transport layer.
You can additionally specify the user name, password and role, via the `provision_config_instance-params` cli arg, using the following properties:

| provision_config_instance-params | default |
| --------- | ------- |
| xpack_security_user_name | rally |
| xpack_security_user_password | rally-password |
| xpack_security_user_role | superuser |

Example:

```
esrally race --distribution-version=7.5.1 --provision-config-instance="defaults,trial-license,x-pack-security" --provision-config-instance-params="xpack_security_user_name:myuser" --client-options="use_ssl:true,verify_certs:false,basic_auth_user:'myuser',basic_auth_password:'rally-password'"
```

If you are benchmarking a single node cluster, you'll also need to add `--cluster-health=yellow ` as precondition checks in Rally mandate that the cluster health has to be "green" by default but the x-pack related indices are created with a higher replica count.

### x-pack-monitoring

When using the `x-pack-monitoring` config base, you must specify a `local` or `http` (remote) exporter via the **mandatory** property `monitoring_type`.
Note that there are two provision_config_instances, `x-pack-monitoring-local` and `x-pack-monitoring-http` that configure the `monitoring_type` property for you.
Please refer to [Elasticsearch Monitoring Settings](https://www.elastic.co/guide/en/elasticsearch/reference/current/monitoring-settings.html) for more details.

When using `http` as `monitoring_type` you should also configure the following properties:

| provision_config_instance-params | description | default |
| --------- | ------------ | ------- |
| monitoring_scheme | The scheme of the monitoring cluster | http |
| monitoring_host | The host of the monitoring cluster | - |
| monitoring_port | The port of the monitoring cluster | 9200 |
| monitoring_user | The user to use on the monitoring cluster | - |
| monitoring_password | The password of the monitoring cluster user | - |

### x-pack-ml

The following optional properties may be specified, see [ML settings](https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-settings.html) for more details:

| provision_config_instance-params |
| ---------- |
| ml_max_open_jobs |
| ml_max_machine_memory_percent |
| ml_max_model_memory_limit |
| ml_node_concurrent_job_allocations |


**Security Note**

The focus here is on providing a usable configuration for benchmarks. This configuration is **NOT** suitable for production use because:

* All clusters configured by Rally will use the same (self-signed) root certificate that will basically never expire
* If you don't specify the provision_config_instance-params `x-pack_security_user_password` and `xpack_security_user_role`, Rally will add a "rally" user with super-user privileges with a hard-coded password.

Both of these measures mean that the cluster is not any more secure than without using x-pack. But once again: The idea is to be able to measure the performance characteristics not to secure the cluster that is benchmarked.
