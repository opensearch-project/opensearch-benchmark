# Provisioning Cluster

## Setup
### Environment Setup
- Install `terraform`
- Install AWS CLI
- In the AWS Console, go to "Security Credentials" and create a new "Access Key"
- Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Copy `terraform.tfvars.example` to `terraform.tfvars`.
- Configure `terraform.tfvars`. Common variables to configure are:
  - `aws_region`, `aws_subnet_zone`: Specify where AWS infrastructure is deployed
  - `prefix_list_id`, `prefix_list_region`: See Prefix List under Resource Setup.
  - `snapshot_user_aws_access_key_id`: See Snapshot S3 Bucket under Resource Setup
  - `snapshot_user_aws_secret_access_key`: See Snapshot S3 Bucket under Resource Setup
  - `s3_bucket_name`: See Snapshot S3 Bucket under Resource Setup
  - `workload`: Name of workload to run
  - `workload_params`: Parameters to configure a workload. See the `workload_params_default/` directory for standard parameters for each workload.
- `terraform workspace new <unique-name>` (e.g. `terraform workspace new rschirone`)
- `terraform init`

### Resource Setup
The following resources are not provisioned by terraform and must be created beforehand:
- Snapshot S3 bucket
- (Optional) Prefix list
#### Prefix List
You can optionally add the load generation machine to a [prefix list](https://docs.aws.amazon.com/vpc/latest/userguide/managed-prefix-lists.html) for IP-based access control to the data store. The prefix list resource must exist before deploying with terraform. Terraform will add the load generation IP to the prefix list, so that the benchmarking client can upload results to the data store.

To use a prefix list:
  - Set `prefix_list_id` to the prefix list's ID.
  - Set `prefix_list_region` to the prefix list's region.
  - The workspace name is used as a description for the prefix list entry

If you don't want to use a prefix list, you don't need to set `prefix_list_id` nor `prefix_list_region`.
#### Snapshot S3 Bucket
If you want to use snapshots for OS and ES, create an AWS S3 bucket.

Create an AWS S3 bucket with default settings. Then, create a `snapshot-user` to access the bucket.

Next, give the user access to the bucket by creating the following IAM policy and associating it with the user:
```
{
	"Statement": [
		{
			"Action": [
				"s3:ListBucket",
				"s3:GetBucketLocation",
				"s3:ListBucketMultipartUploads",
				"s3:ListBucketVersions"
			],
			"Effect": "Allow",
			"Resource": [
				"arn:aws:s3:::snapshots-osb",
			]
		},
		{
			"Action": [
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject",
				"s3:AbortMultipartUpload",
				"s3:ListMultipartUploadParts"
			],
			"Effect": "Allow",
			"Resource": [
				"arn:aws:s3:::snapshots-osb/*",
			]
		}
	],
	"Version": "2012-10-17"
}
```
Note: In this example policy, the bucket is named `snapshots-osb`.

Specify the following `terraform.tfvars` variables:
- `s3_bucket_name`:  S3 Bucket name
- `snapshot_user_aws_access_key_id`: `snapshot-user`'s access id
- `snapshot_user_aws_secret_access_key`: `snapshot-user`'s secret key

Here is some additional information on Snapshot Buckets for [ElasticSearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/repository-s3.html) and [OpenSearch](https://opensearch.org/docs/latest/tuning-your-cluster/availability-and-recovery/snapshots/index/).

## Usage
First run `terraform plan` to confirm that the appropriate resources will be created

Run `terraform apply` to deploy infrastructure.

To specify alternative workloads/parameters, you can run:
  - `terraform apply -var="workload=pmc" -var="workload_params=$(cat workload_params_default/pmc.json)"`
  - or`terraform apply -var-file=my-terraform.tfvars` if you have a different `tfvars` file.

The Terraform script is going to create several AWS EC2 instances. A `target-cluster` instance is used to host the product being benchmarked (e.g. OpenSearch). There may be additional cluster instances if the workload uses a multi-node deployment.

Use `terraform output` to get the IPs/hostnames of the instances.

Use `terraform output cluster-password` to get the password for the cluster.

## Destroy Instances

```shell
terraform destroy
```

## Identify AWS Resources in Use

```shell
./scripts/resources.sh
```

## Resources that should be created

### `provision-cluster.tf` Resources

* `aws_key_pair`: Key pair for providing access to the ec2 instance
* `aws_vpc`: The virtual private cloud 
* `aws_subnet`: The range ips for the VPC
* `aws_internet_gateway`: Provides path for traffic to VPC
* `aws_security_group`: Configures rules for VPC traffic. Also make sure that the ingress and egress rules are configured
* `aws_route_table`: Map for directing the network traffic
* `aws_route_table_association`: Defines how the route table directs its outgoing traffic

### `opensearch.tf` Module Resources

* Some aws ec2 instances should be created. One of them is for the main node, and there could be a couple more for additional cluster nodes.
