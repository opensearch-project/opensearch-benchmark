# Provisioning Cluster

## Setup
### Environment Setup
- Install `terraform`. [Installation Guide](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
- Install AWS CLI
- In the AWS Console, go to "Security Credentials" and create a new "Access Key"
- Set the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Copy `terraform.tfvars.example` to `terraform.tfvars`.
- Configure `terraform.tfvars`. Common variables to configure are:
  - `aws_region`, `aws_subnet_zone`: Specify where AWS infrastructure is deployed
- `terraform init`
- `terraform workspace new <unique-name>` (e.g. `terraform workspace new rschirone`)

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

* One aws ec2 instances should be created for a single-node cluster.
