variable "aws_region" {
  description = "AWS region used for the deployment"
  type        = string
}

variable "aws_subnet_zone" {
  description = "AWS subnet availability zone, tied to the aws_region used"
  type        = string
}

variable "prefix_list_id" {
  description = "ID of prefix list resource to add the load generation IP to"
  default     = ""
  type        = string
}

variable "prefix_list_region" {
  description = "Region of prefix list resource"
  default     = "us-east-1"
  type        = string
}

variable "os_version" {
  description = "Version of OpenSearch to deploy"
  type        = string
  default     = "3.0.0-beta1-nightly-11019"
}

variable "distribution_version" {
  description = "OSB distribution-version to use"
  type        = string
  default     = "3.0.0-beta1-nightly-11019"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for the snapshot"
  type        = string
  default     = "snapshots-osb"
}

variable "snapshot_user_aws_access_key_id" {
  description = "value of the AWS_ACCESS_KEY_ID for the snapshot user"
  type        = string
  sensitive   = true
}

variable "snapshot_user_aws_secret_access_key" {
  description = "value of the AWS_SECRET_ACCESS_KEY for the snapshot user"
  type        = string
  sensitive   = true
}

variable "workload_params" {
  description = "Workload parameters for the cluster"
  type        = string
  default     = <<EOF
{
	"number_of_replicas": 0,
	"bulk_indexing_clients": 1,
	"target_throughput": 0
}
EOF
}

variable "test_procedure" {
  description = "Test procedure for Workload to run"
  type        = string
  default     = ""
}

variable "workload" {
  description = "Workload to deploy"
  type        = string
  default     = "big5"
}

variable "snapshot_version" {
  description = "Version of the snapshot to deploy (latest, new, or a specific version)"
  type        = string
  default     = "latest"
  validation {
    condition     = can(regex("^(latest|new)$", var.snapshot_version)) || can(regex("^\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}$", var.snapshot_version))
    error_message = "Snapshot version must be one of: latest, new, or a specific version (YYYY-mm-dd_HH-MM-ss)"
  }
}

variable "osb_version" {
  description = "OpenSearch Benchmark version"
  type        = string
  default     = "1.15.0"
}