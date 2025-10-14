variable "cluster_instance_type" {
  description = "Instance type for the cluster"
  type        = string
}

variable "loadgen_instance_type" {
  description = "Instance type for the load generation machine"
  type        = string
}

variable "cluster_ami_id" {
  description = "AMI ID to use for the cluster"
  type        = string
}

variable "loadgen_ami_id" {
  description = "AMI ID to use for the load generation machine"
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
}

variable "ssh_key_name" {
  description = "Name of the SSH key to use for the cluster"
  type        = string
}

variable "ssh_priv_key" {
  description = "SSH Private Key"
  type        = string
}

variable "ssh_pub_key" {
  description = "SSH Pub Key"
  type        = string
}

variable "security_groups" {
  description = "List of security groups to apply to the OS instance"
  type        = list(string)
}

variable "subnet_id" {
  description = "Subnet ID"
  type        = string
}

variable "subnet_cidr_block" {
  description = "Subnet CIDR Block"
  type        = string
}

variable "tags" {
  description = "List of Tags to apply to resources"
  type        = any
}

variable "password" {
  description = "Password for the OS cluster"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 bucket name for the OS snapshot"
  type        = string
  default     = ""
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

variable "prefix_list_id" {
  description = "ID of prefix list resource to add the load generation IP to"
  type        = string
}

variable "workload_params" {
  description = "Workload parameters to pass to the ingest and benchmark scripts"
  type        = string
  default     = ""
}

variable "test_procedure" {
  description = "Test procedure for Workload to run"
  type        = string
  default     = ""
}

variable "workload" {
  description = "Workload to run on the cluster"
  type        = string
  default     = "big5"
}

variable "snapshot_version" {
  description = "Specific version of the snapshot to restore"
  type        = string
  validation {
    condition     = can(regex("^\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}$", var.snapshot_version))
    error_message = "Snapshot version must be: YYYY-mm-dd_HH-MM-ss"
  }
}

variable "osb_version" {
  description = "OpenSearch Benchmark version"
  type        = string
}