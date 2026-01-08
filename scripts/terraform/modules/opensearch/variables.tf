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