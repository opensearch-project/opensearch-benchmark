variable "aws_region" {
  description = "AWS region used for the deployment"
  type        = string
}

variable "aws_subnet_zone" {
  description = "AWS subnet availability zone, tied to the aws_region used"
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