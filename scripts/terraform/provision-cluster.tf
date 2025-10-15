terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.65.0"
    }

    random = {
      source  = "hashicorp/random"
      version = "3.6.2"
    }

    external = {
      source  = "hashicorp/external"
      version = "2.3.4"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Workspace = terraform.workspace
      Service   = "OSB"
    }
  }
}

provider "aws" {
  region = var.prefix_list_region
  alias  = "prefix_list_region"

  default_tags {
    tags = {
      Workspace = terraform.workspace
      Service   = "OSB"
    }
  }
}

# Created to access the ec2 instances
resource "tls_private_key" "ssh_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

# Save private key to a local file
resource "local_file" "private_key" {
  content         = tls_private_key.ssh_key.private_key_pem
  filename        = "${path.module}/private_key-${terraform.workspace}.pem"
  file_permission = "0600"
}

# Referred to for ssh`ing into the ec2 instance
resource "aws_key_pair" "ssh_key" {
  key_name   = "${terraform.workspace}-ssh-key"
  public_key = tls_private_key.ssh_key.public_key_openssh
}

/*
In these next steps, we are setting up the security
configurations for accessing the ec2 instance where 
OpenSearch is running. This is required for accessing
the EC2 instances through ssh.
*/
resource "aws_vpc" "vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
}

resource "aws_subnet" "subnet" {
  cidr_block        = cidrsubnet(aws_vpc.vpc.cidr_block, 3, 1)
  vpc_id            = aws_vpc.vpc.id
  availability_zone = var.aws_subnet_zone
}

resource "aws_internet_gateway" "gtw" {
  vpc_id = aws_vpc.vpc.id
}

resource "aws_security_group" "allow_osb" {
  name        = "${terraform.workspace}-allow-osb"
  description = "Allow ES/OS/OSB inbound traffic and all outbound traffic"
  vpc_id      = aws_vpc.vpc.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "allow_ssh" {
  security_group_id = aws_security_group.allow_osb.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 22
  ip_protocol       = "tcp"
  to_port           = 22
}

resource "aws_vpc_security_group_ingress_rule" "allow_es_cluster_traffic_9200" {
  security_group_id = aws_security_group.allow_osb.id
  cidr_ipv4         = "10.0.0.0/16"
  from_port         = 9200
  to_port           = 9200
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "allow_all_traffic_ipv4" {
  security_group_id = aws_security_group.allow_osb.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1" # semantically equivalent to all ports
}

resource "aws_route_table" "route-table-test-env" {
  vpc_id = aws_vpc.vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gtw.id
  }
}

resource "aws_route_table_association" "subnet-association" {
  subnet_id      = aws_subnet.subnet.id
  route_table_id = aws_route_table.route-table-test-env.id
}

data "aws_ec2_managed_prefix_list" "prefix-list" {
  count    = length(var.prefix_list_id) > 0 ? 1 : 0
  provider = aws.prefix_list_region
  id       = var.prefix_list_id
}

data "aws_ami" "ubuntu_ami_amd64" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"]
}

data "aws_ami" "ubuntu_ami_arm64" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"]
}

resource "random_password" "cluster-password" {
  length      = 16
  special     = false
  min_lower   = 1
  min_upper   = 0
  min_numeric = 1
}

data "external" "latest_snapshot_version" {
  program = ["python3", "${path.module}/get_latest_snapshot_version.py"]
  query = {
    s3_bucket_name        = var.s3_bucket_name
    cluster_type          = "OS"
    cluster_version       = var.os_version
    workload              = var.workload
    snapshot_version      = var.snapshot_version
  }
}

locals {
  # set instance types based on the workload
  # TODO set r*.4xlarge if vectorsearch with 10 million docs
  workload_cluster_instance_map = {
    vectorsearch = "r6gd.4xlarge"
  }
  workload_loadgen_instance_map = {
    vectorsearch = "c5d.4xlarge"
  }
  default_cluster_instance = "c5d.2xlarge"
  default_loadgen_instance = "c5d.2xlarge"
  cluster_instance_type    = lookup(local.workload_cluster_instance_map, var.workload, local.default_cluster_instance)
  loadgen_instance_type    = lookup(local.workload_loadgen_instance_map, var.workload, local.default_loadgen_instance)

  workload_cluster_ami_map = {
    vectorsearch = data.aws_ami.ubuntu_ami_arm64.id
  }
  workload_loadgen_ami_map = {
  }
  default_cluster_ami = data.aws_ami.ubuntu_ami_amd64.id
  default_loadgen_ami = data.aws_ami.ubuntu_ami_amd64.id
  cluster_ami_id      = lookup(local.workload_cluster_ami_map, var.workload, local.default_cluster_ami)
  loadgen_ami_id      = lookup(local.workload_loadgen_ami_map, var.workload, local.default_loadgen_ami)
}

module "os-cluster" {
  count = 1

  source                = "./modules/opensearch"
  cluster_instance_type = local.cluster_instance_type
  loadgen_instance_type = local.loadgen_instance_type
  cluster_ami_id        = local.cluster_ami_id
  loadgen_ami_id        = local.loadgen_ami_id
  os_version            = var.os_version
  distribution_version  = var.distribution_version
  ssh_key_name          = aws_key_pair.ssh_key.key_name
  ssh_priv_key          = tls_private_key.ssh_key.private_key_openssh
  ssh_pub_key           = tls_private_key.ssh_key.public_key_openssh
  security_groups       = [aws_security_group.allow_osb.id]
  subnet_id             = aws_subnet.subnet.id
  subnet_cidr_block     = aws_subnet.subnet.cidr_block
  password              = random_password.cluster-password.result
  prefix_list_id        = var.prefix_list_id
  workload              = var.workload
  osb_version           = var.osb_version

  s3_bucket_name                      = var.s3_bucket_name
  snapshot_version                    = data.external.latest_snapshot_version.result.latest_version
  snapshot_user_aws_access_key_id     = var.snapshot_user_aws_access_key_id
  snapshot_user_aws_secret_access_key = var.snapshot_user_aws_secret_access_key
  workload_params                     = var.workload_params
  test_procedure                      = var.test_procedure

  providers = {
    aws                    = aws
    aws.prefix_list_region = aws.prefix_list_region
  }

  tags = {
    Name = "target-cluster"
  }
}
