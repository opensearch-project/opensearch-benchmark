locals {
  cluster_arch_map = {
    vectorsearch = "arm64"
  }
  default_cluster_arch = "x64"
  cluster_arch         = lookup(local.cluster_arch_map, var.workload, local.default_cluster_arch)
}

terraform {
  required_providers {
    aws = {
      source                = "hashicorp/aws"
      version               = "5.65.0"
      configuration_aliases = [aws.prefix_list_region]
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # start at 4 because first 4 addresses are reserved for AWS
  load_generation_private_ip = cidrhost(var.subnet_cidr_block, 4)
  cluster_node_private_ips = [
    cidrhost(var.subnet_cidr_block, 5),
    cidrhost(var.subnet_cidr_block, 6),
    cidrhost(var.subnet_cidr_block, 7)
  ]
  main_cluster_node_private_ip        = local.cluster_node_private_ips[0]
  nodes_type                          = var.workload == "vectorsearch" ? "multi" : "single"
  additional_nodes_idx                = var.workload == "vectorsearch" ? 1 : 3
  additional_cluster_node_private_ips = slice(local.cluster_node_private_ips, local.additional_nodes_idx, 3)
}


resource "aws_instance" "target-cluster-additional-nodes" {
  for_each               = toset(local.additional_cluster_node_private_ips)
  ami                    = var.cluster_ami_id
  instance_type          = var.cluster_instance_type
  key_name               = var.ssh_key_name
  vpc_security_group_ids = var.security_groups

  associate_public_ip_address = true

  subnet_id = var.subnet_id

  private_ip = each.key

  user_data = templatefile("${path.module}/os-cluster.yaml",
    {
      os_cluster_script      = yamlencode(base64gzip(file("${path.module}/os_cluster.sh"))),
      os_password            = var.password,
      os_version             = var.os_version,
      os_arch                = local.cluster_arch,
      os_snapshot_access_key = var.snapshot_user_aws_access_key_id,
      os_snapshot_secret_key = var.snapshot_user_aws_secret_access_key,
      authorized_ssh_key     = var.ssh_pub_key,
      jvm_options_2          = yamlencode(base64gzip(file("${path.module}/jvm.v2.options"))),
      jvm_options_3          = yamlencode(base64gzip(file("${path.module}/jvm.v3.options"))),
      cluster_ips            = join(",", local.cluster_node_private_ips),
      node_name              = format("node-%s", each.key),
      nodes_type             = local.nodes_type,
    }
  )
  user_data_replace_on_change = true

  private_dns_name_options {
    hostname_type = "resource-name"
  }

  provisioner "remote-exec" {
    inline = [
      "echo 'Waiting for user data script to finish'",
      "cloud-init status --wait > /dev/null",
      "echo 'User data script finished'",
    ]
  }

  connection {
    type        = "ssh"
    user        = "ubuntu"
    private_key = var.ssh_priv_key
    host        = self.public_ip
  }

  tags = var.tags
}

resource "aws_instance" "target-cluster-main-node" {
  ami                    = var.cluster_ami_id
  instance_type          = var.cluster_instance_type
  key_name               = var.ssh_key_name
  vpc_security_group_ids = var.security_groups

  associate_public_ip_address = true

  subnet_id = var.subnet_id

  private_ip = local.main_cluster_node_private_ip

  user_data = templatefile("${path.module}/os-cluster.yaml",
    {
      os_cluster_script      = yamlencode(base64gzip(file("${path.module}/os_cluster.sh"))),
      os_password            = var.password,
      os_version             = var.os_version,
      os_arch                = local.cluster_arch,
      os_snapshot_access_key = var.snapshot_user_aws_access_key_id,
      os_snapshot_secret_key = var.snapshot_user_aws_secret_access_key,
      authorized_ssh_key     = var.ssh_pub_key,
      jvm_options_2          = yamlencode(base64gzip(file("${path.module}/jvm.v2.options"))),
      jvm_options_3          = yamlencode(base64gzip(file("${path.module}/jvm.v3.options"))),
      cluster_ips            = join(",", local.cluster_node_private_ips),
      node_name              = "main-node",
      nodes_type             = local.nodes_type,
    }
  )
  user_data_replace_on_change = true

  private_dns_name_options {
    hostname_type = "resource-name"
  }

  provisioner "remote-exec" {
    inline = [
      "echo 'Waiting for user data script to finish'",
      "cloud-init status --wait > /dev/null",
      "echo 'User data script finished'",
    ]
  }

  connection {
    type        = "ssh"
    user        = "ubuntu"
    private_key = var.ssh_priv_key
    host        = self.public_ip
  }

  tags       = var.tags
  depends_on = [aws_instance.target-cluster-additional-nodes]
}

resource "aws_ec2_managed_prefix_list_entry" "prefix-list-entry-load-gen" {
  count          = length(var.prefix_list_id) > 0 ? 1 : 0
  provider       = aws.prefix_list_region
  cidr           = "${aws_instance.load-generation.public_ip}/32"
  description    = terraform.workspace
  prefix_list_id = var.prefix_list_id
}