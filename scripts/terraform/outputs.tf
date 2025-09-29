output "target-cluster-ip" {
  value = module.os-cluster[0].os-cluster-ip
}

output "additional-cluster-ips" {
  value = module.os-cluster[0].os-additional-cluster-ips
}

output "load-generation-ip" {
  value = module.os-cluster[0].load-generation-ip
}

output "cluster-password" {
  value     = random_password.cluster-password.result
  sensitive = true
}

output "snapshot-version" {
  value = data.external.latest_snapshot_version.result.latest_version
}

output "ssh_private_key_file" {
  value = local_file.private_key.filename
}