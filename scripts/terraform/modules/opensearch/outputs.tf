output "os-cluster-ip" {
  value = aws_instance.target-cluster-main-node.public_dns
}

output "os-additional-cluster-ips" {
  value = [for resource in aws_instance.target-cluster-additional-nodes : resource.public_dns]
}
