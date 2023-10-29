
output "region" {
  value = var.region
}

output "k8s_cluster_name" {
  value = module.kubernetes_cluster.k8s_cluster_name
}

output "k8s_cluster_host" {
  value = module.kubernetes_cluster.k8s_cluster_host
}