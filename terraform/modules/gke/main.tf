
resource "google_container_cluster" "gke_cluster" {
  name = var.cluster_name
  location = var.location
  remove_default_node_pool = true
  initial_node_count = 1
}

resource "google_container_node_pool" "gke_node_pool" {
  name = "${google_container_cluster.gke_cluster.name}-node-pool"
  location = var.location
  cluster = google_container_cluster.gke_cluster.name
  node_count = var.cluster_num_nodes

  node_config {
    machine_type = var.machine_type
    oauth_scopes = [
        "https://www.googleapis.com/auth/logging.write",
        "https://www.googleapis.com/auth/monitoring"
    ]

    labels = {
      env = var.project_id
    }

    tags = ["${var.project_id}-gke", "gke-node"]

    metadata = {
      disable-legacy-endpoints = true
    }
  }


}