
terraform {
  required_providers {
    google = {
        source = "hashicorp/google"
        version = "4.80.0"
    }
  }

  required_version = ">=0.13.0"
}


module "gcs_bucket" {
  source = "./modules/storage"
  bucket_name = var.bucket_name
  location = var.location
  environment = var.environment
}

module "gcs_staging_bucket" {
  source = "./modules/storage"
  bucket_name = "${var.bucket_name}-staging"
  location = var.location
  environment = var.environment
}

module "kubernetes_cluster" {
  source = "./modules/gke"
  cluster_name = var.cluster_name
  location = var.zone
  cluster_num_nodes = var.cluster_num_nodes
  machine_type = var.machine_type
  project_id = var.project_id
}


module "bigquery_dwh" {
  source            = "./modules/bigquery"
  project_id        = var.project_id
  dataset_file_path = "${path.module}/resources/dataset.json"
}
