
variable "bucket_name" {
    type = string
}

variable "location" {
  type = string
}

variable "environment" {
  type = string
}

variable "project_id" {
    type = string
}

variable "region" {
    type = string
}

variable "credentials_file" {
    type = string
}

variable "cluster_name" {
  type = string
}

variable "cluster_num_nodes" {
  type = number
  default = 1
}

variable "machine_type" {
  type = string
  default = "n1-standard-2"
}

variable "zone" {
  type = string
}
