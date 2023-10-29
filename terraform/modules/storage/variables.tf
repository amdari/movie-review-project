
variable "bucket_name" {
  type = string
  description = "A GCS bucket name"
}

variable "location" {
    type = string
}

variable "storage_class" {
    type = string
    default = "STANDARD"
}


variable "environment" {
    type = string
    description = "Infra Environment"
}
