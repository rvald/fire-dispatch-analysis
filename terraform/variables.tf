
variable "region" {
    type        = string
    description = "GCP region"
    default     = "us-central1"
}

variable "data_lake_bucket" {
  type        = string
  description = "Data lake bucket name"
}

variable "scripts_bucket" {
  type        = string
  description = "Dataproc scripts bucket name"
}

variable "source_path" {
  type        = string
  description = "Data source path"
}

variable "dproc_cluster_name" {
  type        = string
  description = "Dataproc cluster name"
}

variable "catalog_database" {
  type        = string
  description = "Iceberg catalog table db name"
}

variable "dispatch_table" {
  type        = string
  description = "Iceberg table name"
}