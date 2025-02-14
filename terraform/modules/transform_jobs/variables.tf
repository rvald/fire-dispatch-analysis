variable "dproc_cluster_name" {
  type        = string
  description = "Dataproc cluster name"
}

variable "region" {
  type        = string
  description = "Dataproc cluster region"
  default     = "us-central1"
}

variable "scripts_bucket" {
  type        = string
  description = "Scripts bucket name"
}


variable "catalog_database" {
  type        = string
  description = "Iceberg catalog table db name"
}

variable "dispatch_table" {
  type        = string
  description = "Iceberg table name"
}

variable "data_lake_bucket" {
  type        = string
  description = "Data source path"
}