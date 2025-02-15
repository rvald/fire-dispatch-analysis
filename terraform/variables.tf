
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

variable "dispatch_table" {
  type        = string
  description = "Iceberg table name"
}

variable "iceberg_warehouse" {
  type        = string
  description = "Iceberg data warehouse lake"
}

variable "iceberg_catalog" {
  type        = string
  description = "Iceberg catalog name"
}

variable "bigquery_region" {
  type        = string
  description = "BigQuery dataset region"
}

variable "project" {
  type        = string
  description = "GCP project"
}