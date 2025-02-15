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