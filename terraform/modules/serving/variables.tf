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

variable "iceberg_warehouse" {
  type        = string
  description = "Iceberg data warehouse lake"
}

variable "project" {
  type        = string
  description = "GCP project"
}