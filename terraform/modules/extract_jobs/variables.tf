variable "dproc_cluster_name" {
  type        = string
  description = "Dataproc cluster name"
}

variable "region" {
  type        = string
  description = "Dataproc cluster region"
  default     = "us-central1"
}

variable "data_lake_bucket" {
  type        = string
  description = "Data lake bucket name"
}

variable "scripts_bucket" {
  type        = string
  description = "Scripts bucket name"
}

variable "source_path" {
  type        = string
  description = "Data source path"
}