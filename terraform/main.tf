module "extract_job" {
  source = "./modules/extract_jobs"

  region              = var.region
  source_path         = var.source_path
  data_lake_bucket    = var.data_lake_bucket
  scripts_bucket      = var.scripts_bucket
  dproc_cluster_name  = var.dproc_cluster_name
}

module "transform_job" {
  source = "./modules/transform_jobs"

  region              = var.region
  scripts_bucket      = var.scripts_bucket
  dproc_cluster_name  = var.dproc_cluster_name
  data_lake_bucket    = var.data_lake_bucket

  catalog_database    = var.catalog_database
  dispatch_table      = var.dispatch_table
 
  depends_on = [module.extract_job]
}