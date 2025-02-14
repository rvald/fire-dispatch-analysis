resource "google_storage_bucket_object" "default" {
  name         = "fdny-data-analysis-transform-dispatch-job.py"
  source       = "/home/rvald/fire_dispatch_analysis/terraform/assets/transform_jobs/fdny-data-analysis-transform-dispatch-job.py"
  bucket       = var.scripts_bucket
}

resource "google_dataproc_job" "transform_etl_job" {
    region  = var.region
    force_delete = true
    placement {
        cluster_name = var.dproc_cluster_name
    }

    # Define the PySpark job
    pyspark_config {
        main_python_file_uri = "gs://${var.scripts_bucket}/fdny-data-analysis-transform-dispatch-job.py"

        # Properties can include Spark configurations
        properties = {
            "spark.logConf" = "true"
        }

        # Arguments passed to the PySpark job
        args = [
            "--catalog_database=${var.catalog_database}",
            "--dispatch_table=${var.dispatch_table}",
            "--source_bucket_path=${var.data_lake_bucket}",
            "--target_bucket_path=${var.data_lake_bucket}",
            "--ingest_date=2025-02-14"
        ]
    }
}