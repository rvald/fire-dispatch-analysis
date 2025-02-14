resource "google_storage_bucket_object" "default" {
  name         = "fdny-data-analysis-extract-dispatch-job.py"
  source       = "/home/rvald/fire_dispatch_analysis/terraform/assets/extract_jobs/fdny-data-analysis-extract-dispatch-job.py"
  bucket       = var.scripts_bucket
}

resource "google_dataproc_job" "ingestion_etl_job" {
    region  = var.region
    force_delete = true
    placement {
        cluster_name = var.dproc_cluster_name
    }

    # Define the PySpark job
    pyspark_config {
        main_python_file_uri = "gs://${var.scripts_bucket}/fdny-data-analysis-extract-dispatch-job.py"

        # Properties can include Spark configurations
        properties = {
            "spark.logConf" = "true"
        }

        # Arguments passed to the PySpark job
        args = [
            "--source_path=${var.source_path}",
            "--data_lake_bucket=${var.data_lake_bucket}"
        ]
    }
}