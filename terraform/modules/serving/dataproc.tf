resource "google_storage_bucket_object" "default" {
  name         = "fdny-data-analysis-serving-job.py"
  source       = "/home/rvald/fire_dispatch_analysis/terraform/assets/serving/fdny-data-analysis-serving-job.py"
  bucket       = var.scripts_bucket
}


resource "google_dataproc_job" "serving_job" {
    region  = var.region
    force_delete = true
    placement {
        cluster_name = var.dproc_cluster_name
    }

    # Define the PySpark job
    pyspark_config {
        main_python_file_uri = "gs://${var.scripts_bucket}/fdny-data-analysis-serving-job.py"
        
        # Properties can include Spark configurations
        properties = {
            "spark.logConf" = "true"  
        }

        # Arguments passed to the PySpark job
        args = [
            "--source_iceberg_bucket=${var.iceberg_warehouse}",
            "--ingest_date=2025-02-27",
            "--project=${var.project}"
        ]
    }
}