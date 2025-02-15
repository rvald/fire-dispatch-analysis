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
        jar_file_uris = ["gs://spark-lib/biglake/biglake-catalog-iceberg1.2.0-0.1.1-with-dependencies.jar"]
        

        # Properties can include Spark configurations
        properties = {
            "spark.logConf" = "true"
            "spark.sql.catalog.${var.iceberg_catalog}.blms_catalog"                  = var.iceberg_catalog
            "spark.sql.catalog.${var.iceberg_catalog}.gcp_project"                   = var.project
            "spark.jars.packages"                                                    = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.4.3,org.apache.spark:spark-avro_2.13:3.5.1"
            "spark.sql.catalog.${var.iceberg_catalog}.catalog-impl"                  = "org.apache.iceberg.gcp.biglake.BigLakeCatalog"
            "spark.sql.catalog.${var.iceberg_catalog}.gcp_location"                  = var.bigquery_region
            "spark.sql.catalog.${var.iceberg_catalog}"                               = "org.apache.iceberg.spark.SparkCatalog"
            "spark.sql.catalog.${var.iceberg_catalog}.warehouse"                     = "gs://${var.iceberg_warehouse}/iceberg_warehouse"
        }

        # Arguments passed to the PySpark job
        args = [
            "--iceberg_catalog=${var.iceberg_catalog}",
            "--iceberg_warehouse=${var.iceberg_warehouse}",
            "--source_lake_bucket=${var.data_lake_bucket}",
            "--dispatch_table=${var.dispatch_table}",
            "--ingest_date=2025-02-14",
            "--project=${var.project}"
        ]
    }
}