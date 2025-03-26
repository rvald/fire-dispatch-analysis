from datetime import datetime, timedelta, date
import os

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
   

from airflow.utils.dates import days_ago

INGEST_DAY = date.today()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "start_date": datetime.now() - timedelta(days=1),
}

DATA_BUCKET_NAME = os.environ.get("DATA_BUCKET_NAME")
SCRIPTS_BUCKET_NAME = os.environ.get("SCRIPTS_BUCKET_NAME")
SOURCE_BUCKET_NAME = os.environ.get("SOURCE_BUCKET_NAME")
REGION = os.environ.get("REGION")
PROJECT = os.environ.get("PROJECT")
CLUSTER_NAME = os.environ.get("CLUSTER_NAME")

ICEBERG_CATALOG   = os.environ.get("ICEBERG_CATALOG")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE")
DISPATCH_TABLE    = os.environ.get("DISPATCH_TABLE")
BIGQUERY_REGION   = os.environ.get("BIGQUERY_REGION")
JAR_FILE_BUCKET   = os.environ.get("JAR_FILE_BUCKET")

@dag(
    default_args=default_args,
    schedule_interval='@monthly',  
    catchup=False,
    dag_id="fire_dispatch_analysis_pipeline_dag",
)

def fire_dispatch_analysis_pipeline():

    # `start` task based on a `DummyOperator`
    start = DummyOperator(task_id="start")

    PYSPARK_JOB = {
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://{}/fdny-data-analysis-extract-dispatch-job.py".format(SCRIPTS_BUCKET_NAME),

            "args": [
                "--source_path={}".format(SOURCE_BUCKET_NAME),
                "--data_lake_bucket={}".format(DATA_BUCKET_NAME)
            ],
        },
    }

    extract_dispatch_pyspark_job = DataprocSubmitJobOperator(
        task_id="extract_dispatch_pyspark_job", job=PYSPARK_JOB, region=REGION, project_id=PROJECT
    )


    PYSPARK_TRANSFORM_JOB = {
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://{}/fdny-data-analysis-transform-dispatch-job.py".format(SCRIPTS_BUCKET_NAME),
            
            "jar_file_uris": [
                "gs://{}/iceberg-spark-runtime-3.5_2.12-1.5.2.jar".format(JAR_FILE_BUCKET), 
                "gs://spark-lib/biglake/biglake-catalog-iceberg1.2.0-0.1.0-with-dependencies.jar"
            ],

            # Properties can include Spark configurations
            "properties": {
                "spark.logConf"                                                               : "true",
                "spark.sql.catalog.{}.blms_catalog".format(ICEBERG_CATALOG)                   : ICEBERG_CATALOG,
                "spark.sql.catalog.{}.gcp_project".format(ICEBERG_CATALOG)                    : PROJECT,
                "spark.sql.catalog.{}.catalog-impl".format(ICEBERG_CATALOG)                   : "org.apache.iceberg.gcp.biglake.BigLakeCatalog",
                "spark.sql.catalog.{}.gcp_location".format(ICEBERG_CATALOG)                   : BIGQUERY_REGION,
                "spark.sql.catalog.{}".format(ICEBERG_CATALOG)                                : "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.{}.warehouse".format(ICEBERG_CATALOG)                      : "gs://{}/iceberg_warehouse".format(ICEBERG_WAREHOUSE),
            },

            # Arguments passed to the PySpark job
            "args": [
                "--iceberg_catalog={}".format(ICEBERG_CATALOG),
                "--iceberg_warehouse=iceberg_warehouse",
                "--source_lake_bucket={}".format(DATA_BUCKET_NAME),
                "--dispatch_table={}".format(DISPATCH_TABLE),
                "--ingest_date=2025-02-24",
                "--project={}".format(PROJECT),
            ],
        },
    }

    transform_dispatch_pyspark_job = DataprocSubmitJobOperator(
        task_id="transform_dispatch_pyspark_job", job=PYSPARK_TRANSFORM_JOB, region=REGION, project_id=PROJECT
    )

    dbt_data_modeling_cloud_run = CloudRunExecuteJobOperator(
        task_id="dbt_data_modeling_cloud_run",
        project_id=PROJECT,
        region=REGION,
        job_name="fire-dispatch-modeling-dbt-on-cloud-run",
        deferrable=False,
    )

    # `end` task based on another `DummyOperator`
    end = DummyOperator(task_id="end")

    (
        start
        >> extract_dispatch_pyspark_job
        >> transform_dispatch_pyspark_job
        >> dbt_data_modeling_cloud_run
        >> end
    )

fire_dispatch_analysis_pipeline() 
   