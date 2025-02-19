from datetime import datetime, timedelta, date
import os

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
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


@dag(
    default_args=default_args,
    schedule_interval=timedelta(days=1),  
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

    # upload_extract_dispatch_job_cript_file = LocalFilesystemToGCSOperator(
    #     task_id = "upload_extract_dispatch_job_cript_file",
    #     src = "/home/rvald/fire_dispatch_analysis/terraform/assets/extract_jobs/fdny-data-analysis-extract-dispatch-job.py",
    #     dst = "fdny-data-analysis-extract-dispatch-job.py",
    #     bucket = SCRIPTS_BUCKET_NAME,
    # )

    extract_dispatch_pyspark_job = DataprocSubmitJobOperator(
        task_id="extract_dispatch_pyspark_job", job=PYSPARK_JOB, region=REGION, project_id=PROJECT
    )

    # `end` task based on another `DummyOperator`
    end = DummyOperator(task_id="end")

    (
        start
        >> extract_dispatch_pyspark_job
        >> end
    )

fire_dispatch_analysis_pipeline() 
   