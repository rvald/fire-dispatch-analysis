import sys
from datetime import date
from pyspark.sql import SparkSession
import argparse

# Argument parsing 
parser = argparse.ArgumentParser()
parser.add_argument('--source_path',      required=True)
parser.add_argument('--data_lake_bucket', required=True)
args = parser.parse_args()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataIngestionJob") \
    .getOrCreate()

# Configuration for Cloud SQL
source_path = args.source_path
data_lake_bucket = args.data_lake_bucket

source_gcs_path = (
    f"gs://{source_path}"
)

source_df = spark.read.csv(source_gcs_path)

# Compute ingest date
ingest_day = date.today()
gcs_path = (
    f"gs://{data_lake_bucket}/landing_zone/fire_dispatch/ingest_on={ingest_day.strftime('%Y_%m_%d')}"
)

# Write DataFrame to Google Cloud Storage in CSV format
source_df.write.mode("overwrite").csv(gcs_path)

spark.stop()