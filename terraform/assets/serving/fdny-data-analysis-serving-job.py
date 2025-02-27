import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *



# Argument parsing 
parser = argparse.ArgumentParser()
parser.add_argument('--source_iceberg_bucket',  required=True)
parser.add_argument('--project',                required=True)
parser.add_argument('--ingest_date',            required=True)

args = parser.parse_args()


source_iceberg_bucket     = args.source_iceberg_bucket
project                   = args.project
ingest_date               = args.ingest_date

temporaryGcsBucket = "dataproc-temp-us-central1-894249367486-ffvw30tv"

# Initialize Spark session in Dataproc
spark = SparkSession.builder \
    .config("spark.jars.packages","com.google.cloud.spark:spark-bigquery-with-dependencies_2.12-0.26.0") \
    .appName('serve-dispatch-iceberg-table-to-bigquery') \
    .getOrCreate()

spark.conf.set("temporaryGcsBucket",temporaryGcsBucket)


# Grab data from bucket
source_gcs_path = (
    f"gs://{source_iceberg_bucket}/iceberg_warehouse/iceberg_warehouse.db/biglake_dispatch_iceberg/data/INGEST_ON={ingest_date}/*.parquet"
)


# Read a transformed iceberg data
df = spark.read.parquet(source_gcs_path)

# Save to bigquery warehouse
df.write.format("bigquery") \
    .option("table", "fdny_data_analysis_warehouse.fire_dispatch_serving_data") \
    .mode("overwrite") \
    .save()


spark.stop()