import sys
from datetime import datetime
import argparse

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *



# Argument parsing 
parser = argparse.ArgumentParser()
parser.add_argument('--catalog_database',   required=True)
parser.add_argument('--dispatch_table',     required=True)
parser.add_argument('--source_bucket_path', required=True)
parser.add_argument('--target_bucket_path', required=True)
parser.add_argument('--ingest_date',        required=True)
args = parser.parse_args()

source_lake_bucket = args.source_bucket_path
target_lake_bucket = args.target_bucket_path
ingest_date        = args.ingest_date
catalog_database   = args.catalog_database
dispatch_table     = args.dispatch_table

# Initialize Spark session in Dataproc
conf = (
    SparkConf()
    .setAppName('injest-fire-dispatch-iceberg-table')
    .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .set('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')
    .set('spark.sql.catalog.spark_catalog.type', 'hive')
    .set(f'spark.sql.catalog.dev', 'org.apache.iceberg.spark.SparkCatalog')
    .set(f'spark.sql.catalog.dev.type', 'hive')
    .set(f'spark.sql.warehouse.dir', f"gs://{target_lake_bucket}/transform_zone")
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

date_object = datetime.strptime(ingest_date, "%Y-%m-%d")
ingest_date_str = date_object.strftime("%Y_%m_%d")

source_gcs_path = (
    f"gs://{source_lake_bucket}/landing_zone/fire_dispatch/ingest_on={ingest_date_str}"
)


fire_dispatch_schema = StructType([
    StructField('STARFIRE_INCIDENT_ID', StringType(), True),
    StructField('INCIDENT_DATETIME', StringType(), True),
    StructField('ALARM_BOX_BOROUGH', StringType(), True),
    StructField('ALARM_BOX_NUMBER', IntegerType(), True),
    StructField('ALARM_BOX_LOCATION', StringType(), True),
    StructField('INCIDENT_BOROUGH', StringType(), True),
    StructField('ZIPCODE', StringType(), True),
    StructField('POLICEPRECINCT', IntegerType(), True),
    StructField('CITYCOUNCILDISTRICT', IntegerType(), True),
    StructField('COMMUNITYDISTRICTT', IntegerType(), True),
    StructField('COMMUNITYSCHOOLDISTRICT', IntegerType(), True),
    StructField('CONGRESSIONALDISTRICT', IntegerType(), True),
    StructField('ALARM_SOURCE_DESCRIPTION_TX', StringType(), True),
    StructField('ALARM_LEVEL_INDEX_DESCRIPTION', StringType(), True),
    StructField('HIGHEST_ALARM_LEVEL', StringType(), True),
    StructField('INCIDENT_CLASSIFICATION', StringType(), True),
    StructField('INCIDENT_CLASSIFICATION_GROUP', StringType(), True),
    StructField('DISPATCH_RESPONSE_SECONDS_QY', IntegerType(), True),
    StructField('FIRST_ASSIGNMENT_DATETIME', StringType(), True),
    StructField('FIRST_ACTIVATION_DATETIME', StringType(), True),
    StructField('FIRST_ON_SCENE_DATETIME', StringType(), True),
    StructField('INCIDENT_CLOSE_DATETIME', StringType(), True),
    StructField('VALID_DISPATCH_RSPNS_TIME_INDC', StringType(), True),
    StructField('VALID_INCIDENT_RSPNS_TIME_INDC', StringType(), True),
    StructField('INCIDENT_RESPONSE_SECONDS_QY', IntegerType(), True),
    StructField('INCIDENT_TRAVEL_TM_SECONDS_QY', IntegerType(), True),
    StructField('ENGINES_ASSIGNED_QUANTITY', IntegerType(), True),
    StructField('LADDERS_ASSIGNED_QUANTITY', IntegerType(), True),
    StructField('OTHER_UNITS_ASSIGNED_QUANTITY', IntegerType(), True)
])

df = spark.read.csv(source_gcs_path, schema=fire_dispatch_schema)

# Add Timestamp
# For this script, one of the transformations that you are going to add is the casting the string datetime to a timestamp
# for the columns referenced below.

# Define datetime format string compatible with your data
datetime_format = "MM/dd/yyyy hh:mm:ss a"

datetime_cols = [
    'INCIDENT_DATETIME', 
    'FIRST_ASSIGNMENT_DATETIME', 
    'FIRST_ACTIVATION_DATETIME', 
    'FIRST_ON_SCENE_DATETIME', 
    'INCIDENT_CLOSE_DATETIME'
]

# Create a list of column transformations
transformed_columns = [
    to_timestamp(col(column), datetime_format).alias(column) if column in datetime_cols else col(column)
    for column in df.columns
]

# Apply transformations
df = df.select(*transformed_columns)

# Add Metadata
# For this script, one of the transformations that you are going to add is the creation of a new column named `"ingest_on"`
# Use the `withColumn()` method to create that column
# Use the `F.to_date()` and `F.lit()` PySpark functions applied over the `ingest_date` object to convert it from string into date
df = df.withColumn(
    "ingest_on", F.to_date(F.lit(ingest_date), "yyyy-MM-dd")
).withColumn("source_from", F.lit("gcp_cloud_storage"))

# Check if Iceberg table exists
table_exists = spark.sql(f"SHOW TABLES IN {catalog_database}").filter(F.col("tableName") == dispatch_table).count() > 0

if table_exists:
    print(f"Appending data to table {dispatch_table}")
    df.writeTo(f"{catalog_database}.{dispatch_table}").using("iceberg").tableProperty(
        "format-version", "2"
    ).partitionedBy("ingest_on").append()
else:
    print(f"Creating table {dispatch_table}")
    df.writeTo(f"{catalog_database}.{dispatch_table}").using("iceberg").tableProperty(
        "format-version", "2"
    ).partitionedBy("ingest_on").createOrReplace()


spark.stop()