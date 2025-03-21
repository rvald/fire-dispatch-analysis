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
parser.add_argument('--iceberg_catalog',     required=True)
parser.add_argument('--iceberg_warehouse',   required=True)
parser.add_argument('--source_lake_bucket',  required=True)
parser.add_argument('--dispatch_table',      required=True)
parser.add_argument('--ingest_date',         required=True)
parser.add_argument('--project',             required=True)

args = parser.parse_args()

iceberg_catalog        = args.iceberg_catalog
iceberg_warehouse      = args.iceberg_warehouse
source_lake_bucket     = args.source_lake_bucket
dispatch_table         = args.dispatch_table
ingest_date            = args.ingest_date
project                = args.project



# Initialize Spark session in Dataproc
conf = (
    SparkConf()
    .setAppName('ingest-fire-dispatch-iceberg-table')
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

# Fix typo in COMMUNITYDISTRICTT
df = df.withColumnRenamed("COMMUNITYDISTRICTT", "COMMUNITYDISTRICT")

# Add Metadata
# For this script, one of the transformations that you are going to add is the creation of a new column named `"ingest_on"`
# Use the `withColumn()` method to create that column
# Use the `F.to_date()` and `F.lit()` PySpark functions applied over the `ingest_date` object to convert it from string into date
df = df.withColumn(
    "INGEST_ON", F.to_date(F.lit(ingest_date), "yyyy-MM-dd")
).withColumn("SOURCE_FROM", F.lit("gcp_cloud_storage"))

# Convert schema to SQL column definition
def schema_to_sql(schema):
    return ', '.join(["{} {}".format(field.name, field.dataType.simpleString().upper()) for field in schema])

# Column definitions from schema
column_definitions = schema_to_sql(df.schema)

# SQL for Iceberg Initialization
spark.sql(f"DROP NAMESPACE IF EXISTS `{iceberg_catalog}`;")
spark.sql(f"DROP NAMESPACE IF EXISTS `{iceberg_catalog}`.`{iceberg_warehouse}`;")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{iceberg_catalog}`;")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{iceberg_catalog}`.`{iceberg_warehouse}`;")
spark.sql(f"DROP TABLE IF EXISTS `{iceberg_catalog}`.`{iceberg_warehouse}`.biglake_dispatch_iceberg;")

# Create an Iceberg Table using the fire_dispatch_schema
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{iceberg_catalog}`.`{iceberg_warehouse}`.biglake_dispatch_iceberg
    ({column_definitions})
    USING iceberg
    PARTITIONED BY (INGEST_ON)
    TBLPROPERTIES (bq_table='fire_dispatch_dataset.biglake_dispatch_iceberg', bq_connection='us.fdny-data-analysis-biglake-connection')
""")

# Create a temporary view from the DataFrame
df.createOrReplaceTempView("temp_view_fire_dispatch")

# Insert into the Iceberg table using SQL
spark.sql(f"""
    INSERT INTO {iceberg_catalog}.{iceberg_warehouse}.biglake_dispatch_iceberg
    SELECT 
        STARFIRE_INCIDENT_ID,
        INCIDENT_DATETIME,
        ALARM_BOX_BOROUGH,
        ALARM_BOX_NUMBER,
        ALARM_BOX_LOCATION,
        INCIDENT_BOROUGH,
        ZIPCODE,
        POLICEPRECINCT,
        CITYCOUNCILDISTRICT,
        COMMUNITYDISTRICT,
        COMMUNITYSCHOOLDISTRICT,
        CONGRESSIONALDISTRICT,
        ALARM_SOURCE_DESCRIPTION_TX,
        ALARM_LEVEL_INDEX_DESCRIPTION,
        HIGHEST_ALARM_LEVEL,
        INCIDENT_CLASSIFICATION,
        INCIDENT_CLASSIFICATION_GROUP,
        DISPATCH_RESPONSE_SECONDS_QY,
        FIRST_ASSIGNMENT_DATETIME,
        FIRST_ACTIVATION_DATETIME,
        FIRST_ON_SCENE_DATETIME,
        INCIDENT_CLOSE_DATETIME,
        VALID_DISPATCH_RSPNS_TIME_INDC,
        VALID_INCIDENT_RSPNS_TIME_INDC,
        INCIDENT_RESPONSE_SECONDS_QY,
        INCIDENT_TRAVEL_TM_SECONDS_QY,
        ENGINES_ASSIGNED_QUANTITY,
        LADDERS_ASSIGNED_QUANTITY,
        OTHER_UNITS_ASSIGNED_QUANTITY,
        INGEST_ON,
        SOURCE_FROM
    FROM temp_view_fire_dispatch;
""")


spark.stop()