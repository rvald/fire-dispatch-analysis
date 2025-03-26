# Fire Dispatch Analysis

This project is designed to analyze fire dispatch data using a modern data pipeline architecture. It integrates data extraction, transformation, and modeling workflows to generate insights into fire dispatch response times and other key metrics. Below is an overview of the project's components, architecture, and workflow.

---

## Project Structure

### 1. **Data Extraction**
- **Purpose**: Extract raw fire dispatch data from a source bucket and load it into a data lake.
- **Key Components**:
  - **Script**: [`fdny-data-analysis-extract-dispatch-job.py`](terraform/assets/extract_jobs/fdny-data-analysis-extract-dispatch-job.py)
  - **Terraform Module**: [`extract_jobs`](terraform/modules/extract_jobs)
  - **Airflow Task**: `extract_dispatch_pyspark_job` in [`fire_dispatch_analysis_pipeline.py`](dags/fire_dispatch_analysis_pipeline.py)

### 2. **Data Transformation**
- **Purpose**: Transform raw data into a structured format and load it into an Iceberg table for further analysis.
- **Key Components**:
  - **Script**: [`fdny-data-analysis-transform-dispatch-job.py`](terraform/assets/transform_jobs/fdny-data-analysis-transform-dispatch-job.py)
  - **Terraform Module**: [`transform_jobs`](terraform/modules/transform_jobs)
  - **Airflow Task**: `transform_dispatch_pyspark_job` in [`fire_dispatch_analysis_pipeline.py`](dags/fire_dispatch_analysis_pipeline.py)

### 3. **Data Modeling**
- **Purpose**: Use dbt to create a star schema for analytical queries.
- **Key Components**:
  - **Models**: Located in [`fire_dispatch_modeling/models`](fire_dispatch_modeling/models)
  - **dbt Project Configuration**: [`dbt_project.yml`](fire_dispatch_modeling/dbt_project.yml)
  - **Airflow Task**: `dbt_data_modeling_cloud_run` in [`fire_dispatch_analysis_pipeline.py`](dags/fire_dispatch_analysis_pipeline.py)

### 4. **Data Serving**
- **Purpose**: Serve transformed data to BigQuery for downstream analytics and reporting.
- **Key Components**:
  - **Script**: [`fdny-data-analysis-serving-job.py`](terraform/assets/serving/fdny-data-analysis-serving-job.py)
  - **Terraform Module**: [`serving`](terraform/modules/serving)

---

## Architecture

The project follows a modular architecture with the following key components:

1. **Data Lake**: Stores raw and transformed data in Google Cloud Storage.
2. **Dataproc**: Executes PySpark jobs for data extraction, transformation, and serving.
3. **Iceberg Table**: Stores structured data for efficient querying and partitioning.
4. **BigQuery**: Serves as the final destination for analytical queries and reporting.
5. **dbt**: Implements a star schema for data modeling and analytical views.

---

## Workflow

1. **Data Extraction**:
   - Raw data is extracted from a source bucket using a PySpark job.
   - The data is written to a landing zone in the data lake.

2. **Data Transformation**:
   - The extracted data is transformed using PySpark.
   - Transformations include schema enforcement, timestamp conversion, and metadata addition.
   - The transformed data is loaded into an Iceberg table.

3. **Data Modeling**:
   - dbt is used to create a star schema with dimensions and fact tables.
   - Analytical views are built to calculate metrics like average response times.

4. **Data Serving**:
   - Transformed data is served to BigQuery for downstream analytics.
   - This step ensures the data is accessible for reporting and visualization.

---

## Key Features

- **Modular Design**: Each stage of the pipeline is encapsulated in its own Terraform module and PySpark script.
- **Scalability**: Leverages Google Cloud Dataproc and BigQuery for handling large datasets.
- **dbt Integration**: Implements best practices for data modeling and schema management.
- **Automation**: Orchestrated using Apache Airflow for end-to-end automation.

---

## Analytical Outputs

- **Star Schema**:
  - Fact Table: `fact_response_times`
  - Dimension Tables: `dim_locations`, `dim_alarm_levels`, `dim_dates`

- **Analytical View**:
  - Average response time from dispatch to arrival, grouped by geography and time.
