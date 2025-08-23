from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
import psycopg2
import pandas as pd
from google.cloud import storage
import os

# Python function: Extract from Postgres and upload to GCS

def extract_postgres_to_gcs():
    # Connect to Postgres
    conn = psycopg2.connect(
        host="your_postgres_host",
        dbname="your_db",
        user="your_user",
        password="your_password",
        port="5432"
    )
    query = "SELECT * FROM payments;"   # change to your schema/table
    df = pd.read_sql(query, conn)
    conn.close()

    # Save locally as CSV
    local_file = "/tmp/payments.csv"
    df.to_csv(local_file, index=False)

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket("your-bucket-name")
    blob = bucket.blob("raw/payments.csv")
    blob.upload_from_filename(local_file)

# -----------------------------
# DAG definition
# -----------------------------
default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    "postgres_to_bq_pipeline",
    default_args=default_args,
    description="Extract from Postgres, store in GCS, load into BigQuery",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Extract Task
    extract_task = PythonOperator(
        task_id="extract_postgres_to_gcs",
        python_callable=extract_postgres_to_gcs,
    )

    # Load GCS → BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket="your-bucket-name",
        source_objects=["raw/payments.csv"],
        destination_project_dataset_table="your_project.your_dataset.payments",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",  # overwrite daily
        autodetect=True,
    )

    extract_task >> load_to_bq 
