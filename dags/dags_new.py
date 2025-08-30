from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from google.cloud import storage
import os


# Function to extract & upload
def extract_and_upload():
    conn = psycopg2.connect(
        host="34.173.180.170",
        port="5432",
        database="postgres",
        user="postgres",
        password="Ready-de26"
    )

    tables = [
        "public.order_items",
        "public.order_reviews",
        "public.orders",
        "public.products",
        "public.product_category_name_translation"
    ]

    client = storage.Client()
    bucket_name = "ready-labs-postgres-to-gcs"
    bucket = client.bucket(bucket_name)

    for table in tables:
        print(f"Extracting {table} ...")
        df = pd.read_sql(f"SELECT * FROM {table};", conn)
        table_data = df.to_csv(index=False)

        blob = bucket.blob(f"rawan_DB1/{table}_{datetime.now().strftime('%Y%m%d')}.csv")
        blob.upload_from_string(table_data, content_type="text/csv")
        print(f"✅ {table} uploaded to GCS")

    conn.close()

# Airflow DAG
default_args = {
    "owner": "rawan",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_gcs_dag",
    default_args=default_args,
    start_date=datetime(2025, 8, 23),
    end_date=datetime(2025, 8, 27),
    schedule_interval='@daily',
    catchup=True
) as dag:

    task = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload,
    )
