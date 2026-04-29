import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

RAW_BUCKET = "retail-legacy-dev-raw-mykhailo-2026"
TRANSFORMED_BUCKET = "retail-legacy-dev-transformed-mykhailo-2026"
GLUE_JOB_NAME = "retail-legacy-processing-dev-glue-transform"

def _check_csv_parity(): 
    import polars as pl
    local_path = "/opt/airflow/project/data/raw/source_1150208_to_1171119_part.csv"
    s3_path = f"s3://{RAW_BUCKET}/raw/source_1150208_to_1171119_part.csv"
    
    # Use scan_csv + pl.len() to prevent Out-Of-Memory on 8GB Macs!
    local_count = pl.scan_csv(local_path, separator="|", ignore_errors=True, quote_char=None).select(pl.len()).collect().item()
    s3_count = pl.scan_csv(s3_path, separator="|", ignore_errors=True, quote_char=None, storage_options={"aws_region": "us-east-1"}).select(pl.len()).collect().item()
    
    print(f"Local CSV: {local_count} rows | S3 CSV: {s3_count} rows")
    if local_count != s3_count:
        raise ValueError("Row count mismatch between Local and S3 CSV!")

def _check_parquet_parity():
    import polars as pl
    s3_csv_path = f"s3://{RAW_BUCKET}/raw/source_1150208_to_1171119_part.csv"
    s3_parquet_path = f"s3://{TRANSFORMED_BUCKET}/gold/fact_sales.parquet"
    
    csv_count = pl.scan_csv(s3_csv_path, separator="|", ignore_errors=True, quote_char=None, storage_options={"aws_region": "us-east-1"}).select(pl.len()).collect().item()
    parquet_count = pl.scan_parquet(s3_parquet_path, storage_options={"aws_region": "us-east-1"}).select(pl.len()).collect().item()
    
    print(f"S3 CSV: {csv_count} rows | Fact Sales Parquet: {parquet_count} rows")
    if parquet_count == 0:
        raise ValueError("Parquet file is empty!")
    if parquet_count > csv_count:
        raise ValueError("Parquet row count exceeds source CSV! Duplication detected.")

with DAG(
    'retail_aws_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['retail', 'aws'],
) as dag:

    # Task 1: Ingest and Clean CSV (Runs your ingest.py script)
    ingest_task = BashOperator(
        task_id='ingest_and_upload_to_s3',
        bash_command=f'python /opt/airflow/project/scripts/ingest.py --bucket {RAW_BUCKET}',
    )

    # Task 2: Trigger AWS Glue Transformation
    glue_task = GlueJobOperator(
        task_id='transform_raw_to_parquet',
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True,
        region_name='us-east-1',
    )

    # Task 3: Compare Local CSV vs Uploaded S3 CSV
    csv_parity_task = PythonOperator(
        task_id='compare_csv_vs_uploaded_csv',
        python_callable=_check_csv_parity,
    )

    # Task 4: Compare S3 CSV vs Gold Parquet
    parquet_parity_task = PythonOperator(
        task_id='compare_csv_vs_parquet',
        python_callable=_check_parquet_parity,
    )

    # Define the execution order
    ingest_task >> glue_task >> [csv_parity_task, parquet_parity_task]