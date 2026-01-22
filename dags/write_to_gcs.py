from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET_NAME = "dmt-dev-landing-zone"
OBJECT_NAME = "used_by_airflow"
FILE_CONTENT = "This file is used by Airflow.\n"

def write_to_gcs(**context):
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        data=FILE_CONTENT,
        mime_type="text/plain",
    )

with DAG(
    dag_id="write_used_by_airflow_to_gcs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "airflow"},
) as dag:

    write_file = PythonOperator(
        task_id="write_used_by_airflow",
        python_callable=write_to_gcs,
    )

    write_file

# 2230
