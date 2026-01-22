from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSUploadObjectOperator

BUCKET_NAME = "dmt-dev-landing-zone"
OBJECT_NAME = "used_by_airflow"
FILE_CONTENT = "This file is used by Airflow.\n"

with DAG(
    dag_id="write_used_by_airflow_to_gcs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "airflow"},
) as dag:

    write_file_to_gcs = GCSUploadObjectOperator(
        task_id="write_used_by_airflow",
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        data=FILE_CONTENT,
        mime_type="text/plain",
        # If you have a connection configured, set it here, otherwise omit
        # gcp_conn_id="google_cloud_default",
    )

    write_file_to_gcs

# 009
