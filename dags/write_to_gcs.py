from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateObjectOperator

# # Adjust this to your Airflow connection ID for GCP
# GCP_CONN_ID = "google_cloud_default"

with DAG(
    dag_id="write_to_gcs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # or set a cron, e.g. "0 1 * * *"
    catchup=False,
    default_args={
        "owner": "airflow",
    },
) as dag:

    write_file_to_gcs = GCSCreateObjectOperator(
        task_id="write_used_by_airflow",
        bucket_name="dmt-dev-landing-zone",
        object_name="used_by_airflow",
        data="This file is used by Airflow.\n",
        mime_type="text/plain",
        gcp_conn_id=GCP_CONN_ID,
    )

    write_file_to_gcs
