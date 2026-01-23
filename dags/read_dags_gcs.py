from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET_NAME = "velocity-dev-dags"

def list_gcs_objects():
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    objects = hook.list(bucket_name=BUCKET_NAME)

    # Log the objects so you can see them in the task logs
    if not objects:
        print(f"No objects found in bucket: {BUCKET_NAME}")
    else:
        print(f"Objects in bucket {BUCKET_NAME}:")
        for obj in objects:
            print(obj)

with DAG(
    dag_id="list_objects_in_velocity_dev_dags",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "airflow"},
) as dag:

    list_files = PythonOperator(
        task_id="list_gcs_objects",
        python_callable=list_gcs_objects,
    )

    list_files

# 1311
