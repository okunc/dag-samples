# filename: test_simple_dag2.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello from Airflow!")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="test_simple_dag2",
    default_args=default_args,
    description="A simple test DAG",
    schedule_interval="@daily",      # or None if you want to trigger manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,
    )

    # if you add more tasks, define dependencies like:
    # hello_task >> another_task
