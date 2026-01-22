from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def say_goodbye():
    print("=" * 50)
    print("GOODBYE from dag_goodbye!")
    print(f"Current time: {datetime.now()}")
    print("=" * 50)

with DAG(
    dag_id="dag_goodbye",
    default_args=default_args,
    start_date=datetime(2026, 1, 16),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="say_goodbye",
        python_callable=say_goodbye,
    )