from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello():
    print("Hello, World!")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(seconds=10),
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="print_hello",
        python_callable=hello,
    )
