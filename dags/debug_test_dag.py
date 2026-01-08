from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def hello():
    import os
    if os.environ.get("AIRFLOW_DEBUG") == "true":
        import debugpy
        debugpy.listen(("0.0.0.0", 5678))
        print("Waiting for debugger attach...")
        debugpy.wait_for_client()
        debugpy.breakpoint()
    
    print("Hello, Airflow!")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )
