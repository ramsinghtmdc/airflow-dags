from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the Python function
def hello_airflow():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    dag_id="hello_airflow_dag",
    description="A simple DAG that prints Hello, Airflow!",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    task = PythonOperator(
        task_id="print_hello",
        python_callable=hello_airflow,
    )

