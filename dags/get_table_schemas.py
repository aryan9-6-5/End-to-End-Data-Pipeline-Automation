from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def get_schema_task():
    from get_schema import get_all_table_schemas
    get_all_table_schemas()

with DAG(
    'get_table_schemas',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    schema_task = PythonOperator(
        task_id='get_all_schemas',
        python_callable=get_schema_task
    )