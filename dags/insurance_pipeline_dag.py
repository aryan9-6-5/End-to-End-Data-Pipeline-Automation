from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks.load_data_fixed import load_data
from tasks.validate_data_fixed import validate_data
from tasks.heal_data_fixed import heal_data
from tasks.transform_data_fixed import transform_data

with DAG(
    dag_id="insurance_data_pipeline",
    start_date=datetime(2025, 10, 10),
    schedule_interval="@daily",
    catchup=False,
    tags=["insurance", "data_pipeline"],
) as dag:

    t1 = PythonOperator(task_id="load_data", python_callable=load_data)
    t2 = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t3 = PythonOperator(task_id="heal_data", python_callable=heal_data)
    t4 = PythonOperator(task_id="transform_data", python_callable=transform_data)

    t1 >> t2 >> t3 >> t4