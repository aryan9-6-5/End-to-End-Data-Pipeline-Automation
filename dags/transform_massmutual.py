from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

dag = DAG(
    'transform_massmutual_manual',
    start_date=datetime(2025, 10, 12),
    schedule_interval=None,
    catchup=False,
    tags=['manual', 'massmutual']
)

transform_data = PostgresOperator(
    task_id='run_transformations',
    postgres_conn_id='massmutual_postgres',
    sql='sql/transform.sql',
    dag=dag
)

transform_data