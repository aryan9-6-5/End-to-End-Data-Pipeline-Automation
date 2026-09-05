from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'massmutual',
    'start_date': datetime(2025, 10, 1),
}

with DAG(
    dag_id='master_massmutual_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['master', 'orchestration', 'massmutual'],
    description='Master DAG to orchestrate Load -> Heal -> Transform in exact sequence'
) as dag:

    trigger_load = TriggerDagRunOperator(
        task_id='trigger_load_data',
        trigger_dag_id='load_massmutual_data',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True
    )

    trigger_heal = TriggerDagRunOperator(
        task_id='trigger_heal_data',
        trigger_dag_id='heal_massmutual_data',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True
    )

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform_data',
        trigger_dag_id='transform_massmutual_manual',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True
    )

    trigger_load >> trigger_heal >> trigger_transform
