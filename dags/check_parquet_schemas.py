import pandas as pd
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

default_args = {
    'owner': 'massmutual',
    'start_date': datetime(2025, 10, 11),
}

# Set up Airflow logger
logger = logging.getLogger(__name__)

def check_parquet_schemas():
    data_dir = '/opt/airflow/data'
    logger.info(f"Checking directory: {data_dir}")
    parquet_files = [f for f in os.listdir(data_dir) if f.endswith('.parquet')]
    
    if not parquet_files:
        logger.error("No Parquet files found in %s", data_dir)
        return
    
    for file in parquet_files:
        file_path = os.path.join(data_dir, file)
        logger.info(f"Schema for {file}:")
        try:
            df = pd.read_parquet(file_path)
            logger.info("\n" + str(df.dtypes))
            logger.info(f"Number of rows: {len(df)}")
        except Exception as e:
            logger.error(f"Error reading {file}: {str(e)}")

with DAG('check_parquet_schemas', default_args=default_args, schedule=None) as dag:
    check_schemas = PythonOperator(
        task_id='check_parquet_schemas',
        python_callable=check_parquet_schemas,
    )
