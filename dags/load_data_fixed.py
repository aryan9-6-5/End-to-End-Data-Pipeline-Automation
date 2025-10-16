import pandas as pd
import os
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://airflow:airflow@materials-postgres-1:5432/airflow"

def load_data():
    engine = create_engine(DATABASE_URL)
    print("Connected to PostgreSQL")
    
    RAW_PATH = "/opt/airflow/data"
    
    for file in os.listdir(RAW_PATH):
        if file.endswith(".parquet"):
            table_name = file.replace(".parquet", "")
            df = pd.read_parquet(os.path.join(RAW_PATH, file))
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"Loaded {table_name} into PostgreSQL")
    
    print("Data successfully loaded into PostgreSQL")