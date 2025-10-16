import pandas as pd
import os
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://airflow:airflow@materials-postgres-1:5432/airflow"

def load_data():
    engine = create_engine(DATABASE_URL)
    print("Connected to PostgreSQL")
    
    # Updated path - data is in root data/ directory
    RAW_PATH = "/opt/airflow/data"
    
    # List all parquet files in the data directory
    parquet_files = [f for f in os.listdir(RAW_PATH) if f.endswith('.parquet')]
    print(f"Found {len(parquet_files)} parquet files: {parquet_files}")
    
    for file in parquet_files:
        table_name = file.replace(".parquet", "")
        file_path = os.path.join(RAW_PATH, file)
        
        print(f"Loading {file} as table '{table_name}'...")
        
        try:
            # Read parquet file
            df = pd.read_parquet(file_path)
            print(f"  Read {len(df)} rows from {file}")
            
            # Load into PostgreSQL
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"   Successfully loaded {table_name} into PostgreSQL")
            
        except Exception as e:
            print(f"   Error loading {file}: {str(e)}")
    
    print("Data loading completed")