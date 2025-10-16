import duckdb
import os

RAW_PATH = r"D:\massmutual\cuisine-insight-io-main_replit\cuisine-insight-io-1\data"

def load_data():
    con = duckdb.connect("/tmp/insurance.duckdb")
    print("Connected to DuckDB")

    for file in os.listdir(RAW_PATH):
        if file.endswith(".parquet"):
            table_name = file.replace(".parquet", "")
            query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet('{os.path.join(RAW_PATH, file)}');
            """
            con.execute(query)
            print(f"Loaded {table_name}")

    con.close()
    print(" Data successfully loaded into DuckDB")
