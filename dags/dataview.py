import duckdb
import pandas as pd

# Connect to the correct file
con = duckdb.connect("data/insurance.duckdb")

# List all tables
tables = con.execute("SHOW TABLES").fetchdf()
print(tables)

# Preview some data if available
if not tables.empty:
    first_table = tables.iloc[0, 0]
    print(f"\nPreview of {first_table}:")
    df = con.execute(f"SELECT * FROM {first_table} LIMIT 5").fetchdf()
    print(df)
else:
    print("No tables found in the database.")
