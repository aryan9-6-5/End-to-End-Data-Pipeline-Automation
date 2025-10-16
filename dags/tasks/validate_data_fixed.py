import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://airflow:airflow@materials-postgres-1:5432/airflow"

def validate_data():
    engine = create_engine(DATABASE_URL)
    print("Running validation checks...")

    with engine.connect() as conn:
        invalid_customers = conn.execute(text("""
            SELECT customer_id, age
            FROM customers
            WHERE age IS NULL OR age < 18;
        """)).fetchall()

        invalid_policies = conn.execute(text("""
            SELECT policy_id
            FROM policies
            WHERE customer_id NOT IN (SELECT customer_id FROM customers);
        """)).fetchall()

    print(f"Invalid customers: {len(invalid_customers)}")
    print(f"Invalid policies: {len(invalid_policies)}")
    print("Validation completed")