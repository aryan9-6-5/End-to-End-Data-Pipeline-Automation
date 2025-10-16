import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://airflow:airflow@materials-postgres-1:5432/airflow"

def heal_data():
    engine = create_engine(DATABASE_URL)
    print("Healing invalid data...")

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE customers
            SET age = (SELECT AVG(age) FROM customers WHERE age IS NOT NULL)
            WHERE age IS NULL OR age < 18;
        """))

        conn.execute(text("""
            DELETE FROM policies
            WHERE customer_id NOT IN (SELECT customer_id FROM customers);
        """))

    print("Data healing done")