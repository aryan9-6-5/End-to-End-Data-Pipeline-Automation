import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://airflow:airflow@materials-postgres-1:5432/airflow"

def transform_data():
    engine = create_engine(DATABASE_URL)
    print("Running transformations...")

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS policy_claim_summary;"))
        
        conn.execute(text("""
            CREATE TABLE policy_claim_summary AS
            SELECT
                p.policy_id,
                c.customer_id,
                COALESCE(SUM(cl.claim_amount), 0) AS total_claims,
                COUNT(cl.claim_id) AS claim_count
            FROM policies p
            LEFT JOIN claims cl ON p.policy_id = cl.policy_id
            LEFT JOIN customers c ON p.customer_id = c.customer_id
            GROUP BY p.policy_id, c.customer_id;
        """))

    print("Created policy_claim_summary table")