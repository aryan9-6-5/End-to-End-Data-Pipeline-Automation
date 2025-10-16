import duckdb

def transform_data():
    con = duckdb.connect("/tmp/insurance.duckdb")
    print("Running transformations...")

    con.execute("""
        CREATE OR REPLACE TABLE policy_claim_summary AS
        SELECT
            p.policy_id,
            c.customer_id,
            SUM(cl.claim_amount) AS total_claims,
            COUNT(cl.claim_id) AS claim_count
        FROM policies p
        LEFT JOIN claims cl ON p.policy_id = cl.policy_id
        LEFT JOIN customers c ON p.customer_id = c.customer_id
        GROUP BY p.policy_id, c.customer_id;
    """)

    print(" Created policy_claim_summary table")
    con.close()
