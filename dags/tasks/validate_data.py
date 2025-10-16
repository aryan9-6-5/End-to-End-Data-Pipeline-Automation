import duckdb

def validate_data():
    con = duckdb.connect("/tmp/insurance.duckdb")
    print("Running validation checks...")

    invalid_customers = con.execute("""
        SELECT customer_id, age
        FROM customers
        WHERE age IS NULL OR age < 18;
    """).fetchall()

    invalid_policies = con.execute("""
        SELECT policy_id
        FROM policies
        WHERE customer_id NOT IN (SELECT customer_id FROM customers);
    """).fetchall()

    print(f"Invalid customers: {len(invalid_customers)}")
    print(f"Invalid policies: {len(invalid_policies)}")

    con.close()
    print(" Validation completed")
