import duckdb

def heal_data():
    con = duckdb.connect("/tmp/insurance.duckdb")
    print("Healing invalid data...")

    # Fill missing ages with average
    con.execute("""
        UPDATE customers
        SET age = (SELECT AVG(age) FROM customers WHERE age IS NOT NULL)
        WHERE age IS NULL OR age < 18;
    """)

    # Remove orphan policies
    con.execute("""
        DELETE FROM policies
        WHERE customer_id NOT IN (SELECT customer_id FROM customers);
    """)

    con.close()
    print(" Data healing done")
