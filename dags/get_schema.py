import psycopg2
from psycopg2 import Error

def get_all_table_schemas():
    try:
        # Database connection parameters
        conn = psycopg2.connect(
            host="data-warehouse-postgres",  # Match service name from docker-compose.yml
            database="massmutual_warehouse",
            user="massmutual_user",
            password="massmutual_pass",
            port="5432"  # Internal port
        )
        cur = conn.cursor()

        # Query to get all tables and their columns
        cur.execute("""
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """)

        # Group results by table
        results = cur.fetchall()
        current_table = None
        for row in results:
            table_name, column_name, data_type, nullable = row
            if current_table != table_name:
                if current_table is not None:
                    print()  # Newline between tables
                print(f"Table: {table_name}")
                current_table = table_name
            print(f"  Column: {column_name}, Type: {data_type}, Nullable: {nullable}")

        cur.close()
        conn.close()
    except Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    get_all_table_schemas()