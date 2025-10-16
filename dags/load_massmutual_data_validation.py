from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

def validate_data():
    conn = psycopg2.connect(
        host="data-warehouse-postgres",  # Match service name from docker-compose.yml
        database="massmutual_warehouse",
        user="massmutual_user",
        password="massmutual_pass",
        port="5432"  # Internal port
    )
    cur = conn.cursor()
    validations = {}

    # Validate customers
    cur.execute("SELECT COUNT(*) FROM customers WHERE age <= 18 OR customer_id IS NULL OR email !~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'")
    validations["customers"] = cur.fetchone()[0]
    # Validate policies
    cur.execute("SELECT COUNT(*) FROM policies WHERE policy_id IS NULL OR start_date > CURRENT_DATE")
    validations["policies"] = cur.fetchone()[0]
    # Validate claims
    cur.execute("SELECT COUNT(*) FROM claims WHERE claim_amount <= 0 OR claim_id IS NULL")
    validations["claims"] = cur.fetchone()[0]
    # Validate payments
    cur.execute("SELECT COUNT(*) FROM payments WHERE payment_id IS NULL OR amount <= 0")
    validations["payments"] = cur.fetchone()[0]
    # Validate agents
    cur.execute("SELECT COUNT(*) FROM agents WHERE agent_id IS NULL OR hire_date > CURRENT_DATE")
    validations["agents"] = cur.fetchone()[0]
    # Validate branches (using city as location proxy)
    cur.execute("SELECT COUNT(*) FROM branches WHERE branch_id IS NULL OR TRIM(city) = ''")
    validations["branches"] = cur.fetchone()[0]
    # Validate policy_types
    cur.execute("SELECT COUNT(*) FROM policy_types WHERE type_id IS NULL OR TRIM(description) = ''")
    validations["policy_types"] = cur.fetchone()[0]
    # Validate payment_methods
    cur.execute("SELECT COUNT(*) FROM payment_methods WHERE method_id IS NULL OR TRIM(method_name) = ''")
    validations["payment_methods"] = cur.fetchone()[0]
    # Validate currency_rates
    cur.execute("SELECT COUNT(*) FROM currency_rates WHERE currency IS NULL OR conversion_rate <= 0")
    validations["currency_rates"] = cur.fetchone()[0]
    # Validate coverage_levels
    cur.execute("SELECT COUNT(*) FROM coverage_levels WHERE coverage_id IS NULL OR min_amount > max_amount")
    validations["coverage_levels"] = cur.fetchone()[0]

    # Log and raise error if issues found
    for table, count in validations.items():
        print(f"Validation Results - {table}: {count} issues")
        if count > 0:
            raise ValueError(f"Validation failed for {table} with {count} issues.")

    cur.close()
    conn.close()

with DAG(
    'load_massmutual_data_validation',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    validate_task = PythonOperator(
        task_id='validate_all_tables',
        python_callable=validate_data
    )
    # Add dependency if needed
    # validate_task.set_upstream(existing_load_tasks)