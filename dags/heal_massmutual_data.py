from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import psycopg2
from psycopg2 import sql
import logging
import time

logger = logging.getLogger(__name__)

class DataHealer:
    def __init__(self):
        self.conn = None
        self.cur = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host="data-warehouse-postgres",
                database="massmutual_warehouse",
                user="massmutual_user",
                password="massmutual_pass",
                port="5432",
                connect_timeout=10
            )
            self.conn.autocommit = False
            self.cur = self.conn.cursor()
            self.cur.execute("SET work_mem = '256MB'")
            self.cur.execute("SET maintenance_work_mem = '512MB'")
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    def close(self):
        """Safely close database connection"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
    
    def execute_query(self, query, description, params=None):
        """Execute a single query with logging and timing"""
        start_time = time.time()
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            rows_affected = self.cur.rowcount
            logger.info(f"{description}: {rows_affected} rows affected in {time.time() - start_time:.2f} seconds")
            return rows_affected
        except Exception as e:
            logger.error(f"Error in {description}: {e}")
            self.conn.rollback()
            raise
    
    def quarantine_rows(self, table, select_query, delete_using_subquery, join_key, description, error_message="Data quarantined due to rule violation"):
        """Move problematic rows to quarantine table and delete them using DELETE ... USING"""
        # Quarantine the full rows
        insert_query = sql.SQL("""
            INSERT INTO quarantine (table_name, row_data, error_message)
            SELECT %s, to_jsonb(t.*), %s
            FROM ({}) t
        """).format(sql.SQL(select_query))
        self.execute_query(insert_query, f"Quarantine {description}", (table, error_message))
        
        # Delete the same rows using DELETE ... USING syntax
        delete_query = sql.SQL("""
            DELETE FROM {table}
            USING ({subquery}) sub
            WHERE {table}.{join_key} = sub.{join_key}
        """).format(
            table=sql.Identifier(table),
            subquery=sql.SQL(delete_using_subquery),
            join_key=sql.Identifier(join_key)
        )
        self.execute_query(delete_query, f"Delete quarantined {description}")

    def fix_customer_data(self):
        """Fix customer-related data quality issues"""
        logger.info("Starting customer data fixes")
        start_time = time.time()
        
        # Batch fix customer issues
        self.execute_query("""
            WITH invalid_emails AS (
                SELECT customer_id FROM customers WHERE email LIKE 'invalid@%'
            ),
            invalid_scores AS (
                SELECT customer_id FROM customers WHERE risk_score < 0 OR risk_score > 100
            ),
            temporal_issues AS (
                SELECT customer_id FROM customers WHERE last_login < created_at
            )
            UPDATE customers c
            SET 
                email = CASE WHEN c.customer_id IN (SELECT customer_id FROM invalid_emails) THEN NULL ELSE LOWER(email) END,
                risk_score = CASE WHEN c.customer_id IN (SELECT customer_id FROM invalid_scores) THEN NULL ELSE risk_score END,
                last_login = CASE WHEN c.customer_id IN (SELECT customer_id FROM temporal_issues) THEN created_at ELSE last_login END,
                phone = CASE WHEN phone = '' OR phone IS NULL THEN '000-000-0000' ELSE phone END
            WHERE c.customer_id IN (
                SELECT customer_id FROM invalid_emails
                UNION ALL SELECT customer_id FROM invalid_scores
                UNION ALL SELECT customer_id FROM temporal_issues
            )
        """, "Batch fix customer data quality issues")
        
        # Remove duplicate customers (keep oldest by created_at) with quarantine
        self.quarantine_rows(
            "customers",
            """
                SELECT c.* FROM customers c
                WHERE c.customer_id IN (
                    SELECT customer_id FROM (
                        SELECT customer_id,
                               ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at ASC) as rn
                        FROM customers WHERE email IS NOT NULL
                    ) t WHERE rn > 1
                )
            """,
            """
                SELECT customer_id FROM (
                    SELECT customer_id,
                           ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at ASC) as rn
                    FROM customers WHERE email IS NOT NULL
                ) t WHERE rn > 1
            """,
            "customer_id",
            "duplicate customers by email"
        )
        
        logger.info(f"Customer fixes completed in {time.time() - start_time:.2f} seconds")

    def fix_policy_data(self):
        """Fix policy-related data quality issues"""
        logger.info("Starting policy data fixes")
        start_time = time.time()
        
        # Batch fix policy issues
        self.execute_query("""
            UPDATE policies
            SET 
                premium_amount = ABS(premium_amount),
                end_date = CASE WHEN end_date < start_date THEN start_date + INTERVAL '1 year' ELSE end_date END,
                renewal_date = CASE WHEN renewal_date < CURRENT_DATE THEN CURRENT_DATE + INTERVAL '1 year' ELSE renewal_date END
            WHERE premium_amount <= 0 
                OR end_date < start_date 
                OR renewal_date < CURRENT_DATE
        """, "Batch fix policy data quality issues")
        
        # Remove overlapping active policies with quarantine
        self.quarantine_rows(
            "policies",
            """
                SELECT p1.* FROM policies p1 
                JOIN policies p2 ON p1.customer_id = p2.customer_id 
                    AND p1.policy_id < p2.policy_id
                    AND p1.status = 'Active' AND p2.status = 'Active'
                    AND p1.start_date <= p2.end_date AND p1.end_date >= p2.start_date
                WHERE p1.policy_id IN (
                    SELECT policy_id FROM (
                        SELECT p1.policy_id as policy_id,
                            ROW_NUMBER() OVER (PARTITION BY p1.customer_id ORDER BY p1.start_date ASC) as rn
                        FROM policies p1
                        JOIN policies p2 ON p1.customer_id = p2.customer_id 
                            AND p1.policy_id < p2.policy_id
                            AND p1.status = 'Active' AND p2.status = 'Active'
                            AND p1.start_date <= p2.end_date AND p1.end_date >= p2.start_date
                    ) t WHERE rn > 1
                )
            """,
            """
                SELECT policy_id FROM (
                    SELECT p1.policy_id as policy_id,
                           ROW_NUMBER() OVER (PARTITION BY p1.customer_id ORDER BY p1.start_date ASC) as rn
                    FROM policies p1
                    JOIN policies p2 ON p1.customer_id = p2.customer_id 
                        AND p1.policy_id < p2.policy_id
                        AND p1.status = 'Active' AND p2.status = 'Active'
                        AND p1.start_date <= p2.end_date AND p1.end_date >= p2.start_date
                ) t WHERE rn > 1
            """,
            "policy_id",
            "overlapping active policies"
        )
        
        # Remove policies with non-existent customers with quarantine
        self.quarantine_rows(
            "policies",
            "SELECT * FROM policies WHERE NOT EXISTS (SELECT 1 FROM customers WHERE customers.customer_id = policies.customer_id)",
            "SELECT policy_id FROM policies WHERE NOT EXISTS (SELECT 1 FROM customers WHERE customers.customer_id = policies.customer_id)",
            "policy_id",
            "policies with invalid customers"
        )
        
        logger.info(f"Policy fixes completed in {time.time() - start_time:.2f} seconds")

    def fix_claims_data(self):
        """Fix claims-related data quality issues"""
        logger.info("Starting claims data fixes")
        start_time = time.time()
        
        # Batch fix claim amounts and fraud flags
        self.execute_query("""
            UPDATE claims c
            SET 
                claim_amount = CASE 
                    WHEN c.claim_amount <= 0 THEN ABS(c.claim_amount)
                    WHEN c.claim_amount > p.coverage_amount THEN p.coverage_amount
                    ELSE c.claim_amount 
                END,
                fraud_flag = CASE 
                    WHEN c.fraud_flag = TRUE AND (c.delay_days <= 30 OR c.adjusted_amount <= c.claim_amount) THEN FALSE
                    ELSE c.fraud_flag 
                END
            FROM policies p
            WHERE c.policy_id = p.policy_id
            AND (c.claim_amount <= 0 
                OR c.claim_amount > p.coverage_amount 
                OR (c.fraud_flag = TRUE AND (c.delay_days <= 30 OR c.adjusted_amount <= c.claim_amount)))
        """, "Batch fix claim amounts and fraud flags")
        
        # Remove invalid claims with quarantine
        self.quarantine_rows(
            "claims",
            "SELECT * FROM claims WHERE NOT EXISTS (SELECT 1 FROM policies WHERE policies.policy_id = claims.policy_id)",
            "SELECT claim_id FROM claims WHERE NOT EXISTS (SELECT 1 FROM policies WHERE policies.policy_id = claims.policy_id)",
            "claim_id",
            "claims with invalid policies"
        )
        
        # Remove claims outside policy date range with quarantine
        self.quarantine_rows(
            "claims",
            """
                SELECT * FROM claims c
                WHERE NOT EXISTS (
                    SELECT 1 FROM policies p 
                    WHERE c.policy_id = p.policy_id
                    AND c.claim_date BETWEEN p.start_date AND p.end_date
                )
            """,
            """
                SELECT claim_id FROM claims c
                WHERE NOT EXISTS (
                    SELECT 1 FROM policies p 
                    WHERE c.policy_id = p.policy_id
                    AND c.claim_date BETWEEN p.start_date AND p.end_date
                )
            """,
            "claim_id",
            "claims outside policy date range"
        )
        
        logger.info(f"Claims fixes completed in {time.time() - start_time:.2f} seconds")

    def fix_payments_data(self):
        """Fix payments-related data quality issues"""
        logger.info("Starting payments data fixes")
        start_time = time.time()
        
        # Remove payments for cancelled policies and invalid claims with quarantine
        self.quarantine_rows(
            "payments",
            """
                SELECT * FROM payments p
                WHERE NOT EXISTS (SELECT 1 FROM policies WHERE policies.policy_id = p.policy_id AND policies.status != 'Cancelled')
                OR NOT EXISTS (SELECT 1 FROM claims WHERE claims.claim_id = p.claim_id)
            """,
            """
                SELECT payment_id FROM payments p
                WHERE NOT EXISTS (SELECT 1 FROM policies WHERE policies.policy_id = p.policy_id AND policies.status != 'Cancelled')
                OR NOT EXISTS (SELECT 1 FROM claims WHERE claims.claim_id = p.claim_id)
            """,
            "payment_id",
            "payments for cancelled policies and invalid claims"
        )
        
        # Batch fix transaction dates and success flags
        self.execute_query("""
            UPDATE payments p
            SET 
                transaction_date = CASE 
                    WHEN p.transaction_date < c.resolved_date THEN c.resolved_date
                    ELSE p.transaction_date 
                END,
                success_flag = COALESCE(p.success_flag, 0)
            FROM claims c
            WHERE p.claim_id = c.claim_id
            AND (p.transaction_date < c.resolved_date OR p.success_flag IS NULL)
        """, "Batch fix transaction dates and success flags")
        
        logger.info(f"Payments fixes completed in {time.time() - start_time:.2f} seconds")

    def run(self):
        """Execute all data healing operations"""
        try:
            self.connect()
            self.fix_customer_data()
            self.fix_policy_data()
            self.fix_claims_data()
            self.fix_payments_data()
            self.conn.commit()
            logger.info("All data healing operations completed successfully")
        except Exception as e:
            logger.error(f"Data healing failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.close()


def heal_all_data():
    """Single task that runs all healing operations sequentially"""
    healer = DataHealer()
    try:
        healer.run()
    finally:
        healer.close()


with DAG(
    'heal_massmutual_data',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data-quality', 'massmutual'],
    max_active_runs=1
) as dag:
    heal_task = PythonOperator(
        task_id='heal_all_tables',
        python_callable=heal_all_data,
        retries=2,
        retry_delay=60
    )