from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy as sa
import os
import logging

default_args = {
    'owner': 'massmutual',
    'start_date': datetime(2025, 10, 11),
}

logger = logging.getLogger(__name__)

CUSTOMERS_COLUMNS = ['customer_id', 'first_name', 'last_name', 'gender', 'dob', 'age', 'email', 'phone', 'address', 'city', 'state', 'zip', 'created_at', 'last_login', 'risk_score', 'customer_status', 'segment', 'referral_source', 'preferred_contact', 'updated_at', 'created_by', 'updated_by']

POLICIES_COLUMNS = ['policy_id', 'customer_id', 'policy_type', 'coverage_amount', 'premium_amount', 'start_date', 'end_date', 'status', 'branch_code', 'assigned_agent_id', 'payment_frequency', 'renewal_date', 'beneficiary_count', 'currency', 'premium_due_date', 'created_at', 'updated_at', 'created_by', 'updated_by']

CLAIMS_COLUMNS = ['claim_id', 'policy_id', 'claim_date', 'claim_type', 'claim_amount', 'adjusted_amount', 'status', 'resolved_date', 'fraud_flag', 'delay_days', 'approved_by', 'notes', 'region', 'created_at', 'updated_at', 'created_by', 'updated_by']

PAYMENTS_COLUMNS = ['payment_id', 'policy_id', 'claim_id', 'transaction_date', 'amount', 'method', 'success_flag', 'refunded_amount', 'currency', 'payment_gateway', 'transaction_id', 'created_at', 'updated_at', 'created_by', 'updated_by']

def load_policy_types():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'policy_types.parquet')
    df = pd.read_parquet(file_path)
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('policy_types', engine, if_exists='replace', index=False, dtype={
        'type_id': sa.VARCHAR(50),
        'max_coverage': sa.DECIMAL(15,2),
        'created_at': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
    })
    logger.info(f"Loaded {len(df)} rows into policy_types table")

def load_payment_methods():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'payment_methods.parquet')
    df = pd.read_parquet(file_path)
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('payment_methods', engine, if_exists='replace', index=False, dtype={
        'method_id': sa.VARCHAR(50),
        'created_at': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
    })
    logger.info(f"Loaded {len(df)} rows into payment_methods table")

def load_currency_rates():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'currency_rates.parquet')
    df = pd.read_parquet(file_path)
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('currency_rates', engine, if_exists='replace', index=False, dtype={
        'currency': sa.VARCHAR(3),
        'conversion_rate': sa.DECIMAL(10,6),
        'date': sa.DATE,
        'created_at': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
    })
    logger.info(f"Loaded {len(df)} rows into currency_rates table")

def load_agents():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'agents.parquet')
    df = pd.read_parquet(file_path)
    df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce').dt.date
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('agents', engine, if_exists='replace', index=False, dtype={
        'agent_id': sa.VARCHAR(50),
        'branch_id': sa.VARCHAR(50),
        'hire_date': sa.DATE,
        'created_at': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
    })
    logger.info(f"Loaded {len(df)} rows into agents table")

def load_branches():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'branches.parquet')
    df = pd.read_parquet(file_path)
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('branches', engine, if_exists='replace', index=False, dtype={
        'branch_id': sa.VARCHAR(50),
        'manager_id': sa.VARCHAR(50),
        'created_at': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
    })
    logger.info(f"Loaded {len(df)} rows into branches table")

def load_customers():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'customers.parquet')
    df = pd.read_parquet(file_path)
    df = df[CUSTOMERS_COLUMNS]  # Select only matching columns
    df['customer_id'] = df['customer_id'].astype(str)
    df['dob'] = pd.to_datetime(df['dob'], errors='coerce').dt.date
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['last_login'] = pd.to_datetime(df['last_login'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    df['risk_score'] = pd.to_numeric(df['risk_score'], errors='coerce').astype('Int64')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('customers', engine, if_exists='replace', index=False, dtype={
        'customer_id': sa.VARCHAR(50),
        'dob': sa.DATE,
        'created_at': sa.TIMESTAMP,
        'last_login': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
        'risk_score': sa.INTEGER,
    })
    logger.info(f"Loaded {len(df)} rows into customers table")

def load_policies():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'policies.parquet')
    df = pd.read_parquet(file_path)
    df = df[POLICIES_COLUMNS]  # Select only matching columns
    df['policy_id'] = df['policy_id'].astype(str)
    df['customer_id'] = df['customer_id'].astype(str)
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.date
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.date
    df['renewal_date'] = pd.to_datetime(df['renewal_date'], errors='coerce').dt.date
    df['premium_due_date'] = pd.to_datetime(df['premium_due_date'], errors='coerce').dt.date
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('policies', engine, if_exists='replace', index=False, dtype={
        'policy_id': sa.VARCHAR(50),
        'customer_id': sa.VARCHAR(50),
        'coverage_amount': sa.DECIMAL(15,2),
        'premium_amount': sa.DECIMAL(10,2),
        'start_date': sa.DATE,
        'end_date': sa.DATE,
        'renewal_date': sa.DATE,
        'premium_due_date': sa.DATE,
        'created_at': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
    })
    logger.info(f"Loaded {len(df)} rows into policies table")

def load_claims():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'claims.parquet')
    df = pd.read_parquet(file_path)
    df = df[CLAIMS_COLUMNS]  # Select only matching columns
    df['claim_id'] = df['claim_id'].astype(str)  # Use VARCHAR for IDs
    df['policy_id'] = df['policy_id'].astype(str)
    df['claim_date'] = pd.to_datetime(df['claim_date'], errors='coerce').dt.date
    df['resolved_date'] = pd.to_datetime(df['resolved_date'], errors='coerce').dt.date
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    df['fraud_flag'] = df['fraud_flag'].astype(bool)
    df['delay_days'] = pd.to_numeric(df['delay_days'], errors='coerce').astype('Int64')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('claims', engine, if_exists='replace', index=False, dtype={
        'claim_id': sa.VARCHAR(50),
        'policy_id': sa.VARCHAR(50),
        'claim_amount': sa.DECIMAL(15,2),
        'adjusted_amount': sa.DECIMAL(15,2),
        'claim_date': sa.DATE,
        'resolved_date': sa.DATE,
        'fraud_flag': sa.BOOLEAN,
        'delay_days': sa.INTEGER,
        'created_at': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
    })
    logger.info(f"Loaded {len(df)} rows into claims table")

def load_payments():
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'payments.parquet')
    df = pd.read_parquet(file_path)
    df = df[PAYMENTS_COLUMNS]  # Select only matching columns
    df['payment_id'] = df['payment_id'].astype(str)  # Use VARCHAR for IDs
    df['policy_id'] = df['policy_id'].astype(str)
    df['claim_id'] = df['claim_id'].astype(str)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('payments', engine, if_exists='replace', index=False, dtype={
        'payment_id': sa.VARCHAR(50),
        'policy_id': sa.VARCHAR(50),
        'claim_id': sa.VARCHAR(50),
        'transaction_date': sa.TIMESTAMP,
        'amount': sa.DECIMAL(10,2),
        'refunded_amount': sa.DECIMAL(10,2),
        'created_at': sa.TIMESTAMP,
        'updated_at': sa.TIMESTAMP,
    })
    logger.info(f"Loaded {len(df)} rows into payments table")

def load_coverage_levels():
    df = pd.read_parquet('/opt/airflow/data/coverage_levels.parquet')
    engine = sa.create_engine('postgresql://massmutual_user:massmutual_pass@data-warehouse-postgres:5432/massmutual_warehouse')
    df.to_sql('coverage_levels', engine, if_exists='replace', index=False, dtype={
        'coverage_id': sa.VARCHAR(50),
        'level_name': sa.TEXT,
        'min_amount': sa.INTEGER,
        'max_amount': sa.INTEGER
    })
    logger.info(f"Loaded {len(df)} rows into coverage_levels table")

with DAG('load_massmutual_data', default_args=default_args, schedule=None) as dag:
    load_policy_types_task = PythonOperator(task_id='load_policy_types', python_callable=load_policy_types)
    load_payment_methods_task = PythonOperator(task_id='load_payment_methods', python_callable=load_payment_methods)
    load_currency_rates_task = PythonOperator(task_id='load_currency_rates', python_callable=load_currency_rates)
    load_agents_task = PythonOperator(task_id='load_agents', python_callable=load_agents)
    load_branches_task = PythonOperator(task_id='load_branches', python_callable=load_branches)
    load_customers_task = PythonOperator(task_id='load_customers', python_callable=load_customers)
    load_policies_task = PythonOperator(task_id='load_policies', python_callable=load_policies)
    load_claims_task = PythonOperator(task_id='load_claims', python_callable=load_claims)
    load_payments_task = PythonOperator(task_id='load_payments', python_callable=load_payments)
    load_coverage_levels_task = PythonOperator(task_id='load_coverage_levels', python_callable=load_coverage_levels)

    [load_policy_types_task, load_payment_methods_task, load_currency_rates_task, load_agents_task, load_branches_task] >> load_customers_task >> load_policies_task >> load_claims_task >> load_payments_task >> load_coverage_levels_task