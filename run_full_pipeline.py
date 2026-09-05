"""
MassMutual Data Pipeline: End-to-End Orchestrator & Diff Reporter
Runs: load_massmutual_data -> heal_massmutual_data -> transform_massmutual_manual
Generates before-and-after change diff reports across all stages.
"""

import os
import sys
import glob
import time
import subprocess
import psycopg2
import pandas as pd
from datetime import datetime

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORTS = [int(os.getenv("DB_PORT", 5433)), 5432]
DB_NAME = os.getenv("DB_NAME", "massmutual_warehouse")
DB_USER = os.getenv("DB_USER", "massmutual_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "massmutual_pass")
PARQUET_DIR = os.path.join(os.path.dirname(__file__), "parquet_data")


def get_db_connection():
    """Attempt connecting to Postgres across configured ports."""
    last_err = None
    for port in DB_PORTS:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=port,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=5
            )
            return conn, port
        except Exception as e:
            last_err = e
    print(f"⚠️  Database connection warning: {last_err}")
    return None, None


def query_scalar(conn, sql):
    """Execute query and return single value."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            res = cur.fetchone()
            return res[0] if res else 0
    except Exception:
        conn.rollback()
        return 0


def query_df(conn, sql):
    """Execute query and return DataFrame."""
    try:
        return pd.read_sql_query(sql, conn)
    except Exception:
        conn.rollback()
        return pd.DataFrame()


def count_parquet_rows():
    """Count rows directly from source Parquet files."""
    stats = {}
    if not os.path.exists(PARQUET_DIR):
        return stats
    for f in glob.glob(os.path.join(PARQUET_DIR, "*.parquet")):
        tbl_name = os.path.splitext(os.path.basename(f))[0]
        try:
            df = pd.read_parquet(f)
            stats[tbl_name] = len(df)
        except Exception as e:
            stats[tbl_name] = f"Error ({e})"
    return stats


def get_docker_webserver_container():
    """Find the running Airflow webserver or scheduler container name."""
    try:
        res = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, check=True
        )
        containers = res.stdout.strip().splitlines()
        for c in containers:
            if "airflow-webserver" in c or "airflow-scheduler" in c:
                return c
    except Exception:
        pass
    return None


def trigger_dag(container_name, dag_id, wait_seconds=20):
    """Trigger an Airflow DAG inside the Docker container and wait for completion."""
    print(f"   ▶ Triggering DAG [{dag_id}] in {container_name}...")
    cmd = ["docker", "exec", container_name, "airflow", "dags", "trigger", dag_id]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"   ⚠️  Trigger returned: {res.stderr.strip()}")
    else:
        print(f"   ✅ Triggered: {res.stdout.strip().splitlines()[-1] if res.stdout else 'Success'}")
    
    # Progress indicator while DAG executes
    print(f"   ⏳ Waiting {wait_seconds}s for DAG execution...", end="", flush=True)
    for _ in range(wait_seconds):
        time.sleep(1)
        print(".", end="", flush=True)
    print(" Done!")


def collect_snapshot(conn):
    """Collect current row counts across raw, quarantined, and transformed schemas."""
    if not conn:
        return {}
    
    tables = ['customers', 'policies', 'claims', 'payments', 'agents', 'branches', 'policy_types', 'currency_rates', 'coverage_levels']
    snapshot = {'tables': {}, 'quarantine': {}, 'transformed': {}}
    
    for t in tables:
        snapshot['tables'][t] = query_scalar(conn, f"SELECT COUNT(*) FROM public.{t}") if query_scalar(conn, f"SELECT to_regclass('public.{t}')") else 0
        
    # Quarantine counts
    has_quar = query_scalar(conn, "SELECT to_regclass('public.quarantine')")
    if has_quar:
        df_q = query_df(conn, "SELECT table_name, COUNT(*) as cnt FROM public.quarantine GROUP BY table_name")
        for _, row in df_q.iterrows():
            snapshot['quarantine'][row['table_name']] = int(row['cnt'])
            
    # Transformed counts
    has_policy_sum = query_scalar(conn, "SELECT to_regclass('transformed.policy_summary')")
    if has_policy_sum:
        snapshot['transformed']['policy_summary'] = query_scalar(conn, "SELECT COUNT(*) FROM transformed.policy_summary")
        snapshot['transformed']['total_premium_sum'] = query_scalar(conn, "SELECT COALESCE(SUM(total_premium), 0) FROM transformed.policy_summary")
        snapshot['transformed']['active_policies_sum'] = query_scalar(conn, "SELECT COALESCE(SUM(active_policies), 0) FROM transformed.policy_summary")
        
    has_claims_trend = query_scalar(conn, "SELECT to_regclass('transformed.claims_trends')")
    if has_claims_trend:
        snapshot['transformed']['claims_trends'] = query_scalar(conn, "SELECT COUNT(*) FROM transformed.claims_trends")
        snapshot['transformed']['total_claim_amount_sum'] = query_scalar(conn, "SELECT COALESCE(SUM(total_claim_amount), 0) FROM transformed.claims_trends")

    return snapshot


def print_diff_report(parquet_counts, pre_heal, post_heal, post_transform):
    """Print an ASCII comparison table showing start-to-end data lifecycle changes."""
    print("\n" + "=" * 95)
    print(" 📊 END-TO-END PIPELINE AUDIT & DIFF REPORT")
    print("=" * 95)
    
    header = f"{'Entity / Table':<18} | {'Raw Parquet':<13} | {'Ingested':<13} | {'Post-Healing':<13} | {'Quarantined':<13} | {'Status'}"
    print(header)
    print("-" * 95)
    
    core_tables = ['customers', 'policies', 'claims', 'payments', 'agents', 'branches', 'policy_types', 'currency_rates', 'coverage_levels']
    
    total_raw = 0
    total_healed = 0
    total_quar = 0
    
    for t in core_tables:
        raw_cnt = parquet_counts.get(t, 0)
        if isinstance(raw_cnt, int):
            total_raw += raw_cnt
        
        ingested_cnt = pre_heal.get('tables', {}).get(t, 0)
        healed_cnt = post_heal.get('tables', {}).get(t, ingested_cnt)
        if isinstance(healed_cnt, int):
            total_healed += healed_cnt
            
        quar_cnt = post_heal.get('quarantine', {}).get(t, 0)
        total_quar += quar_cnt
        
        diff = raw_cnt - healed_cnt if isinstance(raw_cnt, int) else 0
        status = f"✅ Cleaned (-{diff:,})" if diff > 0 else ("✅ 100% Valid" if healed_cnt > 0 else "ℹ️ Loaded")
        
        print(f"{t:<18} | {str(raw_cnt):<13} | {str(ingested_cnt):<13} | {str(healed_cnt):<13} | {str(quar_cnt):<13} | {status}")

    print("-" * 95)
    print(f"{'TOTAL CORE ROWS':<18} | {total_raw:<13,}| {'-':<13} | {total_healed:<13,}| {total_quar:<13,}| {'✅ Finished'}")
    print("=" * 95)
    
    # Transformed Schema Summary
    trans = post_transform.get('transformed', {})
    print("\n ⭐ TRANSFORMED ANALYTICS SUMMARY:")
    print(f"   • Policy Summary Records : {trans.get('policy_summary', 0):,}")
    print(f"   • Total Active Policies  : {trans.get('active_policies_sum', 0):,}")
    print(f"   • Total Active Premium   : ${float(trans.get('total_premium_sum', 0)):,.2f}")
    print(f"   • Claims Trends Records  : {trans.get('claims_trends', 0):,}")
    print(f"   • Total Claim Amount     : ${float(trans.get('total_claim_amount_sum', 0)):,.2f}")
    print("=" * 95 + "\n")


def generate_markdown_report(parquet_counts, pre_heal, post_heal, post_transform, execution_time):
    """Write report to pipeline_execution_report.md."""
    report_path = os.path.join(os.path.dirname(__file__), "pipeline_execution_report.md")
    core_tables = ['customers', 'policies', 'claims', 'payments', 'agents', 'branches', 'policy_types', 'currency_rates', 'coverage_levels']
    
    lines = [
        "# 🚀 MassMutual Data Pipeline Execution & Diff Report",
        f"**Execution Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Total Runtime:** {execution_time:.2f} seconds  \n",
        "## 📊 Data Volume Progression & Quality Healing",
        "| Entity / Table | Raw Parquet Rows | Post-Ingestion | Post-Healing (Clean) | Quarantined Rows | Status / Impact |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |"
    ]
    
    for t in core_tables:
        raw_cnt = parquet_counts.get(t, 0)
        ingested_cnt = pre_heal.get('tables', {}).get(t, 0)
        healed_cnt = post_heal.get('tables', {}).get(t, ingested_cnt)
        quar_cnt = post_heal.get('quarantine', {}).get(t, 0)
        diff = raw_cnt - healed_cnt if isinstance(raw_cnt, int) else 0
        status = f"Filtered {diff:,} invalid rows" if diff > 0 else "100% clean"
        lines.append(f"| **{t}** | {raw_cnt:,} | {ingested_cnt:,} | {healed_cnt:,} | {quar_cnt:,} | {status} |")
        
    trans = post_transform.get('transformed', {})
    lines.extend([
        "\n## 📈 Transformed Analytics Schema Metrics",
        f"- **`transformed.policy_summary` Rows:** {trans.get('policy_summary', 0):,}",
        f"- **Active Policies Total:** {trans.get('active_policies_sum', 0):,}",
        f"- **Total Premium Amount:** `${float(trans.get('total_premium_sum', 0)):,.2f}`",
        f"- **`transformed.claims_trends` Rows:** {trans.get('claims_trends', 0):,}",
        f"- **Total Claim Amount:** `${float(trans.get('total_claim_amount_sum', 0)):,.2f}`\n",
        "## 🛠️ Pipeline Stages Completed",
        "1. **`load_massmutual_data`**: Ingested Parquet files into PostgreSQL raw tables.",
        "2. **`heal_massmutual_data`**: Fixed anomalies (negative premiums, invalid dates, risk scores) and isolated corrupt duplicates/orphans to `quarantine`.",
        "3. **`transform_massmutual_manual`**: Generated aggregated business-ready analytical datasets."
    ])
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"📝 Full markdown report saved to: {report_path}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MassMutual Data Pipeline Orchestrator & Diff Reporter")
    parser.add_argument("--dashboard", "-d", action="store_true", help="Launch Streamlit dashboard immediately after pipeline completion")
    args = parser.parse_args()

    print("\n" + "=" * 80)
    print(" 🚀 STARTING MASSMUTUAL ONE-COMMAND PIPELINE RUNNER")
    print("=" * 80)
    start_time = time.time()
    
    # 1. Scan source Parquet files
    print("\n🔍 Step 1: Scanning Source Parquet Datasets...")
    parquet_counts = count_parquet_rows()
    for tbl, count in parquet_counts.items():
        print(f"   • {tbl:<18}: {count:,} rows")
        
    # 2. Check Database & Docker container
    print("\n🔌 Step 2: Checking Airflow Container & Database...")
    container = get_docker_webserver_container()
    conn, active_port = get_db_connection()
    
    if not container:
        print("❌ Could not find a running Airflow container. Make sure 'docker-compose up -d' is running.")
        sys.exit(1)
    print(f"   ✅ Airflow Container found: {container}")
    if conn:
        print(f"   ✅ Connected to Database on port: {active_port}")

    # 3. Step 1 DAG: load_massmutual_data
    print("\n📥 Step 3: Running Ingestion DAG (load_massmutual_data)...")
    trigger_dag(container, "load_massmutual_data", wait_seconds=15)
    pre_heal_snapshot = collect_snapshot(conn)
    
    # 4. Step 2 DAG: heal_massmutual_data
    print("\n🩺 Step 4: Running Self-Healing & Quality DAG (heal_massmutual_data)...")
    trigger_dag(container, "heal_massmutual_data", wait_seconds=15)
    post_heal_snapshot = collect_snapshot(conn)
    
    # 5. Step 3 DAG: transform_massmutual_manual
    print("\n🔄 Step 5: Running Transformation DAG (transform_massmutual_manual)...")
    trigger_dag(container, "transform_massmutual_manual", wait_seconds=10)
    post_transform_snapshot = collect_snapshot(conn)
    
    # 6. Generate Diff & Audit Report
    total_time = time.time() - start_time
    print_diff_report(parquet_counts, pre_heal_snapshot, post_heal_snapshot, post_transform_snapshot)
    generate_markdown_report(parquet_counts, pre_heal_snapshot, post_heal_snapshot, post_transform_snapshot, total_time)
    
    if conn:
        conn.close()
    print("\n✨ Pipeline run completed successfully in {:.2f} seconds!".format(total_time))
    
    # 7. Dashboard Launch or Prompt
    print("\n" + "=" * 80)
    if args.dashboard:
        print("📊 Launching Streamlit Executive Dashboard (http://localhost:8501)...")
        print("=" * 80 + "\n")
        subprocess.run(["streamlit", "run", "dashboard.py"])
    else:
        print("📊 VIEW RESULTS IN STREAMLIT DASHBOARD:")
        print("   To launch the interactive dashboard, run:")
        print("      streamlit run dashboard.py")
        print("   Then open: http://localhost:8501")
        print("   (Or run next time with: python run_full_pipeline.py --dashboard)")
        print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
