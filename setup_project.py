
# setup_project.py
import subprocess
import os
import sys
import time

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

def run_command(command, check=True):
    """Run a shell command and return result"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if check and result.returncode != 0:
            print(f"❌ Command failed: {command}")
            print(f"Error: {result.stderr}")
            return False
        return True
    except Exception as e:
        print(f"❌ Error running command: {e}")
        return False

def setup_project():
    print("🚀 Setting up MassMutual Data Pipeline...")
    
    # 1. Start Docker services
    print("1. Starting Docker services...")
    if not run_command("docker-compose up -d"):
        return False
    
    # 2. Wait for services to be ready
    print("2. Waiting for services to be ready...")
    time.sleep(30)
    
    # 3. Initialize database
    print("3. Initializing database...")
    # massmutual_warehouse is auto-created by docker-compose environment vars, but ensure it exists safely
    run_command("docker exec materials-data-warehouse-postgres-1 createdb -U massmutual_user massmutual_warehouse", check=False)
    
    # 4. Restore database from backup (if any backup exists)
    print("4. Checking database restore from backup...")
    if os.path.exists("restore_database.py"):
        if not run_command("python restore_database.py", check=False):
            print("⚠️  Database restore skipped or not needed, continuing with fresh warehouse...")
    
    # 5. Initialize Airflow (airflow-init container handles this, but verify/ensure)
    print("5. Setting up Airflow...")
    run_command("docker exec materials-airflow-webserver-1 airflow db init", check=False)
    run_command('docker exec materials-airflow-webserver-1 airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com', check=False)
    
    print("✅ Setup completed successfully!")
    print("\n🎯 Next steps:")
    print("1. Access Airflow: http://localhost:8080 (admin/admin)")
    print("2. Run DAGs in order: load → heal → transform")
    print("3. Start dashboard: streamlit run dashboard.py")
    print("4. Access Dashboard: http://localhost:8501")

if __name__ == "__main__":
    setup_project()