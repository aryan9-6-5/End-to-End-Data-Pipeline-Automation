
# setup_project.py
import subprocess
import os
import time

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
    if not run_command("docker exec postgres createdb -U massmutual_user massmutual_warehouse || true"):
        return False
    
    # 4. Restore database from backup
    print("4. Restoring database from backup...")
    if not run_command("python restore_database.py"):
        print("⚠️  Database restore failed, but continuing...")
    
    # 5. Initialize Airflow
    print("5. Setting up Airflow...")
    run_command("docker exec airflow-webserver airflow db init || true")
    run_command('docker exec airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || true')
    
    print("✅ Setup completed successfully!")
    print("\n🎯 Next steps:")
    print("1. Access Airflow: http://localhost:8080 (admin/admin)")
    print("2. Run DAGs in order: load → heal → transform")
    print("3. Start dashboard: streamlit run dashboard.py")
    print("4. Access Dashboard: http://localhost:8501")

if __name__ == "__main__":
    setup_project()