# restore_database.py
import subprocess
import os
import sys
import glob

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

def restore_database():
    print("🚀 Restoring database...")
    
    # Find the latest backup file
    backup_files = glob.glob("database_backup/massmutual_backup_*.sql")
    if not backup_files:
        print("❌ No backup files found in database_backup/")
        return False
    
    latest_backup = max(backup_files, key=os.path.getctime)
    print(f"📦 Using backup file: {latest_backup}")
    
    try:
        # Wait for PostgreSQL to be ready
        print("⏳ Waiting for PostgreSQL to be ready...")
        subprocess.run([
            'docker', 'exec', 'materials-data-warehouse-postgres-1',
            'pg_isready', '-U', 'massmutual_user', '-h', 'localhost', '-p', '5432'
        ], check=True)
        
        # Restore the database
        with open(latest_backup, 'r') as f:
            cmd = [
                'docker', 'exec', '-i', 'materials-data-warehouse-postgres-1',
                'psql', '-U', 'massmutual_user', '-d', 'massmutual_warehouse'
            ]
            
            result = subprocess.run(
                cmd,
                stdin=f,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, 'PGPASSWORD': 'massmutual_pass'}
            )
        
        if result.returncode == 0:
            print("✅ Database restored successfully!")
            return True
        else:
            print(f"❌ Restore failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error during restore: {e}")
        return False

if __name__ == "__main__":
    restore_database()