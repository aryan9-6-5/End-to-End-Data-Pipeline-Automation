# backup_database.py
import subprocess
import os
import datetime

def backup_database():
    print("🚀 Creating database backup...")
    
    # Ensure backup directory exists
    os.makedirs("database_backup", exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"database_backup/massmutual_backup_{timestamp}.sql"
    
    try:
        # Create SQL dump
        cmd = [
            'docker', 'exec', 'postgres',
            'pg_dump', '-U', 'massmutual_user', '-d', 'massmutual_warehouse',
            '-h', 'localhost', '-p', '5432', '--clean', '--if-exists'
        ]
        
        with open(backup_file, 'w') as f:
            result = subprocess.run(
                cmd, 
                stdout=f, 
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, 'PGPASSWORD': 'massmutual_pass'}
            )
        
        if result.returncode == 0:
            print(f"✅ Database backup created: {backup_file}")
            return backup_file
        else:
            print(f"❌ Backup failed: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"❌ Error during backup: {e}")
        return None

if __name__ == "__main__":
    backup_database()