# make_portable.py
import os
import shutil
import datetime

def create_portable_package():
    print("📦 Creating portable package...")
    
    # Create timestamp for backup
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    portable_dir = f"massmutual_portable_{timestamp}"
    
    # Create directory structure
    os.makedirs(portable_dir, exist_ok=True)
    
    # Files to include
    essential_files = [
        'dashboard.py',
        'docker-compose.yaml', 
        'Dockerfile',
        'requirements.txt',
        '.env',
        'airflow_backup.yml',
        'backup_database.py',
        'restore_database.py', 
        'setup_project.py',
        'make_portable.py',
        'README.md',
        '.gitignore'
    ]
    
    # Directories to include
    essential_dirs = [
        'dags',
        'data',
        'database_backup',
        'docs'
    ]
    
    # Copy files
    for file in essential_files:
        if os.path.exists(file):
            shutil.copy2(file, portable_dir)
            print(f"✅ Copied: {file}")
    
    # Copy directories
    for dir_name in essential_dirs:
        if os.path.exists(dir_name):
            shutil.copytree(dir_name, os.path.join(portable_dir, dir_name))
            print(f"✅ Copied: {dir_name}/")
    
    # Create zip file
    shutil.make_archive(portable_dir, 'zip', portable_dir)
    
    # Cleanup temp directory
    shutil.rmtree(portable_dir)
    
    print(f"🎉 Portable package created: {portable_dir}.zip")
    print(f"📧 File size: {os.path.getsize(portable_dir + '.zip') / (1024*1024):.2f} MB")
    print("📤 Ready to email or transfer!")

if __name__ == "__main__":
    create_portable_package()