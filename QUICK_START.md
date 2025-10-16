# 🚀 Quick Start Guide

## For New Users (One-Command Setup)

1. **Extract the ZIP file**
2. **Open terminal in extracted folder**
3. **Run setup script:**
   ```bash
   python setup_project.py
Wait for completion (~2-3 minutes)

Access applications:

Airflow: http://localhost:8080 (admin/admin)

Dashboard: http://localhost:8501

Manual Setup (if needed)
bash
# 1. Start services
docker-compose up -d

# 2. Wait for services to be ready
python restore_database.py

# 3. Start dashboard
streamlit run dashboard.py
Running the Pipeline
Go to Airflow: http://localhost:8080

Run DAGs in this order:

load_massmutual_data

heal_massmutual_data

transform_massmutual

View results in Dashboard: http://localhost:8501

text

### **Step 6: Execute This Process Now**

```bash
# 1. Backup your current database
python backup_database.py

# 2. Create the portable package
python make_portable.py

# 3. Check the created zip file
dir *.zip
```