# 🚀 Quick Start Guide (100% Dockerized)

Everything runs inside Docker containers — **no manual Python packages or local database configuration needed**.

---

## 1️⃣ Start All Services (Airflow, Postgres & Streamlit)

```bash
docker-compose up -d --build
```

This launches all services in isolated containers:
- 🐘 **Data Warehouse PostgreSQL**: Port `5433` (internal `5432`)
- 🌪️ **Airflow Webserver, Scheduler, Worker & DB**: Port `8080`
- 📊 **Streamlit Executive Dashboard**: Port `8501`
- ⚡ **Redis**: Internal queue

---

## 2️⃣ Run the Data Pipeline (Load ➔ Heal ➔ Transform)

### Method A: Via Airflow Web UI (Recommended)
1. Open **[http://localhost:8080](http://localhost:8080)** (login: `admin` / `admin`).
2. Unpause and trigger **`master_massmutual_pipeline`**.
   - It will automatically execute in exact sequence:
     1. `load_massmutual_data` (Ingests Parquet files into Postgres)
     2. `heal_massmutual_data` (Cleans corrupt data & quarantines invalid records)
     3. `transform_massmutual_manual` (Generates business & policy aggregations)

### Method B: One-Command Trigger from Terminal (via Docker)
```bash
docker exec -it materials-airflow-webserver-1 airflow dags trigger master_massmutual_pipeline
```

---

## 3️⃣ View the Executive Dashboard
Open **[http://localhost:8501](http://localhost:8501)** to explore:
- 🏠 **Executive Summary**: Core insurance KPIs, premiums, and active policies.
- 📈 **Business Intelligence**: Claims trends over time & policy distribution.
- 🔍 **Data Quality**: Healing metrics (Cleaned vs. Quarantined records).
- 📋 **Data Explorer**: Live queries into raw, cleaned, and transformed tables.
- 🤖 **AI Assistant**: Natural language queries powered by Gemini AI.

---

## 🛠️ Useful Docker Commands

```bash
# View live container logs
docker-compose logs -f streamlit
docker-compose logs -f airflow-webserver

# Restart all services
docker-compose down && docker-compose up -d --build
```