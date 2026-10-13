# End-to-End Data Pipeline Automation

An end-to-end insurance data pipeline built with Apache Airflow, PostgreSQL, and Docker. This project ingests Parquet source files, validates and heals data quality issues, loads cleaned data into a warehouse, and creates analytical views for reporting and dashboarding.

This repo automates the full lifecycle of data processing across:
- Data ingestion
- Data quality validation and repair
- Warehouse loading
- Transformation into analytical tables
- Executive dashboard reporting

---

## Overview

The project processes insurance-related datasets stored as Parquet files and loads them into a PostgreSQL data warehouse using Airflow DAGs. The pipeline includes:

1. Loading raw source data
2. Validating schema and data quality
3. Healing invalid records and quarantining bad data
4. Transforming cleaned data into business-ready analytical datasets
5. Visualizing KPIs through a Streamlit dashboard

The project is fully containerized for easy setup and reproducibility.

---

## Architecture

The repository uses the following core components:

- Apache Airflow for orchestration
- PostgreSQL for raw and warehouse storage
- Redis as the Airflow message broker
- Streamlit for dashboarding
- Docker Compose for environment setup
- Parquet files as the source data layer

Key workflow:
- `load_massmutual_data` ingests source parquet files into PostgreSQL
- `heal_massmutual_data` cleans data and isolates invalid records
- `transform_massmutual_manual` creates reporting/analytics tables
- `dashboard.py` presents business insights and data metrics

---

## Features

- End-to-end ETL orchestration with Airflow DAGs
- Automated ingestion from Parquet datasets
- Data validation and anomaly detection
- Self-healing logic for data quality issues
- Data quarantine for invalid or corrupted records
- PostgreSQL warehouse for curated and transformed data
- Executive dashboard with business metrics
- Containerized deployment using Docker

---

## Tech Stack

- Python
- Apache Airflow
- PostgreSQL
- Docker / Docker Compose
- Streamlit
- Pandas
- PyArrow / Parquet
- Redis
- SQL

---

## Repository Structure

```text
.
├── dags/
│   ├── load_massmutual_data.py
│   ├── heal_massmutual_data.py
│   ├── transform_massmutual.py
│   ├── master_massmutual_pipeline.py
│   ├── insurance_pipeline_dag.py
│   ├── run_pipeline_local.py
│   ├── check_parquet_schemas.py
│   ├── get_schema.py
│   ├── get_table_schemas.py
│   ├── load_massmutual_data_validation.py
│   └── sql/
│
├── parquet_data/
│   ├── agents.parquet
│   ├── branches.parquet
│   ├── claims.parquet
│   ├── coverage_levels.parquet
│   ├── currency_rates.parquet
│   ├── customers.parquet
│   ├── payment_methods.parquet
│   ├── payments.parquet
│   ├── policies.parquet
│   ├── policy_types.parquet
│   └── snapshots.parquet
│
├── dashboard.py
├── run_full_pipeline.py
├── setup_project.py
├── backup_database.py
├── restore_database.py
├── docker-compose.yaml
├── docker-compose-portable.yaml
├── Dockerfile
├── Dockerfile.dashboard
├── requirements.txt
├── requirements-dashboard.txt
├── QUICK_START.md
├── pipeline_execution_report.md
├── .gitignore
├── .gitattributes
└── README.md
```

---

## Data Sources

The source data consists of multiple Parquet tables representing core insurance domain entities, including:

- Customers
- Policies
- Claims
- Payments
- Agents
- Branches
- Coverage levels
- Policy types
- Currency rates
- Payment methods
- Snapshots

These files provide the raw data foundation for downstream validation and analytics.

---

## Prerequisites

Before running the project, ensure you have:

- Docker
- Docker Compose
- Git
- A local machine with enough resources for Airflow + PostgreSQL + Streamlit

---

## Quick Start

### 1. Start all services

```bash
docker-compose up -d --build
```

This initializes:

- PostgreSQL warehouse on port `5433`
- Airflow web server on port `8080`
- Streamlit dashboard on port `8501`
- Redis for task messaging

---

### 2. Run the pipeline

You can trigger the pipeline either from the Airflow UI or from the terminal.

#### Option A: Airflow UI
Open:

```text
http://localhost:8080
```

Login:

```text
Username: admin
Password: admin
```

Then unpause and trigger:

```text
master_massmutual_pipeline
```

#### Option B: Terminal
```bash
docker exec -it materials-airflow-webserver-1 airflow dags trigger master_massmutual_pipeline
```

---

### 3. Open the dashboard

```text
http://localhost:8501
```

The dashboard contains reporting views for:

- Executive summary
- Policy performance
- Claims trends
- Data quality health
- Warehouse explorer
- AI-assisted analysis (if configured)

---

## Pipeline Flow

The project follows a three-stage pipeline:

### 1. Load Stage
`load_massmutual_data`

- Reads Parquet source files
- Loads raw data into PostgreSQL
- Validates table structures and data availability

### 2. Healing Stage
`heal_massmutual_data`

- Detects invalid or inconsistent records
- Cleans corrupted values
- Quarantines bad records
- Preserves data quality and integrity

### 3. Transformation Stage
`transform_massmutual_manual`

- Creates analytics-ready tables
- Aggregates policy and claims metrics
- Produces reporting datasets for dashboards

---

## Dashboard

The Streamlit dashboard provides an executive-level overview of the insurance data, including:

- Total premiums
- Active policies
- Claims trends
- Data quality metrics
- Raw vs cleaned vs transformed dataset summaries

You can launch it manually:

```bash
streamlit run dashboard.py
```

---

## Useful Commands

### View container logs

```bash
docker-compose logs -f airflow-webserver
docker-compose logs -f streamlit
```

### Restart services

```bash
docker-compose down
docker-compose up -d --build
```

### Stop services

```bash
docker-compose down
```

---

## Notes

- The project is designed to be portable and reproducible in a Docker environment.
- The Airflow setup expects the source Parquet files to be available in the `parquet_data` directory.
- The pipeline execution report is generated in `pipeline_execution_report.md`.

---

## License

This project does not specify a license in the repository metadata. If you intend to publish it publicly, you may want to add an appropriate license file such as MIT or Apache 2.0.

---

## Contributing

Contributions are welcome. If you update the pipeline logic, improve validation rules, or enhance the dashboard, please ensure:

- The Airflow DAGs remain runnable
- Docker setup still works
- Data quality checks continue to pass
- Documentation is updated accordingly

---

## Contact

For questions or collaboration inquiries, please use the repository issues or contact the project maintainer directly.
