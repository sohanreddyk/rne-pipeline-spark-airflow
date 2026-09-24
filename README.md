# Data Engineering Lab — RNE ETL Pipeline

Welcome to this hands-on **data engineering** lab! In this project, you will build a complete ETL (Extract, Transform, Load) pipeline using Apache Spark, Airflow, and PostgreSQL.

## Overview

This lab processes data from the French **National Directory of Elected Officials (RNE)**. You will:

1. **Extract** 12 source tables from PostgreSQL into HDFS
2. **Transform** and unify the data into a single table
3. **Aggregate** the data to create analytical views
4. **Orchestrate** the entire workflow with Apache Airflow

The data covers elected officials at different levels, including mayors, members of parliament, councilors, and others.

## Pipeline Architecture

```text
PostgreSQL (12 tables)
    ↓ [Extract Job]
HDFS (Raw Data)
    ↓ [Transform Job]
HDFS (Unified Table)
    ↓ [Aggregate Job]
PostgreSQL (4 aggregated tables)
```

### Airflow Pipeline

```text
check_source → extract_job → transform_job → aggregate_job → validate_output
```

## Project Structure

```text
TP_ref/
├── dags/                           # Airflow DAGs
│   └── rne_pipeline.py             # Main ETL pipeline
├── spark_jobs/                     # Spark jobs
│   ├── config.py                   # Configuration (paths, URLs, tables)
│   ├── job_extract.py              # PostgreSQL → HDFS extraction
│   ├── job_transform.py            # Data transformation
│   └── job_aggregate.py            # Aggregation and calculations
├── infra/                          # Docker infrastructure
│   ├── docker-compose.yml          # Service orchestration
│   ├── Dockerfile.airflow          # Custom Airflow image
│   ├── Dockerfile.jupyter          # Jupyter image
│   ├── init-postgres.sql           # Database initialization script
│   ├── migrate_csv_to_postgres.py  # CSV → PostgreSQL migration
│   └── notebooks/                  # Jupyter notebooks for exploration
├── rep_national_elus/              # Raw data (CSV)
│   └── elus-*.csv                  # 12 CSV files by elected-official type
├── enonce_tp/                      # Lab assignment
│   └── tp_etudiant.pdf             # Detailed instructions
└── requirements.txt                # Python dependencies
```

## Prerequisites

* **Python 3.10+**
* **Docker & Docker Compose**
* **Java 11+** for Spark
* Around **6 GB of disk space**

## Installation and Setup

### 1. Clone the project and install dependencies

```bash
cd /Users/mouaad/GoldenCollar/BAC4/TP_ref
pip install -r requirements.txt
```

### 2. Start the Docker services

```bash
cd infra
docker-compose up -d
```

This starts:

* **PostgreSQL** on port 5432
* **Apache Airflow** on port 8080
* **Jupyter Notebook** on port 8888
* **Hadoop/HDFS** on port 50070

### 3. Verify the services

```bash
docker-compose ps
```

### 4. Load the initial data

Once PostgreSQL is ready, load the CSV data:

```bash
python infra/migrate_csv_to_postgres.py
```

This populates the 12 source tables in PostgreSQL.

### 5. Access Airflow

1. Open `http://localhost:8080`
2. Default credentials: `airflow` / `airflow`
3. You should see the DAG named `ETL-DAG-RNE`

## Running the Pipeline

### Through the Airflow UI

1. Open the `ETL-DAG-RNE` DAG
2. Click **Trigger DAG**
3. Follow the execution in real time

### Through the command line

```bash
docker exec airflow_container airflow dags trigger ETL-DAG-RNE
```

## Pipeline Stages

### `check_source` — PythonOperator

Checks that all 12 source tables exist and are not empty in PostgreSQL.

**Expected tables:**

* `rne.elus_deputes_dep`
* `rne.elus_senateurs_sen`
* `rne.elus_maires_mai`
* `rne.elus_conseillers_municipaux_cm`
* `rne.elus_conseillers_departementaux_cd`
* `rne.elus_conseillers_regionaux_cr`
* `rne.elus_conseillers_communautaires_epci`
* `rne.elus_conseillers_darrondissements_ca`
* `rne.elus_representants_parlement_europeen_rpe`
* `rne.elus_membres_dune_assemblee_ma`
* `rne.elus_assemblee_des_francais_de_letranger_afe`
* `rne.elus_conseillers_des_francais_de_letranger_cons`

### `job_extract` — SparkSubmitOperator

Exports the 12 PostgreSQL tables to HDFS in parallel.

**Output:**

```text
/data/raw/
```

Stored as Parquet files.

### `job_transform` — SparkSubmitOperator

* Unifies the 12 tables into a single `elus_unified` table
* Normalizes common columns
* Adds a `type_elu` column to identify the elected-official level/type

**Output:**

```text
/data/refined/elus_unified/
```

Stored in Parquet format.

### `job_aggregate` — SparkSubmitOperator

Creates 4 aggregated tables:

* `nb_elus_par_region` — number of elected officials per region
* `nb_elus_par_departement` — number of elected officials per department
* `gender_distribution` — gender distribution
* `top_communes` — top 100 municipalities with the most elected officials

**Output:** PostgreSQL tables in the `rne` schema.

### `validate_output` — PythonOperator

Checks that the 4 aggregated tables were successfully created and contain data.

## Configuration

Edit `spark_jobs/config.py` to modify the following settings:

```python
# PostgreSQL
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_USER = "postgres"
POSTGRES_PASS = "password"
POSTGRES_DB = "rne_db"

# HDFS
HDFS_NAMENODE = "hdfs://namenode:9000"
HDFS_RAW = "/data/raw"
HDFS_REFINED = "/data/refined"
HDFS_GOLD = "/data/gold"

# Spark
SPARK_MASTER = "spark://spark:7077"
```

## Important Files

| File                          | Description                    |
| ----------------------------- | ------------------------------ |
| `dags/rne_pipeline.py`        | Main DAG and task dependencies |
| `spark_jobs/config.py`        | Centralized configuration      |
| `spark_jobs/job_extract.py`   | Extraction logic               |
| `spark_jobs/job_transform.py` | Transformation logic           |
| `spark_jobs/job_aggregate.py` | Aggregation logic              |
| `infra/init-postgres.sql`     | PostgreSQL schema              |
| `infra/docker-compose.yml`    | Docker services                |

## Troubleshooting

### Services do not start

```bash
docker-compose logs
docker-compose down
docker-compose up --build
```

### PostgreSQL says "connection refused"

Wait around 30 seconds for PostgreSQL to become ready:

```bash
docker-compose ps
```

Make sure the service status is `Up`.

### Airflow cannot find the DAGs

```bash
docker-compose restart airflow
```

### Spark jobs fail

* Check the logs in the Airflow UI
* Check available disk space:

```bash
df -h
```

* Verify HDFS permissions

### Data is missing after extraction

```sql
SELECT * FROM rne.elus_deputes_dep LIMIT 5;
```

If the table is empty, rerun:

```bash
migrate_csv_to_postgres.py
```

## Additional Documentation

See `enonce_tp/tp_etudiant.pdf` for:

* Detailed exercise instructions
* Questions to answer
* Evaluation criteria
* Use cases and SQL queries

## Useful Tips

1. **Inspect the data:** Use Jupyter to explore the datasets before processing them.
2. **Start small:** Test transformations on a subset before processing the full dataset.
3. **Use logs:** Check Airflow logs when debugging jobs.
4. **Inspect schemas:** Print DataFrame schemas to understand the structure.
5. **Performance:** Use `explain()` to analyze Spark execution plans.

## Learning Objectives

By the end of this lab, you should be able to:

* Build a complete ETL pipeline with Spark
* Orchestrate jobs with Airflow
* Integrate PostgreSQL and HDFS
* Transform and aggregate large-scale datasets
* Monitor and debug production data pipelines

## Support

If you encounter a problem:

1. Check the logs:

```bash
docker-compose logs <service>
```

2. Verify the configuration in `config.py`
3. Restart the relevant service
4. Ask the instructor for help

**Good luck with the lab!**
