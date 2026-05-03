# Project 3 — CDC + Orchestrated Lakehouse Pipeline

CDC pipeline capturing PostgreSQL changes via Debezium, landing them in an Apache Iceberg lakehouse (bronze → silver → gold), combined with the streaming NYC taxi pipeline from Project 2 — orchestrated end-to-end with Apache Airflow.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Path A — CDC Pipeline                                                      │
│                                                                             │
│  PostgreSQL ──► Debezium ──► Kafka ──► bronze_cdc ──► silver_cdc           │
│  (customers,       (WAL)   (dbserver1   (raw events,   (MERGE INTO,         │
│   drivers)                  .public.*)   append-only)   current state)      │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Path B — Streaming Taxi Pipeline                                           │
│                                                                             │
│  produce.py ──► Kafka ──► bronze_taxi ──► silver_taxi ──► gold_taxi        │
│  (parquet       (taxi-    (raw JSON)      (cleaned,        (top-5           │
│   replay)        trips)                   enriched)         zones)          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Cross-pipeline Gold                                                        │
│                                                                             │
│  silver_cdc + silver_taxi ──► gold_demand_patterns                         │
│                               (demand by country × hour of day)            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Airflow DAG  (taxi_cdc_pipeline, */15 * * * *)                            │
│                                                                             │
│  health_check ──► snapshot_pg ──► bronze_cdc  ──► silver_cdc ──┐          │
│                               └── bronze_taxi ──► silver_taxi ──┼──► gold_taxi ──────────┐   │
│                                                                  └──► gold_demand ────────┴──► validation │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────┐
│  Storage — MinIO (S3-compatible) │
│  Catalog  — Iceberg (Hadoop)     │
│  Format   — Apache Iceberg       │
└──────────────────────────────────┘
```

### Services

| Container | Role | Port |
|-----------|------|------|
| `kafka` | Message broker (KRaft, no ZooKeeper) | — |
| `postgres` | OLTP source for CDC | 5432 |
| `connect` | Kafka Connect + Debezium PostgreSQL connector | 8083 |
| `minio` | S3-compatible object storage for Iceberg | 9000 / 9001 |
| `iceberg-rest` | Iceberg REST catalog | 8181 |
| `airflow` | Airflow webserver + scheduler (standalone) | 8080 |
| `jupyter` | Jupyter + PySpark — Spark jobs run here | 8888 / 4040 |

---

## Prerequisites

- Docker + Docker Compose
- Taxi parquet files (same as Project 1 & 2) placed in `data/`
---

## Quick Start

### 1. Configure credentials

### 2. Start the stack

```bash
docker compose up -d
```

Wait ~30 seconds for all services to become healthy. Check with:

```bash
docker compose ps
```

All containers should show `healthy` or `running`.

### 3. Seed PostgreSQL

Creates `customers` and `drivers` source tables with initial data:

```bash
docker exec jupyter python /home/jovyan/project/seed.py
```

### 4. Produce taxi data into Kafka

Replays the January 2025 parquet file into the `taxi-trips` topic:

```bash
docker exec jupyter python /home/jovyan/project/produce.py
```

Runs once and exits. Add `--loop` to replay continuously.

### 5. Start the CDC change simulator (optional — separate terminal)

Continuously inserts, updates, and deletes rows in PostgreSQL to simulate a live OLTP workload:

```bash
docker exec jupyter python /home/jovyan/project/simulate.py
```

Press `Ctrl-C` to stop. Stop this before running the final validation to get an exact row-count match.

### 6. Trigger the Airflow DAG

Open Airflow at **http://localhost:8080** — login with `admin` and the password printed in the container logs:

```bash
docker compose logs airflow | grep "password:"
```

Navigate to `taxi_cdc_pipeline` → enable the DAG → click **Trigger DAG**.

The DAG runs automatically every 15 minutes once enabled.
---

## Stopping

```bash
# Stop containers, keep data volumes (Iceberg tables survive)
docker compose down

# Full reset — wipes all volumes including Iceberg tables and Postgres data
docker compose down -v
# Also clear Kafka checkpoints from the host:
rm -rf checkpoints/
```

---

## Project Structure

```
├── compose.yml                  # Full service stack
├── .env.example                 # Credential template
├── seed.py                      # Seed PostgreSQL source tables
├── produce.py                   # Kafka taxi trip producer
├── simulate.py                  # Live OLTP change simulator
├── conf/
│   └── spark-defaults.conf      # Spark + Iceberg + MinIO config
├── dags/
│   └── pipeline_dag.py          # Airflow DAG (both pipelines)
├── src/
│   ├── cdc/
│   │   ├── bronze_cdc.py        # Kafka → Bronze Iceberg (CDC)
│   │   ├── silver_cdc.py        # Bronze → Silver (MERGE)
│   │   └── validate.py          # Silver vs PostgreSQL snapshot check
│   ├── streaming/
│   │   ├── bronze.py            # Kafka → Bronze Iceberg (taxi)
│   │   ├── silver.py            # Bronze → Silver (clean + enrich)
│   │   └── gold.py              # Silver → Gold (top-5 zones)
│   └── gold_demand_patterns.py  # Silver CDC + Silver taxi → Gold demand
├── data/                        # Not in git — place parquet files here
└── REPORT.md
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | `admin` / see container logs |
| Jupyter | http://localhost:8888 | `JUPYTER_TOKEN` from `.env` |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| Spark UI | http://localhost:4040 | — (available while a job runs) |
| Kafka Connect API | http://localhost:8083 | — |
