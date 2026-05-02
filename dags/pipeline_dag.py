"""
dags/pipeline_dag.py — Orchestrates the CDC pipeline (Path A) and the
Streaming Taxi pipeline (Path B) end-to-end.

DAG topology:
    health_check
        ├── bronze_cdc  ──► silver_cdc ──┐
        └── bronze_taxi ──► silver_taxi ──┴──► gold_taxi ──► validation

Schedule: every 15 minutes → freshness SLA of ~20 min (15 min cadence +
~5 min processing). max_active_runs=1 prevents concurrent runs from racing
over shared Iceberg tables.

Idempotency:
  - Taxi bronze/silver use availableNow=True + Kafka checkpoints: a second
    run for the same interval finds no new offsets and exits immediately.
  - CDC bronze uses availableNow=True + Kafka checkpoints: same guarantee.
  - CDC silver uses MERGE: deterministic given the same bronze rows.
  - Gold uses CREATE OR REPLACE: deterministic given the same silver rows.

Retries: each task retries up to 2 times with a 2-minute back-off.
SLA: each DAG run must complete within 30 minutes; a miss triggers a log alert.
Failure notification: on_failure_callback logs the failed task so it shows up
in Airflow task logs and can be forwarded to a monitoring system.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Any

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Service addresses (injected by Docker Compose)
# ---------------------------------------------------------------------------
CONNECT_URL  = os.environ.get("CONNECT_URL",  "http://connect:8083")
PG_HOST      = os.environ.get("PG_HOST",      "postgres")
PG_PORT      = int(os.environ.get("PG_PORT",  "5432"))
PG_DB        = os.environ.get("PG_DB",        "sourcedb")
PG_USER      = os.environ.get("PG_USER",      "cdc_user")
PG_PASSWORD  = os.environ.get("PG_PASSWORD",  "cdc_pass")

CONNECTOR_NAME = "pg-cdc-connector"

# Debezium connector config — captures both source tables.
# snapshot.mode=initial: reads all existing rows first (op='r'), then tails
# the WAL for subsequent inserts/updates/deletes. Chosen because we need a
# complete initial state in the lakehouse before tracking deltas.
CONNECTOR_CONFIG: dict[str, Any] = {
    "connector.class":             "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname":           "postgres",
    "database.port":               "5432",
    "database.user":               PG_USER,
    "database.password":           PG_PASSWORD,
    "database.dbname":             "sourcedb",
    "topic.prefix":                "dbserver1",
    "table.include.list":          "public.customers,public.drivers",
    "plugin.name":                 "pgoutput",
    "snapshot.mode":               "initial",
    "key.converter":               "org.apache.kafka.connect.json.JsonConverter",
    "value.converter":             "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":   "true",
    "value.converter.schemas.enable": "true",
    "tombstones.on.delete":        "true",   # emit tombstone after delete event
}

# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------
def on_failure_callback(context: dict) -> None:
    ti   = context["task_instance"]
    dag  = context["dag"].dag_id
    task = ti.task_id
    run  = context["run_id"]
    print(
        f"[ALERT] DAG '{dag}' run '{run}': task '{task}' FAILED. "
        "Check task logs for details."
    )


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    print(
        f"[SLA MISS] DAG '{dag.dag_id}': the following tasks breached their SLA: "
        f"{[str(s) for s in slas]}"
    )


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def health_check(**_) -> None:
    """
    Ensure the Debezium connector exists and is RUNNING.
    Registers the connector (idempotent PUT) if it has never been created.
    Raises on any non-RUNNING state so the DAG stops early.
    """
    cfg_url    = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config"
    status_url = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status"

    # PUT /connectors/{name}/config creates-or-updates (idempotent)
    put = requests.put(
        cfg_url,
        json=CONNECTOR_CONFIG,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if put.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to register/update connector: {put.status_code} {put.text}"
        )
    print(f"Connector registered/updated (HTTP {put.status_code}).")

    # Give Kafka Connect a moment to start the connector tasks
    time.sleep(5)

    resp = requests.get(status_url, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Status check failed: {resp.status_code} {resp.text}")

    status           = resp.json()
    connector_state  = status["connector"]["state"]
    print(f"Connector state: {connector_state}")

    if connector_state != "RUNNING":
        raise RuntimeError(f"Connector is not RUNNING: {connector_state}")

    for task in status.get("tasks", []):
        if task["state"] != "RUNNING":
            raise RuntimeError(
                f"Connector task {task['id']} is not RUNNING: {task['state']}"
            )

    print("Health check PASSED.")


def _exec_in_jupyter(script_path: str) -> None:
    """
    Run *script_path* (relative to /home/jovyan/project) inside the running
    jupyter container via the Docker SDK.  Streams stdout/stderr to the
    Airflow task log and raises on non-zero exit so Airflow marks the task
    as failed.
    """
    import docker  # installed via entrypoint pip install

    client    = docker.from_env()
    container = client.containers.get("jupyter")
    cmd       = f"python /home/jovyan/project/{script_path}"

    print(f"Running in jupyter container: {cmd}")

    # exec_create + exec_start gives us streaming output
    exec_id = client.api.exec_create(
        container.id, cmd, stdout=True, stderr=True
    )
    output_stream = client.api.exec_start(exec_id["Id"], stream=True, demux=False)

    for chunk in output_stream:
        print(chunk.decode("utf-8", errors="replace"), end="", flush=True)

    inspect   = client.api.exec_inspect(exec_id["Id"])
    exit_code = inspect["ExitCode"]

    if exit_code != 0:
        raise RuntimeError(
            f"Script '{script_path}' exited with code {exit_code}"
        )


def run_bronze_cdc(**_)   -> None: _exec_in_jupyter("src/cdc/bronze_cdc.py")
def run_bronze_taxi(**_)  -> None: _exec_in_jupyter("src/streaming/bronze.py")
def run_silver_cdc(**_)   -> None: _exec_in_jupyter("src/cdc/silver_cdc.py")
def run_silver_taxi(**_)  -> None: _exec_in_jupyter("src/streaming/silver.py")
def run_gold_taxi(**_)    -> None: _exec_in_jupyter("src/streaming/gold.py")
def run_validation(**_)   -> None: _exec_in_jupyter("src/cdc/validate.py")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner":               "airflow",
    "depends_on_past":     False,
    "retries":             2,
    "retry_delay":         timedelta(minutes=2),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="taxi_cdc_pipeline",
    default_args=DEFAULT_ARGS,
    description="CDC (PostgreSQL → Iceberg) + Streaming Taxi (Kafka → Iceberg) pipeline",
    # Every 15 minutes → freshness SLA of ~20 min (schedule + processing time)
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,           # prevent concurrent runs from racing on Iceberg
    dagrun_timeout=timedelta(minutes=30),
    sla_miss_callback=on_sla_miss_callback,
    tags=["cdc", "taxi", "iceberg", "project3"],
) as dag:

    t_health_check = PythonOperator(
        task_id="health_check",
        python_callable=health_check,
        retries=3,                       # extra retries: connector may be slow to start
        retry_delay=timedelta(minutes=1),
        sla=timedelta(minutes=3),
    )

    t_bronze_cdc = PythonOperator(
        task_id="bronze_cdc",
        python_callable=run_bronze_cdc,
        sla=timedelta(minutes=10),
    )

    t_bronze_taxi = PythonOperator(
        task_id="bronze_taxi",
        python_callable=run_bronze_taxi,
        sla=timedelta(minutes=10),
    )

    t_silver_cdc = PythonOperator(
        task_id="silver_cdc",
        python_callable=run_silver_cdc,
        sla=timedelta(minutes=18),
    )

    t_silver_taxi = PythonOperator(
        task_id="silver_taxi",
        python_callable=run_silver_taxi,
        sla=timedelta(minutes=18),
    )

    t_gold_taxi = PythonOperator(
        task_id="gold_taxi",
        python_callable=run_gold_taxi,
        sla=timedelta(minutes=25),
    )

    t_validation = PythonOperator(
        task_id="validation",
        python_callable=run_validation,
        sla=timedelta(minutes=28),
        # Keep going for taxi tasks even if CDC validation fails
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Dependency graph ────────────────────────────────────────────────────
    #
    #   health_check
    #       ├── bronze_cdc  ──► silver_cdc ──┐
    #       └── bronze_taxi ──► silver_taxi ──┴──► gold_taxi ──► validation
    #
    t_health_check >> [t_bronze_cdc, t_bronze_taxi]
    t_bronze_cdc   >> t_silver_cdc
    t_bronze_taxi  >> t_silver_taxi
    [t_silver_cdc, t_silver_taxi] >> t_gold_taxi
    t_gold_taxi    >> t_validation
