"""
Airflow DAG for transforming json files into parquet files.

This DAG triggers transformation task for the silver medallion, which
normalize json files, clean, and enforcing shemas before converting to 
parquet files.

Schedule:
    - Runs daily at midnight (UTC by Airflow default)
    - No backfilling (`catchup=False`)

Notes:
    - The ingestion logic is implemented in `src.silver.`.
    - Checkpointing ensures incremental ingestion and safe restarts.
    - `/opt/airflow` is added to PYTHONPATH so the `src` package is discoverable.
"""

from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.sensors.external_task import ExternalTaskSensor # type: ignore
from datetime import datetime, timedelta

from src.silver.pipeline.bronze_to_silver import bronze_to_silver

with DAG(
    dag_id="silver_transform",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="Transforms Bronze JSON files into Silver Parquet tables",
):

     # Wait for ingest_daily (games, stats, etc.)
    wait_for_ingest_daily = ExternalTaskSensor(
        task_id="wait_for_ingest_daily",
        external_dag_id="ingest_daily",
        external_task_id="fetch_games",
        mode="reschedule",
        poke_interval=30,
        timeout=2 * 60 * 60,
        allowed_states=['success'],
        check_existence=True,
    )

    @task
    def run_bronze_to_silver():
        bronze_to_silver()

    # Dependencies - call the task and set dependency
    transform_task = run_bronze_to_silver()
    wait_for_ingest_daily >> transform_task # type: ignore
    