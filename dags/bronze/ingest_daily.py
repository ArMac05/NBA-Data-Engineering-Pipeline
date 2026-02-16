"""
Airflow DAG for ingesting NBA game data into the Bronze layer.

This DAG triggers the daily ingestion tasks, which retrieves
current-season NBA game data from the BallDontLie API using cursor-based
pagination and writes raw JSON output to the Bronze storage path.

Schedule:
    - Runs daily at midnight (UTC by Airflow default)
    - No backfilling (`catchup=False`)

Notes:
    - The ingestion logic is implemented in `src.ingestion.`.
    - Checkpointing ensures incremental ingestion and safe restarts.
    - `/opt/airflow` is added to PYTHONPATH so the `src` package is discoverable.
"""


import sys
from pathlib import Path

# Add /opt/airflow to Python path so src module is discoverable
sys.path.insert(0, "/opt/airflow")

from airflow import DAG     # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
from src.ingestion.fetch_games import fetch_games

with DAG(
    dag_id="ingest_daily",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):
    # Ingest Games
    ingest_games_task = PythonOperator(
        task_id="fetch_games",
        python_callable=fetch_games
    )