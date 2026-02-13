from airflow import DAG # type: ignore
from airflow.decorators import task # type: ignore
from airflow.sensors.external_task import ExternalTaskSensor # type: ignore
from datetime import datetime

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
        poke_interval=60,
        timeout=60 * 60,
    )

    @task
    def run_bronze_to_silver():
        bronze_to_silver()

    # Dependencies
    transform_task = run_bronze_to_silver()
    wait_for_ingest_daily >> transform_task # type: ignore
