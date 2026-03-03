"""
Airflow DAG: dbt_run

This DAG orchestrates dbt commands (debug and run) using DockerOperator.
It mounts the dbt project and data directories into the container using
environment variables for portability. The DAG is scheduled to run daily.

Requirements:
  - Set Airflow Variables: DBT_PATH and DATA_PATH (absolute paths on the host)
  - Ensure ./dbt and ./data directories exist in your project root
  - Docker and Docker Compose must be installed and configured
  - Share project folder to Docker desktop (Resources -> File Sharing)
""" 

from airflow import DAG # type: ignore
from airflow.providers.docker.operators.docker import DockerOperator # type: ignore
from docker.types import Mount  # type: ignore
from datetime import datetime
from airflow.models import Variable # type: ignore

dbt_path = Variable.get("DBT_PATH")
data_path = Variable.get("DATA_PATH")


default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dbt_run",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
):

    dbt_run = DockerOperator(
        task_id="run_dbt",
        image="dbt-runner:latest",
        command="dbt run",
        working_dir="/usr/app/dbt",
        auto_remove=True,
        mount_tmp_dir=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source=dbt_path, target="/usr/app/dbt", type="bind"),
            Mount(source=data_path, target="/usr/app/data", type="bind"),
        ],
    )

    dbt_debug = DockerOperator(
    task_id="dbt_debug",
    image="dbt-runner:latest",
    command="dbt debug",
    working_dir="/usr/app/dbt",
    auto_remove=True,
    mount_tmp_dir=False,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    mounts=[
            Mount(source=dbt_path, target="/usr/app/dbt", type="bind"),
            Mount(source=data_path, target="/usr/app/data", type="bind"),
        ],
)

dbt_debug >> dbt_run # type: ignore

