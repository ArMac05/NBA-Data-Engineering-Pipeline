from airflow import DAG # type: ignore
from airflow.providers.docker.operators.docker import DockerOperator # type: ignore
from docker.types import Mount  # type: ignore
from datetime import datetime

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
            Mount(source="/usr/app/dbt", target="/usr/app/dbt", type="bind"),
            Mount(source="/usr/app/data", target="/usr/app/data", type="bind"),
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
        Mount(source="/usr/app/dbt", target="/usr/app/dbt", type="bind"),
        Mount(source="/usr/app/data", target="/usr/app/data", type="bind"),
    ],
)

dbt_debug >> dbt_run # type: ignore

