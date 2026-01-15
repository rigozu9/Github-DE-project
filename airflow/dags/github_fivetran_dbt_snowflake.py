from __future__ import annotations

import os
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

API_BASE = "https://api.fivetran.com/v1"


def _req(method: str, path: str, payload: dict | None = None) -> dict:
    key = os.environ["FIVETRAN_API_KEY"]
    secret = os.environ["FIVETRAN_API_SECRET"]
    url = f"{API_BASE}{path}"
    headers = {"Accept": "application/json;version=2"}

    r = requests.request(method, url, auth=(key, secret), headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def ensure_manual_schedule() -> None:
    # Optional but recommended: let Airflow be the scheduler of syncs
    cid = os.environ["FIVETRAN_CONNECTION_ID"]
    _req("PATCH", f"/connections/{cid}", {"schedule_type": "manual", "run_setup_tests": False})


def trigger_sync(ti) -> None:
    cid = os.environ["FIVETRAN_CONNECTION_ID"]

    # snapshot current succeeded_at / failed_at to detect fresh completion
    before = _req("GET", f"/connections/{cid}")["data"]["status"]
    ti.xcom_push(key="before_succeeded_at", value=before.get("succeeded_at"))
    ti.xcom_push(key="before_failed_at", value=before.get("failed_at"))

    # Trigger sync: POST /v1/connections/{connectionId}/sync  {"force": true}
    _req("POST", f"/connections/{cid}/sync", {"force": True})


def wait_until_fresh_success(ti) -> bool:
    cid = os.environ["FIVETRAN_CONNECTION_ID"]
    before_succeeded = ti.xcom_pull(key="before_succeeded_at", task_ids="trigger_fivetran_sync")
    before_failed = ti.xcom_pull(key="before_failed_at", task_ids="trigger_fivetran_sync")

    data = _req("GET", f"/connections/{cid}")["data"]["status"]
    sync_state = data.get("sync_state")         # scheduled / syncing / paused / rescheduled
    succeeded_at = data.get("succeeded_at")
    failed_at = data.get("failed_at")

    # If still running, keep waiting
    if sync_state == "syncing":
        return False

    # If failed_at changed, fail the task
    if failed_at and failed_at != before_failed:
        raise AirflowException(f"Fivetran sync failed (failed_at changed to {failed_at}).")

    # We consider it successful when succeeded_at changes after our trigger
    return succeeded_at is not None and succeeded_at != before_succeeded


with DAG(
    dag_id="github_fivetran_sync_then_dbt_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fivetran", "snowflake", "dbt"],
) as dag:

    t0_manual = PythonOperator(
        task_id="ensure_fivetran_manual_schedule",
        python_callable=ensure_manual_schedule,
    )

    t1 = PythonOperator(
        task_id="trigger_fivetran_sync",
        python_callable=trigger_sync,
    )

    t2 = PythonSensor(
        task_id="wait_for_fivetran_sync",
        python_callable=wait_until_fresh_success,
        poke_interval=60,
        timeout=3 * 60 * 60,
        mode="poke",
    )

    host_project_dir = os.environ["HOST_PROJECT_DIR"]
    host_home = os.environ["HOST_HOME"]

    t3 = DockerOperator(
        task_id="dbt_run_test",
        image="ghcr.io/dbt-labs/dbt-snowflake:1.9.1",
        docker_url="unix://var/run/docker.sock",
        auto_remove='success',
        command=[
            "bash",
            "-lc",
            "dbt deps --project-dir /dbt && "
            "dbt run  --project-dir /dbt --profiles-dir /root/.dbt --profile github_data_pipeline --target dev && "
            "dbt test --project-dir /dbt --profiles-dir /root/.dbt --profile github_data_pipeline --target dev",
        ],
        mounts=[
            Mount(source=os.path.join(host_project_dir, "github-de"), target="/dbt", type="bind"),
            Mount(source=os.path.join(host_home, ".dbt"), target="/root/.dbt", type="bind"),
        ],
        environment={
            "SNOWFLAKE_ACCOUNT": os.environ["SNOWFLAKE_ACCOUNT"],
            "SNOWFLAKE_USER": os.environ["SNOWFLAKE_USER"],
            "SNOWFLAKE_PASSWORD": os.environ["SNOWFLAKE_PASSWORD"],
            "SNOWFLAKE_ROLE": os.environ["SNOWFLAKE_ROLE"],
            "SNOWFLAKE_WAREHOUSE": os.environ["SNOWFLAKE_WAREHOUSE"],
            "SNOWFLAKE_DATABASE": os.environ["SNOWFLAKE_DATABASE"],
            "SNOWFLAKE_SCHEMA": os.environ["SNOWFLAKE_SCHEMA"],
        },
    )

    t0_manual >> t1 >> t2 >> t3
