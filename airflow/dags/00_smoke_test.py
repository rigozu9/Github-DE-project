from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="00_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # only manual runs for now
    catchup=False,
    tags=["setup"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Airflow OK' && date && whoami && ls -la /opt/airflow/dags",
    )
