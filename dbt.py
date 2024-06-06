from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

"""
DAG with BashOperator to run a dbt model
"""
with DAG(
    dag_id="dbt_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"dbt_model_name": "+test_view"},
) as dag:
    run_this = BashOperator(
        task_id="run_dbt_task",
        bash_command="source /Users/gursingh9/install/venv/dbt/bin/activate; cd /Users/gursingh9/install/dbt; dbt run --select {{ params.dbt_model_name }}",
    )

    run_this
