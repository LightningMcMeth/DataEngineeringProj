"""
Daily DAG: перебудовує mart-моделі з тегом 'daily'
(важкі агрегації, які оновлюємо раз на добу).

Schedule: '15 3 * * *' — о 3:15 ночі локального часу, щоб:
  * не впиратись у DuckDB-lock з hourly DAG-ом, який стартує на :00
  * бути поза peak-годинами активності гравців
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.append("/opt/airflow/scripts")

import generate_analytics_summary

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="game_analytics_daily",
    description="dbt build tag:daily (важкі mart-агрегації)",
    start_date=datetime(2026, 4, 1),
    schedule="15 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["elt", "game_analytics", "daily"],
) as dag:

    t_dbt_build_daily = BashOperator(
        task_id="dbt_build_daily",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt build --select tag:daily --target dev"
        ),
    )

    t_generate_analytics_summary = PythonOperator(
        task_id="generate_analytics_summary",
        python_callable=generate_analytics_summary.run,
    )

    t_dbt_build_daily >> t_generate_analytics_summary
