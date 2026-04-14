"""
Hourly DAG: ETL з MySQL + JSON у DuckDB raw-шар, потім dbt build для моделей
з тегом 'hourly' (весь stg-шар + операційні mart-моделі як fct_daily_revenue).

Паралельно з цим DAG-ом існує game_analytics_daily для важких агрегацій.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.append("/opt/airflow/scripts")

import my_sql_to_duck as extract_mysql_to_duckdb
import load_json_to_duck as load_json_to_duckdb

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="game_analytics_hourly",
    description="ETL → DuckDB raw + dbt build tag:hourly",
    start_date=datetime(2026, 4, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,              # уникаємо DuckDB lock між послідовними ранами
    default_args=default_args,
    tags=["elt", "game_analytics", "hourly"],
) as dag:

    t_extract_mysql = PythonOperator(
        task_id="extract_mysql_to_duckdb",
        python_callable=extract_mysql_to_duckdb.run,
    )

    t_load_json = PythonOperator(
        task_id="load_json_to_duckdb",
        python_callable=load_json_to_duckdb.run,
    )

    # Seeds вантажимо один раз за ран; якщо CSV не змінився, це швидкий no-op.
    t_dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt && dbt seed --target dev",
    )

    # dbt build = run + test + seed + snapshot, обмежений тегом 'hourly'.
    # DBT_PROFILES_DIR і DBT_PROJECT_DIR задані в docker-compose.yml.
    t_dbt_build_hourly = BashOperator(
        task_id="dbt_build_hourly",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt build --select tag:hourly --target dev"
        ),
    )

    t_extract_mysql >> t_load_json >> t_dbt_seed >> t_dbt_build_hourly
