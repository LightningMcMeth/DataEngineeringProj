"""
Hourly DAG: ETL з MinIO + MySQL + JSON у DuckDB raw-шар, потім dbt build для моделей
з тегом 'hourly' (весь stg-шар + операційні mart-моделі як fct_daily_revenue).

Паралельно з цим DAG-ом існує game_analytics_daily для важких агрегацій.

Замість dbt seeds довідкові CSV тепер вантажаться з MinIO (S3-сумісне сховище)
окремою таскою load_minio_to_duckdb → raw.country_codes, raw.fx_rates тощо.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.append("/opt/airflow/scripts")

import my_sql_to_duck as extract_mysql_to_duckdb
import load_json_to_duck as load_json_to_duckdb
import load_minio_to_duck as load_minio_to_duckdb

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="game_analytics_hourly",
    description="ETL (MinIO + MySQL + JSON) → DuckDB raw + dbt build tag:hourly",
    start_date=datetime(2026, 4, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,              # уникаємо DuckDB lock між послідовними ранами
    default_args=default_args,
    tags=["elt", "game_analytics", "hourly"],
) as dag:

    # Завантажуємо довідкові CSV з MinIO у raw.* (country_codes, fx_rates, ...).
    t_load_minio = PythonOperator(
        task_id="load_minio_to_duckdb",
        python_callable=load_minio_to_duckdb.run,
    )

    t_extract_mysql = PythonOperator(
        task_id="extract_mysql_to_duckdb",
        python_callable=extract_mysql_to_duckdb.run,
    )

    t_load_json = PythonOperator(
        task_id="load_json_to_duckdb",
        python_callable=load_json_to_duckdb.run,
    )

    # dbt build = run + test + snapshot, обмежений тегом 'hourly'.
    # DBT_PROFILES_DIR і DBT_PROJECT_DIR задані в docker-compose.yml.
    t_dbt_build_hourly = BashOperator(
        task_id="dbt_build_hourly",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt build --select tag:hourly --target dev"
        ),
    )

    t_load_minio >> t_extract_mysql >> t_load_json >> t_dbt_build_hourly
