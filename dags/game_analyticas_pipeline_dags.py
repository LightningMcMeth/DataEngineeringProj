from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append("/opt/airflow/scripts")

import my_sql_to_duck as extract_mysql_to_duckdb
import load_json_to_duck as load_json_to_duckdb

with DAG(
    dag_id="game_analytics_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule=None,                        # поки запускаємо вручну
    catchup=False,
    tags=["elt", "game_analytics"],
) as dag:

    t_extract_mysql = PythonOperator(
        task_id="extract_mysql_to_duckdb",
        python_callable=extract_mysql_to_duckdb.run,
    )

    t_load_json = PythonOperator(
        task_id="load_json_to_duckdb",
        python_callable=load_json_to_duckdb.run,
    )

    t_extract_mysql >> t_load_json
