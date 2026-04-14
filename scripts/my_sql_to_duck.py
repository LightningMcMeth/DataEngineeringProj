import duckdb
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

DUCKDB_PATH = "/opt/airflow/duckdb/warehouse.duckdb"

def run():
    hook = MySqlHook(mysql_conn_id="mysql_game_db")
    df: pd.DataFrame = hook.get_pandas_df(
        "SELECT * FROM purchase_transactions"
    )
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.register("tx_df", df)
    con.execute("""
        CREATE OR REPLACE TABLE raw.purchase_transactions AS
        SELECT * FROM tx_df;
    """)
    print(f"Loaded {len(df)} rows into raw.purchase_transactions")