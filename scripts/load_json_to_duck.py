import duckdb
DUCKDB_PATH = "/opt/airflow/duckdb/warehouse.duckdb"
EVENTS_PATH = "/opt/airflow/data/raw/game_events.json"

def run():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.events AS
        SELECT * FROM read_json_auto(
            '{EVENTS_PATH}',
            format='newline_delimited',
            maximum_object_size=16777216
        );
    """)
    n = con.execute("SELECT COUNT(*) FROM raw.events;").fetchone()[0]
    print(f"Loaded {n} events into raw.events")