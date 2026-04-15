"""
Завантажує довідкові CSV з MinIO у схему `raw` DuckDB.

Замінює dbt seeds для бонусу: MinIO → DuckDB → dbt staging.
DuckDB ходить у MinIO через httpfs-екстеншн, тобто напряму S3 API.
"""

import os
import duckdb

DUCKDB_PATH = "/opt/airflow/duckdb/warehouse.duckdb"

# endpoint/creds беремо з env (задані в docker-compose.yml → airflow-common)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "game-analytics-seeds")

# table_name → ім'я файлу в бакеті
FILES = {
    "country_codes": "country_codes.csv",
    "product_category_groups": "product_category_groups.csv",
    "platform_os_family": "platform_os_family.csv",
    "fx_rates": "fx_rates.csv",
}


def run():
    con = duckdb.connect(DUCKDB_PATH)

    # httpfs дає DuckDB можливість читати s3:// URL
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # конфіг для MinIO (path-style, бо це не реальний AWS S3)
    con.execute("SET s3_url_style = 'path';")
    con.execute(f"SET s3_endpoint = '{MINIO_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id = '{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key = '{MINIO_SECRET_KEY}';")
    con.execute("SET s3_use_ssl = false;")

    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    for table, fname in FILES.items():
        url = f"s3://{MINIO_BUCKET}/{fname}"
        con.execute(
            f"""
            CREATE OR REPLACE TABLE raw.{table} AS
            SELECT * FROM read_csv_auto('{url}', header=True);
            """
        )
        n = con.execute(f"SELECT COUNT(*) FROM raw.{table};").fetchone()[0]
        print(f"Loaded {n} rows into raw.{table} from {url}")

    con.close()


if __name__ == "__main__":
    run()
