FROM apache/airflow:2.10.5

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      default-libmysqlclient-dev \
      build-essential \
      pkg-config \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow-providers-mysql==5.7.0" \
      "dbt-core==1.8.7" \
      "dbt-duckdb==1.8.3" \
      "duckdb==1.0.0" \
      "polars==1.6.0"
