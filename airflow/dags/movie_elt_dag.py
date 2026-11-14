"""
# Movie Streaming ELT Pipeline DAG
# 
# Orchestrates the end-to-end ELT lifecycle:
# 1. Trigger Airbyte connection to sync MovieLens and TMDB Trending data into BigQuery (raw_movies).
# 2. Run dbt models to transform raw records into cleaned staging views and analytics dimensional marts.
# 3. Execute dbt tests to enforce data quality and schema constraints.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

# ---------------------------------------------------------------------------
# Pipeline Configuration & Constants
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_movies")
AIRBYTE_CONN_ID = os.getenv("AIRFLOW_AIRBYTE_CONN_ID", "airbyte_default")

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=60),
}

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="movie_elt_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-End ELT: Airbyte Sync -> BigQuery -> dbt Transformations -> dbt Data Quality Tests",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["elt", "movies", "airbyte", "bigquery", "dbt"],
    doc_md=__doc__,
) as dag:

    # Task 1: Trigger Airbyte synchronization
    # Connects to Airbyte API via airbyte_default connection and triggers sync
    # Parameterized by Airflow Variable: AIRBYTE_CONNECTION_ID
    trigger_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id="{{ var.value.get('AIRBYTE_CONNECTION_ID', 'placeholder_connection_id') }}",
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,
        doc_md="""
        ### Trigger Airbyte Sync
        Triggers the configured Airbyte connection to extract MovieLens & TMDB datasets
        and load them into Google BigQuery `raw_movies` dataset.
        """,
    )

    # Task 2: Execute dbt transformations
    # Transforms raw tables into staging views and dimensional marts in analytics_movies
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt deps --profiles-dir . && "
            "dbt run --profiles-dir . --target dev"
        ),
        env={
            "BIGQUERY_PROJECT": os.getenv("BIGQUERY_PROJECT", ""),
            "GOOGLE_APPLICATION_CREDENTIALS": os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/gcp/service_account.json"
            ),
            "BIGQUERY_LOCATION": os.getenv("BIGQUERY_LOCATION", "US"),
            "BIGQUERY_DATASET_RAW": os.getenv("BIGQUERY_DATASET_RAW", "raw_movies"),
            "BIGQUERY_DATASET_ANALYTICS": os.getenv("BIGQUERY_DATASET_ANALYTICS", "analytics_movies"),
            "PATH": os.environ.get("PATH", ""),
        },
        doc_md="""
        ### dbt Run
        Executes dbt models:
        1. Staging (`stg_movies`, `stg_ratings`, `stg_tmdb_trending`)
        2. Marts (`mart_movie_performance`, `mart_genre_trends`)
        """,
    )

    # Task 3: Execute dbt data quality tests
    # Enforces uniqueness, not-null constraints, and accepted rating score ranges
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir . --target dev",
        env={
            "BIGQUERY_PROJECT": os.getenv("BIGQUERY_PROJECT", ""),
            "GOOGLE_APPLICATION_CREDENTIALS": os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/gcp/service_account.json"
            ),
            "BIGQUERY_LOCATION": os.getenv("BIGQUERY_LOCATION", "US"),
            "BIGQUERY_DATASET_RAW": os.getenv("BIGQUERY_DATASET_RAW", "raw_movies"),
            "BIGQUERY_DATASET_ANALYTICS": os.getenv("BIGQUERY_DATASET_ANALYTICS", "analytics_movies"),
            "PATH": os.environ.get("PATH", ""),
        },
        doc_md="""
        ### dbt Test
        Runs all configured dbt data quality tests to validate transformations
        prior to downstream BI consumption.
        """,
    )

    # -----------------------------------------------------------------------
    # Task Dependencies: Linear ELT Pipeline
    # -----------------------------------------------------------------------
    trigger_airbyte_sync >> dbt_run >> dbt_test

