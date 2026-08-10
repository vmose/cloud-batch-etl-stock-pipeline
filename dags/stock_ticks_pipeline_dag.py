"""
stock_ticks_pipeline_dag.py

Orchestrates the full batch ELT run: API -> GCS -> BigQuery (raw) -> dbt.

Secrets handling: the API key is never referenced directly in this file.
`extract_api_to_gcs.py` reads it from the STOCK_API_KEY environment
variable, which in this deployment is populated from an Airflow Variable
(ideally backed by GCP Secret Manager or another secrets backend, not the
Airflow metadata DB in plaintext) — see README.md for setup.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@example.com"],
}

DBT_PROJECT_DIR = "/opt/airflow/dbt/stock_pipeline"


def _run_extract(**context):
    # Import locally so the DAG file itself has no hard runtime dependency
    # on requests/google-cloud-storage at parse time.
    import os
    import sys

    sys.path.insert(0, "/opt/airflow/scripts")
    os.environ["AIRFLOW_RUN_DATE"] = context["ds"]
    os.environ["STOCK_API_KEY"] = Variable.get("stock_api_key")  # pulled from Airflow's secrets backend

    import extract_api_to_gcs
    extract_api_to_gcs.main()


def _run_load(**context):
    import os
    import sys

    sys.path.insert(0, "/opt/airflow/scripts")
    os.environ["AIRFLOW_RUN_DATE"] = context["ds"]

    import load_gcs_to_bigquery
    load_gcs_to_bigquery.main()


with DAG(
    dag_id="stock_ticks_pipeline",
    description="Batch ELT: stock ticks API -> GCS -> BigQuery -> dbt",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # daily at 06:00 UTC, after market data settles
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "bigquery", "dbt", "stocks"],
) as dag:

    extract_api_to_gcs = PythonOperator(
        task_id="extract_api_to_gcs",
        python_callable=_run_extract,
    )

    load_gcs_to_bigquery = PythonOperator(
        task_id="load_gcs_to_bigquery",
        python_callable=_run_load,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
    )

    extract_api_to_gcs >> load_gcs_to_bigquery >> dbt_run >> dbt_test
