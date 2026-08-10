"""
load_gcs_to_bigquery.py

Loads a day's raw NDJSON file from GCS into the `raw` BigQuery dataset,
append-only. This table is intentionally untransformed — it's the
queryable source of truth for exactly what was received from the API,
which dbt's staging models then build on top of.
"""

import os
from datetime import datetime, timezone

from google.cloud import bigquery


RAW_TABLE_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("open", "FLOAT64"),
    bigquery.SchemaField("high", "FLOAT64"),
    bigquery.SchemaField("low", "FLOAT64"),
    bigquery.SchemaField("close", "FLOAT64"),
    bigquery.SchemaField("volume", "INT64"),
    # Ingestion metadata — useful for debugging and for dbt freshness checks
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    bigquery.SchemaField("_source_file", "STRING"),
]


def load_day(run_date: str) -> None:
    client = bigquery.Client()

    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    bucket_name = os.environ["GCS_RAW_BUCKET"]

    table_id = f"{project_id}.{dataset}.stock_ticks"
    source_uri = f"gs://{bucket_name}/raw/stock_ticks/dt={run_date}/ticks_{run_date}.jsonl"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=RAW_TABLE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Append-only by design: if this DAG run needs to be re-run for the
        # same day, dedupe happens in dbt staging (on symbol+timestamp),
        # not by overwriting raw history.
    )

    load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
    load_job.result()  # blocks until the load finishes or raises

    table = client.get_table(table_id)
    print(f"Loaded {source_uri} into {table_id} — table now has {table.num_rows} total rows.")


def main() -> None:
    run_date = os.environ.get("AIRFLOW_RUN_DATE") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    load_day(run_date)


if __name__ == "__main__":
    main()
