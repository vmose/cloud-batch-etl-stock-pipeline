# Cloud Batch ETL/ELT Pipeline, Stock Ticks

A production-shaped batch pipeline that ingests stock tick data from a REST API, lands it in cloud storage, loads it into a warehouse, and models it into analysis-ready tables, orchestrated end to end, not just a notebook that happens to work once.

Built to demonstrate the classic batch ELT pattern with the tools most data teams actually run in production: **Airflow** for orchestration, **GCS** as a raw data lake, **BigQuery** as the warehouse, and **dbt** for transformation.

## Architecture

```
                 ┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌────────────────┐
   REST API  ──▶ │   Extract   │ ──▶  │  GCS (raw)   │ ──▶  │  BigQuery   │ ──▶  │   dbt models    │
 (stock ticks)   │  (Airflow   │      │  landing     │      │  raw table  │      │ staging →       │
                 │   task)     │      │  zone        │      │  (external/ │      │ intermediate →  │
                 └─────────────┘      └──────────────┘      │  loaded)    │      │ marts           │
                                                              └─────────────┘      └────────────────┘
                                                                                          │
                                                                                          ▼
                                                                                  dbt tests + docs
```

Orchestrated as a single Airflow DAG (`dags/stock_ticks_pipeline_dag.py`) with four tasks:

1. **`extract_api_to_gcs`**, pulls the latest ticks from the API, writes raw JSON to a dated path in GCS (`raw/stock_ticks/dt=YYYY-MM-DD/`).
2. **`load_gcs_to_bigquery`**, loads that day's raw file into a `raw` dataset table in BigQuery (append-only, source of truth for what was actually received).
3. **`dbt_run`**, runs the dbt project: staging → intermediate → marts.
4. **`dbt_test`**, runs dbt's data tests; fails the DAG run (and should page/alert) if a test fails, rather than silently loading bad data downstream.

## Why this shape (design notes)

- **Landing raw data in GCS before loading to BigQuery**, rather than loading directly from the API into the warehouse, means the raw API response is preserved exactly as received. If a downstream bug is discovered later, you can replay from the raw files instead of having lost the original payload.
- **The raw BigQuery table is append-only and untransformed.** All cleaning, typing, and business logic lives in dbt, the raw layer is a source of truth, not a place to fix data quality problems.
- **dbt owns staging → intermediate → marts**, not the ingestion script. Ingestion's only job is "get the data in reliably"; modeling logic belongs in one place (dbt), version-controlled and testable independently of the orchestration layer.
- **Secrets are never hardcoded.** The API key is read from an environment variable, sourced from an Airflow Connection/Variable in a real deployment (or GCP Secret Manager, referenced in `terraform/`). `.env.example` shows the shape without a real value.

## Repo structure

```
.
├── dags/
│   └── stock_ticks_pipeline_dag.py     # Airflow DAG definition
├── scripts/
│   ├── extract_api_to_gcs.py           # API → GCS
│   └── load_gcs_to_bigquery.py         # GCS → BigQuery raw table
├── dbt/stock_pipeline/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/                    # 1:1 with raw, typed/renamed
│       ├── intermediate/               # daily OHLC aggregation
│       └── marts/                      # analysis-ready fact table
├── terraform/                          # GCS bucket + BigQuery datasets (IaC)
├── docs/architecture.md                # deeper design notes + failure modes
├── docker-compose.yml                  # local Airflow for development
├── requirements.txt
└── .env.example
```

## Running it locally

**Prerequisites:** Docker Desktop, a GCP project with billing enabled, a service account JSON key with `Storage Object Admin` + `BigQuery Data Editor` roles, and an API key from your data provider.

```bash
# 1. Copy the env template and fill in your real values, never commit .env
cp .env.example .env

# 2. Place your GCP service account key at the path referenced in .env
#    (this file is .gitignored, do not commit it)

# 3. Provision the GCS bucket and BigQuery datasets
cd terraform && terraform init && terraform apply && cd ..

# 4. Start Airflow locally
docker compose up airflow-init
docker compose up -d

# 5. Open the Airflow UI at http://localhost:8080 (default: airflow/airflow),
#    set the Airflow Connections/Variables listed below, and trigger the DAG
```

### Required Airflow Connections / Variables (set in the UI or via `airflow variables set`)

| Name | Type | Purpose |
|---|---|---|
| `stock_api_key` | Variable (or better: Secret Manager backend) | API key, never stored in DAG code |
| `google_cloud_default` | Connection | GCP auth for GCS/BigQuery operators |

## Testing the transformation layer independently

```bash
cd dbt/stock_pipeline
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## What I'd add with more time

- A **backfill-safe** extraction task (currently pulls "latest," would extend to accept a date-range parameter for historical backfills without re-running the whole DAG).
- **Great Expectations or dbt-expectations** for richer data-quality checks beyond dbt's built-in schema tests.
- A **CI pipeline** (GitHub Actions) running `dbt test` against a CI dataset on every PR touching `dbt/`.
