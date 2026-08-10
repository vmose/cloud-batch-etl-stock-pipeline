"""
extract_api_to_gcs.py

Pulls stock tick data from the REST API and writes the raw response(s) to
GCS, untransformed, partitioned by ingestion date.

Design choices:
- The API key is read from an environment variable (STOCK_API_KEY), never
  hardcoded. In the Airflow deployment this env var is populated from an
  Airflow Variable / a Secret Manager-backed connection — see the DAG file.
- Raw JSON is written exactly as received. Any cleaning/typing happens later
  in dbt, not here — this script's only job is reliable, faithful ingestion.
- NOTE: the exact request/response shape below is a placeholder based on a
  typical financial-ticks API. Confirm the real endpoint path, auth method
  (header vs. query param), and response schema against the provider's
  actual docs before running this against production, and adjust
  `build_request` / `parse_response` accordingly — those are the only two
  functions that should need to change.
"""

import json
import os
import sys
from datetime import datetime, timezone

import requests
from google.cloud import storage


def build_request(symbol: str) -> tuple[str, dict, dict]:
    """Constructs the URL, headers, and params for one symbol's tick request.

    Placeholder shape — adjust to match the provider's actual API docs.
    Common patterns are either an `Authorization: Bearer <key>` header or an
    `apikey=<key>` query param; this uses the header pattern by default.
    """
    base_url = os.environ["STOCK_API_BASE_URL"].rstrip("/")
    api_key = os.environ["STOCK_API_KEY"]

    url = f"{base_url}/stocks/{symbol}/ticks"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"interval": "1min"}
    return url, headers, params


def parse_response(raw_json: dict) -> list[dict]:
    """Normalizes the API response into a flat list of tick records.

    Placeholder — adjust the key names below (`data`, `symbol`, `timestamp`,
    `open`/`high`/`low`/`close`/`volume`) to match the provider's real schema.
    """
    ticks = raw_json.get("data", [])
    return ticks


def fetch_symbol_ticks(symbol: str) -> list[dict]:
    url, headers, params = build_request(symbol)
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return parse_response(response.json())


def write_to_gcs(records: list[dict], bucket_name: str, run_date: str) -> str:
    """Writes the day's raw tick records to GCS as newline-delimited JSON."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob_path = f"raw/stock_ticks/dt={run_date}/ticks_{run_date}.jsonl"
    blob = bucket.blob(blob_path)

    ndjson_body = "\n".join(json.dumps(record) for record in records)
    blob.upload_from_string(ndjson_body, content_type="application/x-ndjson")

    return f"gs://{bucket_name}/{blob_path}"


def main() -> None:
    run_date = os.environ.get("AIRFLOW_RUN_DATE") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    symbols = os.environ["STOCK_API_SYMBOLS"].split(",")
    bucket_name = os.environ["GCS_RAW_BUCKET"]

    all_records: list[dict] = []
    for symbol in symbols:
        symbol = symbol.strip()
        try:
            records = fetch_symbol_ticks(symbol)
            all_records.extend(records)
            print(f"Fetched {len(records)} ticks for {symbol}")
        except requests.RequestException as exc:
            # Fail loudly rather than silently skipping a symbol — a partial
            # day's data landing without anyone noticing is worse than the
            # DAG run failing and getting retried/alerted on.
            print(f"ERROR fetching {symbol}: {exc}", file=sys.stderr)
            raise

    if not all_records:
        raise RuntimeError("No records fetched for any symbol — aborting write to GCS.")

    gcs_path = write_to_gcs(all_records, bucket_name, run_date)
    print(f"Wrote {len(all_records)} records to {gcs_path}")


if __name__ == "__main__":
    main()
