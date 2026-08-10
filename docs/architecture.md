# Architecture Notes

## Why batch, not streaming

Stock ticks *can* be streamed, but this pipeline is deliberately batch — daily granularity is enough for the downstream use case (daily OHLC, trend analysis), and batch is simpler to build, test, backfill, and reason about than a streaming system. Reaching for Kafka/Pub-Sub-based streaming here would be solving a problem the use case doesn't have. If the real requirement became "alert within seconds of a price move," that's a genuinely different architecture, not a tweak to this one.

## Failure modes and how this pipeline handles them

| Failure | What happens | Why |
|---|---|---|
| API request fails for one symbol | The whole `extract_api_to_gcs` task fails and retries (Airflow default retry policy: 2 retries, 5 min apart) | A partial day silently landing without anyone noticing is worse than a failed, retried, eventually-alerting task |
| API returns zero records for all symbols | Task raises explicitly rather than writing an empty file | An empty successful run looks identical to "the market was closed" unless it fails loudly — better to force a human to check |
| DAG re-run for a day that already loaded | Raw BigQuery table is append-only (duplicates possible); `stg_stock_ticks` dedupes on `(symbol, tick_at)` keeping the most recently ingested row | Keeps raw history immutable and auditable; correctness lives in dbt, not in fragile "don't double-write" logic in the extraction script |
| dbt test fails (e.g. `high_price < low_price`) | `dbt_test` task fails, DAG run is marked failed, `email_on_failure` alerts the team | Bad data should never silently reach the marts layer that dashboards/analysts query |
| GCS raw file exists but BigQuery load fails | `load_gcs_to_bigquery` task fails and retries independently of extraction — doesn't re-hit the API | Load failures (auth, quota, schema mismatch) are usually transient/local; no need to re-call a rate-limited external API to retry a local load step |

## What "production-grade" means here, concretely

- **Idempotent tasks** — each task can be safely retried without duplicating effects (append + downstream dedup, rather than relying on exactly-once delivery).
- **Explicit failure over silent success** — several places in this pipeline choose to raise loudly rather than proceed with partial/empty data.
- **Secrets never in code** — API keys and service account credentials are environment-injected, sourced from Airflow's secrets backend / `.env` (gitignored), never hardcoded or committed.
- **Infrastructure as code** — the GCS bucket and BigQuery datasets are provisioned via Terraform, not clicked together manually, so the environment is reproducible and reviewable.
- **Tests as a gate, not documentation** — `dbt_test` is a DAG task with real failure consequences, not a step someone runs manually and forgets.

## Known limitations of this version

- Extraction currently pulls "the latest" data only — there's no parameterized backfill path for historical date ranges yet (noted in the README as a next step).
- No CI pipeline runs `dbt test` automatically on pull requests yet — tests only run when the DAG executes.
- The `int_daily_ohlc` open/close logic assumes ticks arrive in a consistent, sortable order from the API; if the source ever delivers out-of-order or delayed ticks for the same day, this would need a watermark/late-arrival strategy rather than a simple `max`/`min`/first/last aggregation.
