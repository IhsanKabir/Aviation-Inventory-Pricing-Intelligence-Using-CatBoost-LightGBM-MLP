# BigQuery Warehouse Plan

## Purpose

BigQuery is the bounded hosted read layer for this platform. Local PostgreSQL stays operational for ingestion, comparisons, ML/DL training, and long-term history; BigQuery holds only a recent hot-cache slice for the website, Looker Studio, and current operational dashboards.

## Chosen BI Layer

- BigQuery sandbox for current hosted reads and SQL analytics
- Looker Studio for dashboards

This is the strongest free-to-start combination for:

- query management
- portfolio visibility
- dashboard delivery
- thesis-friendly current analytics outputs

Long-history analysis should use local PostgreSQL, database backups, or ignored Parquet exports under `output/warehouse/`, not permanent BigQuery storage.

## Curated Tables

- `dim_airline`
- `dim_route`
- `fact_cycle_run`
- `fact_offer_snapshot`
- `fact_change_event`
- `fact_penalty_snapshot`
- `fact_tax_snapshot`
- `fact_forecast_bundle`
- `fact_forecast_model_eval`
- `fact_forecast_route_eval`
- `fact_forecast_route_winner`
- `fact_forecast_next_day`
- `fact_backtest_eval`
- `fact_backtest_route_winner`
- `fact_backtest_split`

`fact_offer_snapshot` now includes round-trip route-monitor fields:

- `search_trip_type`
- `trip_request_id`
- `requested_outbound_date`
- `requested_return_date`
- `trip_duration_days`
- `trip_origin`
- `trip_destination`
- `trip_pair_key`
- `leg_direction`
- `leg_sequence`
- `itinerary_leg_count`

## Export Contract

Source of truth for export layout:

- [sql/bigquery/create_analytics_tables.sql](../../sql/bigquery/create_analytics_tables.sql)
- [sql/bigquery/create_analytics_views.sql](../../sql/bigquery/create_analytics_views.sql)
- [sql/bigquery/alter_aviation_intel_live_schema.sql](../../sql/bigquery/alter_aviation_intel_live_schema.sql)
- [sql/bigquery/apply_hot_cache_retention.sql](../../sql/bigquery/apply_hot_cache_retention.sql)
- [sql/bigquery/create_aviation_intel_dataset.sql](../../sql/bigquery/create_aviation_intel_dataset.sql)
- [sql/bigquery/create_aviation_intel_tables.sql](../../sql/bigquery/create_aviation_intel_tables.sql)
- [sql/bigquery/create_aviation_intel_looker_views.sql](../../sql/bigquery/create_aviation_intel_looker_views.sql)
- [tools/export_bigquery_stage.py](../../tools/export_bigquery_stage.py)
- [warehouse/bigquery/BOOTSTRAP_CHECKLIST.md](BOOTSTRAP_CHECKLIST.md)

## Step-by-Step Setup

1. Create a Google Cloud project.
2. Enable BigQuery API.
3. Create dataset `aviation_intel` for this platform.
4. Create a service account with BigQuery Data Editor access for that dataset.
5. Point `GOOGLE_APPLICATION_CREDENTIALS` to the service account JSON locally.
6. Run the concrete dataset bootstrap SQL.
7. Run the local export staging command.
8. Load staged parquet files into BigQuery using `partition-refresh`.
9. Apply hot-cache retention.
10. Create Looker-facing views.
11. Point hosted API reads to BigQuery-backed tables/views.
12. Connect Looker Studio to the curated dataset views.

## Local Export Example

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-07
```

## Optional Direct Load Example

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-07 --load-bigquery --project-id your-gcp-project --dataset aviation_intel
```

The default direct load mode is `partition-refresh`: dimensions are replaced, fact-table partitions in the export window are deleted, then fresh rows are appended.

## Retention And Audit

Default policy:

- high-volume core scraper fact tables: `35` days
- GDS fare/change/tax fact tables: intentionally excluded for now because their storage footprint is small
- partitioned forecast/backtest fact tables: `90` days
- currently unpartitioned tiny winner tables: intentionally excluded for now because their storage footprint is small
- dimension tables: no expiration
- dataset time travel: `48` hours

Dry-run the retention DDL:

```powershell
.\.venv\Scripts\python.exe tools\bigquery_apply_retention.py --project-id aeropulseintelligence --dataset aviation_intel
```

Apply retention and audit storage:

```powershell
.\.venv\Scripts\python.exe tools\bigquery_apply_retention.py --project-id aeropulseintelligence --dataset aviation_intel --hot-days 35 --forecast-days 90 --time-travel-hours 48 --apply
.\.venv\Scripts\python.exe tools\bigquery_storage_audit.py --project-id aeropulseintelligence --dataset aviation_intel
```

## Schema Note

If `fact_offer_snapshot` already exists in BigQuery, add the new round-trip columns before the next partition-refresh load or rerun the bootstrap SQL against a fresh table set. The canonical schema is in [sql/bigquery/create_aviation_intel_tables.sql](../../sql/bigquery/create_aviation_intel_tables.sql).

For a live additive patch, run [sql/bigquery/alter_aviation_intel_live_schema.sql](../../sql/bigquery/alter_aviation_intel_live_schema.sql) first. It covers:

- round-trip route-monitor columns on `fact_offer_snapshot`
- `via_airports` on `fact_offer_snapshot`
- forecast bundle flags that older live tables may still be missing

## Live Reload Path

For the current production dataset:

1. Open BigQuery SQL workspace and run [sql/bigquery/alter_aviation_intel_live_schema.sql](../../sql/bigquery/alter_aviation_intel_live_schema.sql)
2. Reload the recent window with the loader helper:

```powershell
.\tools\load_bigquery_latest.ps1 -CredentialsJson "C:\path\to\aero-pulse-bq-loader.json" -StartDate 2026-03-03 -EndDate 2026-03-10
```

3. Validate the new column has data:

```sql
SELECT
  COUNTIF(via_airports IS NOT NULL AND via_airports != '') AS rows_with_via_airports,
  COUNTIF(search_trip_type = 'RT') AS round_trip_rows
FROM `aeropulseintelligence.aviation_intel.fact_offer_snapshot`;
```

4. Validate hosted operations reads:

```sql
SELECT
  route_key,
  airline,
  via_airports,
  stops,
  departure_date
FROM `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
WHERE via_airports IS NOT NULL
ORDER BY captured_at_utc DESC
LIMIT 50;
```

---

> **Codex will review your output once you are done.**
