# BigQuery Bootstrap Checklist

Project target:

- `aeropulseintelligence`

Suggested dataset:

- `aviation_intel`

Suggested region:

- choose one region and keep it consistent for all warehouse assets
- if no existing constraint, use a single-region location close to your reporting workflow

## Step 1: Confirm project access

In Google Cloud Console:

1. Open project `aeropulseintelligence`
2. Confirm billing-free sandbox or available BigQuery sandbox access
3. Confirm you can create datasets in the selected project

## Step 2: Enable required APIs

Enable:

1. BigQuery API
2. IAM API if not already enabled

## Step 3: Create the dataset

Dataset settings:

1. Dataset ID: `aviation_intel`
2. Location: one fixed region
3. Default table expiration: none
4. Default partition expiration: `35` days
5. Time travel window: `48` hours

## Step 4: Create service account

Recommended name:

- `aero-pulse-bq-loader`

Grant:

1. `BigQuery Data Editor` on dataset `aviation_intel`
2. `BigQuery Job User` on project `aeropulseintelligence`

Download the JSON key only if you will load from local scripts.

## Step 5: Set local environment

PowerShell:

```powershell
$env:BIGQUERY_PROJECT_ID="aeropulseintelligence"
$env:BIGQUERY_DATASET="aviation_intel"
$env:BIGQUERY_SYNC_ENABLED="1"
$env:BIGQUERY_SYNC_LOOKBACK_DAYS="2"
$env:BIGQUERY_LOAD_MODE="partition-refresh"
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\\path\\to\\aero-pulse-bq-loader.json"
```

Or use the helper script:

```powershell
.\tools\load_bigquery_latest.ps1 -CredentialsJson "C:\path\to\aero-pulse-bq-loader.json" -StartDate 2026-03-01 -EndDate 2026-03-08
```

## Step 6: Create the BigQuery tables and views

Run in order:

1. [sql/bigquery/create_aviation_intel_dataset.sql](../../sql/bigquery/create_aviation_intel_dataset.sql)
2. [sql/bigquery/create_aviation_intel_tables.sql](../../sql/bigquery/create_aviation_intel_tables.sql)
3. [sql/bigquery/create_aviation_intel_looker_views.sql](../../sql/bigquery/create_aviation_intel_looker_views.sql)

## Step 7: Stage export files locally

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-08
```

Check:

1. parquet files are created
2. `manifest.json` is written
3. row counts look reasonable

## Step 8: Load to BigQuery

Direct from script:

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-08 --load-bigquery --project-id aeropulseintelligence --dataset aviation_intel
```

Default direct loads use `partition-refresh`: dimensions are replaced, exported fact partitions are refreshed, and routine syncs avoid unbounded append growth.

Apply cost-safe retention after table creation:

```powershell
.\.venv\Scripts\python.exe tools\bigquery_apply_retention.py --project-id aeropulseintelligence --dataset aviation_intel --hot-days 35 --forecast-days 90 --time-travel-hours 48 --apply
.\.venv\Scripts\python.exe tools\bigquery_storage_audit.py --project-id aeropulseintelligence --dataset aviation_intel
```

If the dataset already exists and you are applying new additive columns to a live table set, run this first:

1. [sql/bigquery/alter_aviation_intel_live_schema.sql](../../sql/bigquery/alter_aviation_intel_live_schema.sql)

Current live patch coverage:

- `fact_offer_snapshot.via_airports`
- round-trip route-monitor fields on `fact_offer_snapshot`
- forecast bundle flags on `fact_forecast_bundle`

## Step 9: Validate warehouse tables

Minimum checks:

1. `fact_cycle_run` has the latest cycle IDs
2. `fact_offer_snapshot` row counts roughly track PostgreSQL cycle exports
3. `fact_change_event` contains route and field-level changes
4. `fact_penalty_snapshot` and `fact_tax_snapshot` contain non-empty rows where expected
5. forecast/backtest tables contain rows where expected:
   - `fact_forecast_bundle`
   - `fact_forecast_model_eval`
   - `fact_forecast_route_eval`
   - `fact_forecast_route_winner`
   - `fact_forecast_next_day`
   - `fact_backtest_eval`
   - `fact_backtest_route_winner`
   - `fact_backtest_split`
6. Looker-facing views resolve without errors:
   - `vw_cycle_health`
   - `vw_route_daily_fare`
   - `vw_change_activity_daily`
   - `vw_penalty_reference`
   - `vw_tax_reference`
   - `vw_forecast_model_latest`
   - `vw_forecast_route_latest`
   - `vw_forecast_route_winner_latest`
   - `vw_forecast_next_day_latest`
   - `vw_backtest_eval_latest`
   - `vw_backtest_route_winner_latest`

Additional current checks for the via-airport rollout:

```sql
SELECT
  COUNTIF(via_airports IS NOT NULL AND via_airports != '') AS rows_with_via_airports,
  COUNTIF(search_trip_type = 'RT') AS round_trip_rows
FROM `aeropulseintelligence.aviation_intel.fact_offer_snapshot`;
```

```sql
SELECT
  route_key,
  airline,
  via_airports,
  stops,
  departure_date,
  captured_at_utc
FROM `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
WHERE via_airports IS NOT NULL
ORDER BY captured_at_utc DESC
LIMIT 50;
```

## Step 10: Connect Looker Studio

Create initial dashboards:

1. cycle freshness and coverage
2. route-level change activity
3. airline fare movement
4. penalty and tax comparison
5. ML/DL evaluation summaries later

Detailed setup:

- [warehouse/bigquery/LOOKER_STUDIO_SETUP.md](LOOKER_STUDIO_SETUP.md)

Live forecast review report already created:

- `Aero Pulse Forecast Review`
- `https://lookerstudio.google.com/reporting/896e47f1-6d52-45ae-b1d7-d9034a2db69f`

## Recommended order after bootstrap

1. keep loading parquet from local first
2. validate schemas and row counts
3. validate hosted API reads against BigQuery
4. enable automatic post-cycle sync through `run_pipeline.py`

Automatic post-cycle sync controls:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --bigquery-sync-enabled --bigquery-sync-lookback-days 2 --bigquery-load-mode partition-refresh
.\.venv\Scripts\python.exe run_pipeline.py --skip-bigquery-sync
.\.venv\Scripts\python.exe run_pipeline.py --fail-on-bigquery-sync-error
```

Default behavior after this integration:

- if `BIGQUERY_SYNC_ENABLED=1`, `BIGQUERY_PROJECT_ID`, and `BIGQUERY_DATASET` are configured, successful pipeline runs refresh the bounded BigQuery hot cache
- the sync window is recent UTC capture dates, not just the single latest cycle id
- manual `tools/export_bigquery_stage.py --load-bigquery ...` remains available for backfills

## Current production reload path

Use this exact sequence for the live `aeropulseintelligence.aviation_intel` dataset:

1. Run [sql/bigquery/alter_aviation_intel_live_schema.sql](../../sql/bigquery/alter_aviation_intel_live_schema.sql)
2. Reload the recent window:

```powershell
.\tools\load_bigquery_latest.ps1 -CredentialsJson "C:\path\to\aero-pulse-bq-loader.json" -StartDate 2026-03-03 -EndDate 2026-03-10
```

3. Refresh the hosted web/API after the load completes

## Hosted API note

For the preferred zero-cost hosted deployment:

- Cloud Run should be configured with:
  - `API_FORECASTING_SOURCE=bigquery`
  - `BIGQUERY_PROJECT_ID=aeropulseintelligence`
  - `BIGQUERY_DATASET=aviation_intel`
- `AIRLINE_DB_URL` should be omitted unless you intentionally keep transitional PostgreSQL-backed endpoints enabled.
