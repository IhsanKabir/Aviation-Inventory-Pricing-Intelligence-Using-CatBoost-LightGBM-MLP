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
4. Partition expiration: none for now

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
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\\path\\to\\aero-pulse-bq-loader.json"
```

## Step 6: Create the BigQuery tables

Use:

- [sql/bigquery/create_analytics_tables.sql](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/sql/bigquery/create_analytics_tables.sql)

Replace:

1. `__PROJECT_ID__` -> `aeropulseintelligence`
2. `__DATASET__` -> `aviation_intel`

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

## Step 9: Validate warehouse tables

Minimum checks:

1. `fact_cycle_run` has the latest cycle IDs
2. `fact_offer_snapshot` row counts roughly track PostgreSQL cycle exports
3. `fact_change_event` contains route and field-level changes
4. `fact_penalty_snapshot` and `fact_tax_snapshot` contain non-empty rows where expected

## Step 10: Connect Looker Studio

Create initial dashboards:

1. cycle freshness and coverage
2. route-level change activity
3. airline fare movement
4. penalty and tax comparison
5. ML/DL evaluation summaries later

## Recommended order after bootstrap

1. keep loading parquet from local first
2. validate schemas and row counts
3. only then automate scheduled export/load
