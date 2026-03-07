# BigQuery Warehouse Plan

## Purpose

BigQuery is the historical analytics layer for this platform. PostgreSQL stays operational; BigQuery holds curated fact tables for BI, thesis analysis, and longer-range query workloads.

## Chosen BI Layer

- BigQuery sandbox for storage and SQL analytics
- Looker Studio for dashboards

This is the strongest free-to-start combination for:

- query management
- portfolio visibility
- dashboard delivery
- thesis-friendly analytics outputs

## Curated Tables

- `dim_airline`
- `dim_route`
- `fact_cycle_run`
- `fact_offer_snapshot`
- `fact_change_event`
- `fact_penalty_snapshot`
- `fact_tax_snapshot`

## Export Contract

Source of truth for export layout:

- [sql/bigquery/create_analytics_tables.sql](../../sql/bigquery/create_analytics_tables.sql)
- [sql/bigquery/create_analytics_views.sql](../../sql/bigquery/create_analytics_views.sql)
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
8. Load staged parquet files into BigQuery.
9. Create Looker-facing views.
10. Connect Looker Studio to the curated dataset views.

## Local Export Example

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-07
```

## Optional Direct Load Example

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-07 --load-bigquery --project-id your-gcp-project --dataset aviation_intel
```
