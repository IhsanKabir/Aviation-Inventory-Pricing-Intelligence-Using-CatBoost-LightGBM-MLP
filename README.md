# Aero Pulse Intelligence Platform

Multi-airline fare, inventory, OTA benchmarking, reporting, and forecasting platform for thesis-grade aviation intelligence work.

## What This Repository Does

This project captures flight offer data from airline-direct and OTA channels, stores normalized cycle snapshots in PostgreSQL, generates operational Excel workbooks, and trains forecasting models for fare and inventory movement.

Current implemented scope:

- cycle-based multi-airline accumulation
- route/flight fare monitoring reports in `.xlsx` and `.xlsm`
- OTA benchmarking support for interim carrier coverage
- penalty and tax comparison sheets
- ML/DL prediction pipeline with optional `catboost`, `lightgbm`, and `mlp`
- FastAPI reporting API scaffold for the next web phase
- BigQuery export scaffold for analytics and BI handoff

Core project decisions and operating policy live in:

- [PROJECT_DECISIONS.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/PROJECT_DECISIONS.md)
- [OPERATIONS_RUNBOOK.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/OPERATIONS_RUNBOOK.md)
- [docs/WEB_PLATFORM_PLAN.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/docs/WEB_PLATFORM_PLAN.md)
- [warehouse/bigquery/README.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/warehouse/bigquery/README.md)

## Main Entry Points

- [run_pipeline.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/run_pipeline.py)
  End-to-end accumulation + reports + optional prediction/intelligence steps.

- [run_all.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/run_all.py)
  Core source-capture, normalization, and persistence runner.

- [generate_reports.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/generate_reports.py)
  Standard reporting pack generator.

- [generate_route_flight_fare_monitor.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/generate_route_flight_fare_monitor.py)
  Dedicated route-flight fare monitor workbook, including macro-enabled `.xlsm` export.

- [predict_next_day.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/predict_next_day.py)
  Forecasting layer for event and numeric target prediction.

- [apps/api/README.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/apps/api/README.md)
  Reporting API scaffold on top of PostgreSQL.

## Data Flow

1. Collect airline-direct and OTA offers through source-specific connectors.
2. Normalize into PostgreSQL tables such as `flight_offers` and `flight_offer_raw_meta`.
3. Group parallel airline runs into one shared `cycle_id`.
4. Compare current vs previous cycle snapshots.
5. Generate operational Excel outputs, API-ready reporting views, and forecasting artifacts.
6. Export curated facts to BigQuery for BI and long-horizon analytics.

## Target Platform Split

- Operational application:
  FastAPI reporting API + Next.js frontend on top of PostgreSQL.

- Historical analytics and BI:
  BigQuery curated warehouse + Looker Studio dashboards.

- Excel:
  Keep as export and delivery format, not as the primary interactive analysis surface.

## Environment Setup

Windows / PowerShell oriented setup:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip setuptools wheel
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

If browser-assisted flows are needed:

```powershell
.\.venv\Scripts\python.exe -m playwright install chromium
```

Database configuration is resolved from either:

- `AIRLINE_DB_URL`
- or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

See [core/runtime_config.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/core/runtime_config.py).

## Common Commands

Run the full pipeline:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --route-monitor --route-monitor-macro-xlsm
```

Generate reports only:

```powershell
.\.venv\Scripts\python.exe generate_reports.py --format xlsx --route-monitor --route-monitor-macro-xlsm
```

Generate only the route monitor workbook:

```powershell
.\.venv\Scripts\python.exe generate_route_flight_fare_monitor.py --export-macro-xlsm
```

Run the reporting API locally:

```powershell
.\.venv\Scripts\python.exe -m uvicorn apps.api.app.main:app --reload
```

Stage BigQuery export files:

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-07
```

Run local CI checks:

```powershell
.\.venv\Scripts\python.exe tools\ci_checks.py --allow-db-skip --reports-dir output/reports --timestamp-tz local
```

## Repository Layout

- `config/`
  Airline, route, schedule, market, and date configuration.

- `modules/`
  Source connectors and airline/OTA acquisition logic.

- `engines/`
  Comparison, reporting, route-scoping, and workbook rendering logic.

- `apps/api/`
  PostgreSQL-backed reporting API scaffold for the web application.

- `apps/web/`
  Next.js application plan and route map.

- `warehouse/bigquery/`
  BigQuery table design and BI export contract.

- `tools/`
  CI, diagnostics, browser-assisted capture, export helpers, and maintenance utilities.

- `scheduler/`
  Scheduled execution helpers and maintenance runners.

- `tests/`
  Automated tests and validation coverage.

## Current Operational Constraints

- Some airline-direct endpoints are protected by anti-bot/WAF systems.
- Excel route-monitor workbooks are feature-rich but can become slow under heavy route/history density.
- OTA inventory availability is not exposed consistently for every carrier/channel.
- PostgreSQL remains the operational source of truth for current-cycle comparisons and model training.

## File Role Notes

- [strategy_engine.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/strategy_engine.py)
  Experimental signal-derivation layer. It is not part of the main reporting path and is now treated as optional.

- [generate_reports.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/generate_reports.py)
  Standard pack generator for broad report output.

- [generate_route_flight_fare_monitor.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/generate_route_flight_fare_monitor.py)
  Specialized route-flight workbook generator for the operational monitor.

## Status

The repository currently supports:

- parallel cycle-based accumulation
- route-level and flight-level monitoring
- OTA pricing normalization
- penalty and tax comparison outputs
- ML/DL forecasting groundwork with expanding feature engineering
- first-pass API and warehouse scaffolding for the web and BI layers

Use [PROJECT_DECISIONS.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/PROJECT_DECISIONS.md) for strategic scope and [OPERATIONS_RUNBOOK.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/OPERATIONS_RUNBOOK.md) for command-level operations.
