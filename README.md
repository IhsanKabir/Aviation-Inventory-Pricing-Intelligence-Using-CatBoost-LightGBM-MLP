# Aero Pulse Intelligence Platform

Multi-airline fare, inventory, OTA benchmarking, reporting, and forecasting pipeline for thesis-grade aviation intelligence work.

## What This Repository Does

This project collects flight offer data from airline-direct and OTA channels, stores normalized snapshots in PostgreSQL, generates operational Excel reports, and trains forecasting models for fare and inventory movement.

Current implemented scope:

- cycle-based multi-airline accumulation
- route/flight fare monitoring reports in `.xlsx` and `.xlsm`
- OTA benchmarking support for interim carrier coverage
- penalty and tax comparison sheets
- ML/DL prediction pipeline with optional `catboost`, `lightgbm`, and `mlp`

Core project decisions and operating policy live in:

- [PROJECT_DECISIONS.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/PROJECT_DECISIONS.md)
- [OPERATIONS_RUNBOOK.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/OPERATIONS_RUNBOOK.md)

## Main Entry Points

- [run_pipeline.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/run_pipeline.py)
  End-to-end accumulation + reports + optional prediction/intelligence steps.

- [run_all.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/run_all.py)
  Core scrape/normalize/persist runner.

- [generate_reports.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/generate_reports.py)
  Standard reporting pack generator.

- [generate_route_flight_fare_monitor.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/generate_route_flight_fare_monitor.py)
  Dedicated route-flight fare monitor workbook, including macro-enabled `.xlsm` export.

- [predict_next_day.py](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/predict_next_day.py)
  Forecasting layer for event and numeric target prediction.

## Data Flow

1. Scrape airline and OTA offers.
2. Normalize into PostgreSQL tables such as `flight_offers` and `flight_offer_raw_meta`.
3. Group parallel airline runs into one shared `cycle_id`.
4. Compare current vs previous cycle snapshots.
5. Generate operational Excel outputs and forecasting artifacts.

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

Run local CI checks:

```powershell
.\.venv\Scripts\python.exe tools\ci_checks.py --allow-db-skip --reports-dir output/reports --timestamp-tz local
```

## Repository Layout

- `config/`
  Airline, route, schedule, market, and date configuration.

- `modules/`
  Source connectors and airline/OTA fetch logic.

- `engines/`
  Comparison, reporting, route-scoping, and workbook rendering logic.

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
- PostgreSQL remains the operational source of truth for current comparisons and model training.

## Recommended Next Platform Split

- Operational interactive dashboard:
  Build a website on top of PostgreSQL for fast filter/search/change-history browsing.

- Historical analytics and BI:
  Publish curated fact tables to BigQuery and build dashboards in Looker Studio or another BI layer.

- Excel:
  Keep as export/delivery format, not as the primary interactive analysis surface.

## Status

The repository currently supports:

- parallel cycle-based accumulation
- route-level and flight-level monitoring
- OTA pricing normalization
- penalty and tax comparison outputs
- ML/DL forecasting groundwork with expanding feature engineering

Use [PROJECT_DECISIONS.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/PROJECT_DECISIONS.md) for strategic scope and [OPERATIONS_RUNBOOK.md](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/OPERATIONS_RUNBOOK.md) for command-level operations.
