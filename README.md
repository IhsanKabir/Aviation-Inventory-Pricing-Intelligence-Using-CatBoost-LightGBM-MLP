# Aero Pulse Intelligence Platform

Multi-airline fare, inventory, OTA benchmarking, reporting, and forecasting platform for thesis-grade aviation intelligence work.

## Why This Repository Is Strong

This repository is an end-to-end airline intelligence pipeline, from data collection and normalization to reporting, forecasting, API delivery, and hosted analytics.

End-to-end pipeline:

`Airline + OTA collection -> normalization -> PostgreSQL cycle snapshots -> Excel/report outputs -> ML/DL forecasting -> BigQuery curated warehouse -> FastAPI -> Next.js web app`

It is built around:

1. Clear architecture
2. Modular code
3. Reproducibility
4. Deployment capability
5. Documentation

## What This Project Does

This project captures flight offer data from airline-direct and OTA channels, stores normalized cycle snapshots in local PostgreSQL for operational collection/training, generates operational Excel workbooks, and publishes curated analytics plus ML/DL outputs into BigQuery for hosted reads and BI.

Current implemented scope:

- cycle-based multi-airline accumulation
- route/flight fare monitoring reports in `.xlsx` and `.xlsm`
- OTA benchmarking support for interim carrier coverage
- penalty and tax comparison sheets
- ML/DL prediction pipeline with optional `catboost`, `lightgbm`, and `mlp`
- FastAPI reporting API for the web phase
- BigQuery-backed hosted analytics, forecasting, and BI handoff

Core project decisions and operating policy live in:

- [PROJECT_DECISIONS.md](PROJECT_DECISIONS.md)
- [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md)
- [docs/WEB_PLATFORM_PLAN.md](docs/WEB_PLATFORM_PLAN.md)
- [warehouse/bigquery/README.md](warehouse/bigquery/README.md)

## Main Entry Points

- [run_pipeline.py](run_pipeline.py)
  End-to-end accumulation + reports + optional prediction/intelligence steps.

- [run_all.py](run_all.py)
  Core collection, normalization, and persistence runner.

- [generate_reports.py](generate_reports.py)
  Standard reporting pack generator.

- [generate_route_flight_fare_monitor.py](generate_route_flight_fare_monitor.py)
  Dedicated route-flight fare monitor workbook, including macro-enabled `.xlsm` export.

- [predict_next_day.py](predict_next_day.py)
  Forecasting layer for event and numeric target prediction.

- [apps/api/README.md](apps/api/README.md)
  Reporting API scaffold with BigQuery-first hosted reads and optional PostgreSQL transitional endpoints.

## Data Flow

1. Collect airline-direct and OTA offers through channel-specific connectors.
2. Normalize into PostgreSQL tables such as `flight_offers` and `flight_offer_raw_meta`.
3. Group parallel airline runs into one shared `cycle_id`.
4. Compare current vs previous cycle snapshots.
5. Generate operational Excel outputs, API-ready reporting views, and forecasting artifacts.
6. After successful pipeline runs, export/load a rolling recent capture window into BigQuery for hosted reads, BI, and long-horizon analytics.

## Target Platform Split

- Operational collection and training:
  local PostgreSQL + Python pipeline.

- Hosted application:
  FastAPI reporting API + Next.js frontend on top of BigQuery curated reads.

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

See [core/runtime_config.py](core/runtime_config.py).

## Common Commands

Run the full pipeline:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --route-monitor --route-monitor-macro-xlsm
```

Automatic BigQuery sync now runs after a successful `run_pipeline.py` execution when:

- `BIGQUERY_PROJECT_ID` is configured
- `BIGQUERY_DATASET` is configured
- `--skip-bigquery-sync` is not used

Useful controls:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --bigquery-sync-lookback-days 7
.\.venv\Scripts\python.exe run_pipeline.py --skip-bigquery-sync
.\.venv\Scripts\python.exe run_pipeline.py --fail-on-bigquery-sync-error
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
  Channel connectors and airline/OTA collection logic.

- `engines/`
  Comparison, reporting, route-scoping, and workbook rendering logic.

- `apps/api/`
  Reporting API scaffold for the web application, with BigQuery-first hosted reads and optional PostgreSQL transitional endpoints.

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
- PostgreSQL remains the operational source of truth for local current-cycle comparisons and model training.
- Hosted read surfaces should progressively move to BigQuery so the public/runtime app does not depend on paid managed PostgreSQL.

## File Role Notes

- [strategy_engine.py](strategy_engine.py)
  Experimental signal-derivation layer. It is not part of the main reporting path and is now treated as optional.

- [generate_reports.py](generate_reports.py)
  Standard pack generator for broad report output.

- [generate_route_flight_fare_monitor.py](generate_route_flight_fare_monitor.py)
  Specialized route-flight workbook generator for the operational monitor.

## Status

The repository currently supports:

- parallel cycle-based accumulation
- route-level and flight-level monitoring
- OTA pricing normalization
- penalty and tax comparison outputs
- ML/DL forecasting groundwork with expanding feature engineering
- first-pass API and warehouse scaffolding for the web and BI layers

Use [PROJECT_DECISIONS.md](PROJECT_DECISIONS.md) for strategic scope and [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md) for command-level operations.
