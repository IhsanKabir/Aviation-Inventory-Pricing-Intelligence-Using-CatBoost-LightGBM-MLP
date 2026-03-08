# Web Platform Plan

## Objective

Move the main interactive experience away from Excel and into a responsive web application without breaking the existing report pipeline.

## Chosen Architecture

- Local collection/training backend:
  - Python collection pipeline
  - local PostgreSQL for raw operational state, comparisons, and feature generation

- Hosted reporting backend:
  - FastAPI
  - BigQuery curated reads for public/runtime pages

- Frontend:
  - Next.js
  - API-driven route, airline, and signal filtering

- Analytics:
  - BigQuery sandbox
  - Looker Studio for dashboards
  - ML/DL evaluation and route-winner outputs published from local training runs

## Why This Is Better Than Extending Excel

- Excel is now acting as both report output and interaction surface.
- The workbook is large, macro-heavy, and expensive to recalculate.
- Button-driven row/column masking will keep getting slower as route and history coverage grows.
- The web application can push filtering and aggregation into SQL instead of VBA.

## Backend Scope

The API should expose:

- latest cycle snapshot / freshness
- route-monitor matrix
- route-level change summary
- detailed field-level change log
- penalty snapshot
- tax snapshot
- forecasting bundle, route winners, backtest route winners
- filter metadata (airlines, routes)

## Frontend Scope

The web app should support:

- route-first browsing
- airline and signal toggles
- fast history expansion
- penalty/tax tabs
- prediction tabs
- export links back to workbook outputs

## Step-by-Step Delivery Order

1. Stabilize the FastAPI reporting contract.
2. Build the Next.js route monitor application.
3. Replace Excel-only filtering with API-driven filtering.
4. Move hosted read paths from PostgreSQL to BigQuery.
5. Add change-history drilldown.
6. Add penalty and tax views.
7. Add ML/DL prediction pages and route-winner views.
8. Add cycle health and freshness page.
9. Keep workbook generation as an export path only.

## Current File Role Assessment

- `run_pipeline.py`
  Main orchestrator. Keep.

- `run_all.py`
  Main collection runner. Keep.

- `generate_reports.py`
  Standard reporting pack. Keep.

- `generate_route_flight_fare_monitor.py`
  Specialized workbook output. Keep.

- `predict_next_day.py`
  Core forecasting entry point. Keep.

- `strategy_engine.py`
  Optional experimental layer. It is not required for the current report path or the new web/API path.

## Success Condition

The web application becomes the main interactive monitor, Excel remains a downloadable operational artifact, local PostgreSQL remains the collection/training store, and BigQuery becomes both the historical analytics layer and the hosted read layer.
