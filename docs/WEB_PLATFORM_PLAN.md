# Web Platform Plan

## Objective

Move the main interactive experience away from Excel and into a responsive web application without breaking the existing report pipeline.

## Chosen Architecture

- Backend:
  - FastAPI
  - PostgreSQL reporting queries

- Frontend:
  - Next.js
  - API-driven route, airline, and signal filtering

- Analytics:
  - BigQuery sandbox
  - Looker Studio for dashboards

## Why This Is Better Than Extending Excel

- Excel is now acting as both report output and interaction surface.
- The workbook is large, macro-heavy, and expensive to recalculate.
- Button-driven row/column masking will keep getting slower as route and history coverage grows.
- The web application can push filtering and aggregation into SQL instead of VBA.

## Backend Scope

The API should expose:

- latest cycle snapshot
- recent cycle list
- route-level change summary
- detailed field-level change log
- penalty snapshot
- tax snapshot
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
2. Build the Next.js route monitor shell.
3. Replace Excel-only filtering with API-driven filtering.
4. Add change-history drilldown.
5. Add penalty and tax views.
6. Add ML/DL prediction pages.
7. Add cycle health and freshness page.
8. Keep workbook generation as an export path only.

## Current File Role Assessment

- `run_pipeline.py`
  Main orchestrator. Keep.

- `run_all.py`
  Main source-capture runner. Keep.

- `generate_reports.py`
  Standard reporting pack. Keep.

- `generate_route_flight_fare_monitor.py`
  Specialized workbook output. Keep.

- `predict_next_day.py`
  Core forecasting entry point. Keep.

- `strategy_engine.py`
  Optional experimental layer. It is not required for the current report path or the new web/API path.

## Success Condition

The web application becomes the main interactive monitor, while Excel remains a downloadable operational artifact and BigQuery becomes the historical analytics layer.
