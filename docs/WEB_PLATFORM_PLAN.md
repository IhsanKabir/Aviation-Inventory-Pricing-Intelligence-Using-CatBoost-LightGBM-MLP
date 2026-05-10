# Web Platform Plan

## Objective

Move the main interactive experience away from Excel and into a responsive web application without breaking the existing report pipeline.

Active product requirements and delivery priorities are tracked in [docs/WEB_PRODUCT_REQUIREMENTS.md](WEB_PRODUCT_REQUIREMENTS.md).

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
- consistent date and cycle selection across pages
- operations-oriented route views, not only raw row tables
- scan-first market movement review for changes and taxes

## Current Delivery Priorities

Priority order for the next implementation cycle:

1. Fix active navigation highlighting.
2. Sort route comparison rows by departure time across airlines.
3. Redesign the Changes page for scanning, filtering, and drilldown.
4. Introduce shared date-selection controls.
5. Add generic Excel export based on current filters.
6. Add `DOM` / `INT` route categorization.
7. Hide inventory-estimation columns unless relevant data exists.
8. Upgrade Tax into a comparative monitoring page.
9. Add a market-level Changes dashboard.
10. Add the Airline Operations page.
11. Land round-trip architecture in collection/runtime first, then expose it through API and UI.
12. Expand forecasting and preserve penalty-model integration flexibility.

## Recommended UX Direction

- Keep the top-level pages shallow and operational:
  - overview for status
  - routes for flight-level comparison
  - operations for schedule patterns
  - taxes and changes for market movement
  - forecasting for forward-looking interpretation
- Prefer sticky filters and pinned identity columns on dense analytical pages.
- Default sort order should be chronological when comparing flight options.
- Use shared filter semantics across frontend, API, and Excel export.

## Step-by-Step Delivery Order

1. Stabilize the FastAPI reporting contract.
2. Fix cross-page navigation state and filter consistency.
3. Replace airline-block ordering with departure-time ordering where route comparison is the primary task.
4. Replace Excel-only filtering with API-driven filtering.
5. Add shared date and history controls.
6. Add change-history drilldown plus market-level Changes dashboard.
7. Upgrade tax monitoring into a route-comparison and trend surface.
8. Add Airline Operations route-pattern views.
9. Add ML/DL prediction pages and route-winner views.
10. Add round-trip search and display architecture, starting from one-way-compatible trip metadata.
11. Add cycle health and freshness page.
12. Keep workbook generation as an export path only.

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

The web application becomes the main interactive monitor, Excel remains a downloadable operational artifact, local PostgreSQL remains the collection/training/history store, and BigQuery becomes the bounded hosted hot-cache read layer.

## Round-Trip Note

Round-trip support now starts from shared search-intent metadata instead of replacing the one-way fact model. See [docs/ROUND_TRIP_ARCHITECTURE.md](ROUND_TRIP_ARCHITECTURE.md).

---

> **Codex will review your output once you are done.**
