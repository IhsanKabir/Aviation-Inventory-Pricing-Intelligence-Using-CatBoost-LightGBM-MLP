# Looker Studio Setup

Use Looker Studio as the dashboard layer on top of the curated `aviation_intel` dataset.

## Prerequisites

- BigQuery dataset `aviation_intel` exists
- curated tables are loaded
- you can query the dataset from BigQuery console

## Connect Looker Studio

1. Open Looker Studio
2. Create a new data source
3. Choose `BigQuery`
4. Select project `aeropulseintelligence`
5. Select dataset `aviation_intel`
6. Add these views first:
   - `vw_cycle_health`
   - `vw_route_daily_fare`
   - `vw_change_activity_daily`
   - `vw_penalty_reference`
   - `vw_tax_reference`
   - `vw_forecast_model_latest`
   - `vw_forecast_route_latest`
   - `vw_forecast_next_day_latest`
   - `vw_backtest_eval_latest`

## First dashboards to build

1. Cycle freshness
   - latest cycle completion time
   - offer row count
   - airline count
   - route count
   - cycle age in minutes

2. Route movement
   - route-level fare movement by day
   - change event count by route
   - airline distribution by route

3. Carrier comparison
   - minimum fare by airline and route
   - tax comparison
   - penalty comparison

4. Forecast review
   - predicted vs actual movement
   - route-level error trend
   - model performance summary
   - backtest split review and model ranking

Detailed layout spec:

- [warehouse/bigquery/FORECASTING_BACKTEST_DASHBOARD_SPEC.md](FORECASTING_BACKTEST_DASHBOARD_SPEC.md)
- [warehouse/bigquery/LOOKER_CLICK_CHECKLIST.md](LOOKER_CLICK_CHECKLIST.md)

## Exact Forecast Report Build

Use these views for the forecast report pages:

1. `vw_forecast_model_latest`
   - page name: `Forecast Health`
2. `vw_forecast_route_latest`
   - page name: `Route Performance`
3. `vw_forecast_next_day_latest`
   - page name: `Next-Day Outlook`
4. `vw_backtest_eval_latest`
   - page name: `Backtest Review`

Build the page visuals exactly as specified in:

- [warehouse/bigquery/FORECASTING_BACKTEST_DASHBOARD_SPEC.md](FORECASTING_BACKTEST_DASHBOARD_SPEC.md)

Live report in workspace:

- `Aero Pulse Forecast Review`
- `https://lookerstudio.google.com/reporting/896e47f1-6d52-45ae-b1d7-d9034a2db69f`

Current live status:

1. `Forecast Health`
   - live and warehouse-backed
   - core visuals present:
     - MAE scorecard
     - model ranking table
     - model vs `mae` bar chart
     - model vs `directional_accuracy_pct` bar chart
   - remaining polish:
     - add more summary scorecards if presentation density is needed
     - add target/model quick filters if manual canvas work is preferred

2. `Route Performance`
   - live and warehouse-backed
   - core visuals present:
     - route performance chart
     - route-model detail table
     - scatter plot
   - report-level `airline` quick filter is present
   - remaining polish:
     - add `route_key`, `cabin`, and `model` quick filters manually if needed
     - tighten visual alignment

3. `Next-Day Outlook`
   - live and warehouse-backed
   - core visuals present:
     - route-level next-day bar chart
     - `route_key` dropdown filter
   - remaining polish:
     - add the larger detail table and extra summary scorecards if needed

4. `Backtest Review`
   - live and warehouse-backed
   - core visuals present:
     - record count scorecard
     - bundle table
     - `dataset` dropdown filter
   - remaining polish:
     - add additional summary scorecards and a fuller comparison table if needed

## Recommended dimensions and metrics

- dimensions:
  - `cycle_id`
  - `captured_at_utc`
  - `report_day`
  - `airline`
  - `origin`
  - `destination`
  - `route_key`
  - `flight_number`
  - `cabin`

- metrics:
  - `offer_rows`
  - `COUNT(*)`
  - `SUM(total_price_bdt)`
  - `AVG(total_price_bdt)`
  - `AVG(tax_amount)`
  - `COUNT(change_type)`
  - `COUNT(DISTINCT route_key)`
  - `COUNT(DISTINCT airline)`

## Practical rule

Use PostgreSQL + API for operational monitoring.

Use BigQuery + Looker Studio for:

- historical analysis
- trend dashboards
- thesis visuals
- executive-style query and chart outputs

---

> **Codex will review your output once you are done.**
