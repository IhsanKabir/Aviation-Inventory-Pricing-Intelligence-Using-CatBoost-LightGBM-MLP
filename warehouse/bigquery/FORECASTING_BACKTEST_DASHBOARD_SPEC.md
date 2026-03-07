# Forecasting / Backtest Dashboard Spec

Use this as the first Looker Studio dashboard layout for ML/DL forecast review.

## Scope

Purpose:

- review latest forecast bundle freshness
- compare model quality at aggregate and route level
- inspect next-day directional expectations
- support thesis presentation with a stable dashboard layout

Important constraint:

- current forecast artifacts are file-based outputs under `output/reports`
- they are exposed operationally through the web page:
  - [apps/web/app/forecasting/page.tsx](../../apps/web/app/forecasting/page.tsx)
- they are not yet loaded into BigQuery curated tables

So this spec is the correct dashboard layout to build next, but the BigQuery data source for it should be added only after forecast artifacts are promoted into warehouse tables.

## Recommended Dashboard Name

- `Aero Pulse Forecast Review`

## Page Structure

Build this as a 3-page Looker Studio dashboard.

### Page 1: Forecast Health

Goal:

- show whether the latest forecast run is fresh and usable

Top scorecards:

1. latest prediction target
2. latest bundle timestamp
3. best prediction MAE
4. best backtest MAE
5. model count in latest bundle
6. backtest split count

Main visuals:

1. table: latest model ranking
   - dimensions:
     - `model`
   - metrics:
     - `mae`
     - `rmse`
     - `mape_pct`
     - `smape_pct`
     - `directional_accuracy_pct`
     - `f1_macro`

2. bar chart: model vs MAE
3. bar chart: model vs directional accuracy

Filters:

1. target
2. model
3. bundle timestamp

### Page 2: Route Performance

Goal:

- identify which airline-route pairs forecast well and which do not

Top scorecards:

1. route rows evaluated
2. best route-level MAE
3. best route-level directional accuracy
4. distinct airline count

Main visuals:

1. heatmap or pivot:
   - dimensions:
     - `airline`
     - `route_key`
   - metrics:
     - `mae`
     - `directional_accuracy_pct`

2. table: route-level model performance
   - dimensions:
     - `airline`
     - `origin`
     - `destination`
     - `cabin`
     - `model`
   - metrics:
     - `n`
     - `mae`
     - `directional_accuracy_pct`

3. scatter plot:
   - x: `n`
   - y: `mae`
   - breakdown: `airline`

Filters:

1. airline
2. route
3. cabin
4. model

### Page 3: Next-Day Outlook

Goal:

- show forward-looking predicted movement for the next operating day

Top scorecards:

1. next-day row count
2. average predicted change
3. max predicted upside
4. max predicted downside

Main visuals:

1. table: next-day prediction output
   - dimensions:
     - `predicted_for_day`
     - `airline`
     - `origin`
     - `destination`
     - `cabin`
   - metrics:
     - `latest_actual_value`
     - `pred_last_value`
     - `pred_rolling_mean_3`
     - `pred_ewm_alpha_0.30`
     - `pred_dl_mlp_q50`

2. bar chart:
   - dimension: `route_key`
   - metric: `pred_dl_mlp_q50`

3. filtered scorecards for selected airline / route

Filters:

1. predicted day
2. airline
3. route
4. cabin

## Visual Style

Use the same presentation logic as the web app:

- calm paper background
- deep blue for operational state
- warm gold for benchmark / emphasis
- muted red only for deterioration or error emphasis

Avoid:

- heavy rainbow palettes
- default Looker bright colors without overrides

## Metric Definitions

Use these labels:

- `MAE`: mean absolute error
- `RMSE`: root mean squared error
- `MAPE %`: mean absolute percentage error
- `sMAPE %`: symmetric mean absolute percentage error
- `Directional Accuracy %`: percent of correct movement direction
- `F1 Macro`: classification balance across movement classes

## Warehouse Promotion Plan

To make this dashboard fully BigQuery-backed, add these future curated tables:

1. `fact_forecast_bundle`
2. `fact_forecast_model_eval`
3. `fact_forecast_route_eval`
4. `fact_forecast_next_day`
5. `fact_backtest_eval`

Suggested source:

- parse the latest forecast bundle outputs already surfaced by:
  - [apps/api/app/repositories/reporting.py](../../apps/api/app/repositories/reporting.py)

## Immediate Practical Recommendation

For now:

1. use the operational forecasting web page for daily review
2. use BigQuery + Looker Studio first for cycle, route, penalty, tax, and change analysis
3. promote forecast artifacts into warehouse tables only after the forecast schema is stabilized
