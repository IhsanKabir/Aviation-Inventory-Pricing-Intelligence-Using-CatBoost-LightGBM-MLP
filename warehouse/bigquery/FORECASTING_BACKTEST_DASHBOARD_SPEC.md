# Forecasting / Backtest Dashboard Spec

Use this as the first Looker Studio dashboard layout for ML/DL forecast review.

## Scope

Purpose:

- review latest forecast bundle freshness
- compare model quality at aggregate and route level
- inspect next-day directional expectations
- support thesis presentation with a stable dashboard layout

Current status:

- forecast and backtest outputs are promoted into BigQuery curated tables
- the operational web page now reads through the API contract that is intended to be warehouse-backed
  - [apps/web/app/forecasting/page.tsx](../../apps/web/app/forecasting/page.tsx)
- Looker Studio should be built directly on the forecast views listed below

## Recommended Dashboard Name

- `Aero Pulse Forecast Review`

## Page Structure

Build this as a 4-page Looker Studio dashboard.

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

## Warehouse Tables

Use these curated tables:

1. `fact_forecast_bundle`
2. `fact_forecast_model_eval`
3. `fact_forecast_route_eval`
4. `fact_forecast_next_day`
5. `fact_backtest_eval`
6. `fact_backtest_split`

Use these Looker-facing views first:

1. `vw_forecast_model_latest`
2. `vw_forecast_route_latest`
3. `vw_forecast_next_day_latest`
4. `vw_backtest_eval_latest`

## Immediate Practical Recommendation

For day-to-day use:

1. use the operational forecasting web page for quick review
2. use BigQuery + Looker Studio for presentation-grade historical forecast analysis
3. add more forecast warehouse fields only after the forecast schema stabilizes further

## Exact Looker Build Sequence

Page 1: `Forecast Health`

1. Data source: `vw_forecast_model_latest`
2. Add scorecards:
   - `target`
   - `bundle_created_at_utc`
   - `COUNT_DISTINCT(model)`
   - `MIN(mae)`
   - `MIN(rmse)`
3. Add table with:
   - dimension: `model`
   - metrics:
     - `n`
     - `mae`
     - `rmse`
     - `mape_pct`
     - `smape_pct`
     - `directional_accuracy_pct`
     - `f1_macro`
4. Sort by `mae` ascending

Page 2: `Route Performance`

1. Data source: `vw_forecast_route_latest`
2. Add filter controls:
   - `airline`
   - `route_key`
   - `cabin`
   - `model`
3. Add pivot table:
   - rows: `airline`
   - columns: `route_key`
   - metric: `mae`
4. Add detail table:
   - dimensions:
     - `airline`
     - `origin`
     - `destination`
     - `cabin`
     - `model`
   - metrics:
     - `n`
     - `mae`
     - `rmse`
     - `directional_accuracy_pct`

Page 3: `Next-Day Outlook`

1. Data source: `vw_forecast_next_day_latest`
2. Add filter controls:
   - `predicted_for_day`
   - `airline`
   - `route_key`

Page 4: `Backtest Review`

1. Data source: `vw_backtest_eval_latest`
2. Add scorecards:
   - `COUNT(*)`
   - `MIN(mae)`
   - `MIN(rmse)`
3. Add table:
   - dimensions:
     - `dataset`
     - `target`
     - `model`
   - metrics:
     - `n`
     - `mae`
     - `rmse`
     - `mape_pct`
     - `smape_pct`
     - `directional_accuracy_pct`
     - `f1_macro`
4. Add filter controls:
   - `dataset`
   - `target`
   - `model`

## Live Report Status

Live report currently deployed in the signed-in workspace:

- `Aero Pulse Forecast Review`
- `https://lookerstudio.google.com/reporting/896e47f1-6d52-45ae-b1d7-d9034a2db69f`

Current completion snapshot:

1. `Forecast Health`
   - materially complete
   - live visuals present for model table, `mae`, and directional accuracy

2. `Route Performance`
   - materially complete
   - detail table and scatter are live
   - report-level `airline` quick filter exists

3. `Next-Day Outlook`
   - materially usable
   - route chart and `route_key` dropdown are live

4. `Backtest Review`
   - materially usable
   - bundle table and `dataset` dropdown are live

Remaining work is presentation polish, not warehouse wiring.
   - `cabin`
3. Add detail table:
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
     - `pred_ewm_alpha_0_30`
     - `pred_dl_mlp_q50`
     - `pred_ml_catboost_q50`
     - `pred_ml_lightgbm_q50`
4. Add bar chart:
   - dimension: `route_key`
   - metric: `pred_dl_mlp_q50`

Page 4: `Backtest Review`

1. Data source: `vw_backtest_eval_latest`
2. Add scorecards:
   - `backtest_status`
   - `backtest_split_count`
   - `MIN(mae)`
   - `AVG(directional_accuracy_pct)`
3. Add filter controls:
   - `dataset`
   - `model`
   - `split_id`
4. Add table:
   - dimensions:
     - `split_id`
     - `dataset`
     - `model`
   - metrics:
     - `selected_on_val`
     - `n`
     - `mae`
     - `rmse`
     - `mape_pct`
     - `smape_pct`
     - `directional_accuracy_pct`
     - `f1_macro`

---

> **Codex will review your output once you are done.**
