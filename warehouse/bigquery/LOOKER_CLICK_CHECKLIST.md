# Looker Click Checklist

Use this during the actual Looker Studio build session after signing in with the Google account that has access to project `aeropulseintelligence`.

## Open

1. Go to `https://lookerstudio.google.com/`
2. Click `Blank Report`
3. Click `Add data`
4. Choose connector `BigQuery`
5. Project: `aeropulseintelligence`
6. Dataset: `aviation_intel`

## Add Data Sources

Add these sources in this order:

1. `vw_forecast_model_latest`
2. `vw_forecast_route_latest`
3. `vw_forecast_next_day_latest`
4. `vw_backtest_eval_latest`

Use:

1. `Resource`
2. `Manage added data sources`
3. `Add a data source`

until all four views are attached to the same report.

## Page 1: Forecast Health

Page name:

- `Forecast Health`

Data source:

- `vw_forecast_model_latest`

Add:

1. Scorecard: `target`
2. Scorecard: `bundle_created_at_utc`
3. Scorecard: `COUNT_DISTINCT(model)`
4. Scorecard: `MIN(mae)`
5. Scorecard: `MIN(rmse)`

Then add a table:

Dimensions:

- `model`

Metrics:

- `n`
- `mae`
- `rmse`
- `mape_pct`
- `smape_pct`
- `directional_accuracy_pct`
- `f1_macro`

Sort:

- `mae` ascending

## Page 2: Route Performance

Page name:

- `Route Performance`

Data source:

- `vw_forecast_route_latest`

Add filter controls:

1. `airline`
2. `route_key`
3. `cabin`
4. `model`

Add a pivot table:

Rows:

- `airline`

Columns:

- `route_key`

Metric:

- `mae`

Add a detail table:

Dimensions:

- `airline`
- `origin`
- `destination`
- `cabin`
- `model`

Metrics:

- `n`
- `mae`
- `rmse`
- `directional_accuracy_pct`

## Page 3: Next-Day Outlook

Page name:

- `Next-Day Outlook`

Data source:

- `vw_forecast_next_day_latest`

Add filter controls:

1. `predicted_for_day`
2. `airline`
3. `route_key`
4. `cabin`

Add a detail table:

Dimensions:

- `predicted_for_day`
- `airline`
- `origin`
- `destination`
- `cabin`

Metrics:

- `latest_actual_value`
- `pred_last_value`
- `pred_rolling_mean_3`
- `pred_ewm_alpha_0_30`
- `pred_dl_mlp_q50`
- `pred_ml_catboost_q50`
- `pred_ml_lightgbm_q50`

Add a bar chart:

Dimension:

- `route_key`

Metric:

- `pred_dl_mlp_q50`

## Page 4: Backtest Review

Page name:

- `Backtest Review`

Data source:

- `vw_backtest_eval_latest`

Add scorecards:

1. `backtest_status`
2. `backtest_split_count`
3. `MIN(mae)`
4. `AVG(directional_accuracy_pct)`

Add filter controls:

1. `dataset`
2. `model`
3. `split_id`

Add a table:

Dimensions:

- `split_id`
- `dataset`
- `model`

Metrics:

- `selected_on_val`
- `n`
- `mae`
- `rmse`
- `mape_pct`
- `smape_pct`
- `directional_accuracy_pct`
- `f1_macro`

## Final Checks

1. Confirm every chart uses the intended view
2. Confirm scorecards are not aggregating text fields incorrectly
3. Set report title:
   - `Aero Pulse Forecast Review`
4. Save the report
5. Share with view access only unless editing is needed

## Reference

- [warehouse/bigquery/FORECASTING_BACKTEST_DASHBOARD_SPEC.md](FORECASTING_BACKTEST_DASHBOARD_SPEC.md)
- [warehouse/bigquery/LOOKER_STUDIO_SETUP.md](LOOKER_STUDIO_SETUP.md)

---

> **Codex will review your output once you are done.**
