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

Detailed layout spec:

- [warehouse/bigquery/FORECASTING_BACKTEST_DASHBOARD_SPEC.md](FORECASTING_BACKTEST_DASHBOARD_SPEC.md)

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
