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
6. Add these tables first:
   - `fact_cycle_run`
   - `fact_offer_snapshot`
   - `fact_change_event`
   - `fact_penalty_snapshot`
   - `fact_tax_snapshot`

## First dashboards to build

1. Cycle freshness
   - latest cycle completion time
   - offer row count
   - airline count
   - route count

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
