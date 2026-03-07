-- Generic Looker-ready BigQuery views for Aero Pulse Intelligence Platform.

CREATE SCHEMA IF NOT EXISTS `__PROJECT_ID__.__DATASET__`;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_cycle_health` AS
SELECT
  cycle_id,
  cycle_started_at_utc,
  cycle_completed_at_utc,
  offer_rows,
  airline_count,
  route_count,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), cycle_completed_at_utc, MINUTE) AS cycle_age_minutes
FROM `__PROJECT_ID__.__DATASET__.fact_cycle_run`;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_route_daily_fare` AS
SELECT
  DATE(captured_at_utc) AS report_day,
  airline,
  origin,
  destination,
  route_key,
  cabin,
  COUNT(*) AS offer_rows,
  MIN(total_price_bdt) AS min_total_price_bdt,
  AVG(total_price_bdt) AS avg_total_price_bdt,
  MAX(total_price_bdt) AS max_total_price_bdt,
  AVG(tax_amount) AS avg_tax_amount,
  AVG(load_factor_pct) AS avg_load_factor_pct,
  SUM(CASE WHEN soldout THEN 1 ELSE 0 END) AS soldout_rows
FROM `__PROJECT_ID__.__DATASET__.fact_offer_snapshot`
GROUP BY report_day, airline, origin, destination, route_key, cabin;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_change_activity_daily` AS
SELECT
  report_day,
  airline,
  origin,
  destination,
  route_key,
  domain,
  change_type,
  direction,
  field_name,
  COUNT(*) AS event_count,
  AVG(magnitude) AS avg_magnitude,
  AVG(percent_change) AS avg_percent_change
FROM `__PROJECT_ID__.__DATASET__.fact_change_event`
GROUP BY report_day, airline, origin, destination, route_key, domain, change_type, direction, field_name;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_penalty_reference` AS
SELECT
  cycle_id,
  DATE(captured_at_utc) AS report_day,
  airline,
  origin,
  destination,
  route_key,
  flight_number,
  departure_utc,
  cabin,
  fare_basis,
  penalty_source,
  penalty_currency,
  fare_change_fee_before_24h,
  fare_change_fee_within_24h,
  fare_change_fee_no_show,
  fare_cancel_fee_before_24h,
  fare_cancel_fee_within_24h,
  fare_cancel_fee_no_show,
  fare_changeable,
  fare_refundable,
  penalty_rule_text
FROM `__PROJECT_ID__.__DATASET__.fact_penalty_snapshot`;

CREATE OR REPLACE VIEW `__PROJECT_ID__.__DATASET__.vw_tax_reference` AS
SELECT
  cycle_id,
  DATE(captured_at_utc) AS report_day,
  airline,
  origin,
  destination,
  route_key,
  flight_number,
  departure_utc,
  cabin,
  fare_basis,
  tax_amount,
  currency
FROM `__PROJECT_ID__.__DATASET__.fact_tax_snapshot`;
