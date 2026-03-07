-- Looker-ready BigQuery views for project aeropulseintelligence and dataset aviation_intel.

CREATE SCHEMA IF NOT EXISTS `aeropulseintelligence.aviation_intel`;

CREATE OR REPLACE VIEW `aeropulseintelligence.aviation_intel.vw_cycle_health` AS
SELECT
  cycle_id,
  cycle_started_at_utc,
  cycle_completed_at_utc,
  offer_rows,
  airline_count,
  route_count,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), cycle_completed_at_utc, MINUTE) AS cycle_age_minutes
FROM `aeropulseintelligence.aviation_intel.fact_cycle_run`;

CREATE OR REPLACE VIEW `aeropulseintelligence.aviation_intel.vw_route_daily_fare` AS
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
FROM `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
GROUP BY report_day, airline, origin, destination, route_key, cabin;

CREATE OR REPLACE VIEW `aeropulseintelligence.aviation_intel.vw_change_activity_daily` AS
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
FROM `aeropulseintelligence.aviation_intel.fact_change_event`
GROUP BY report_day, airline, origin, destination, route_key, domain, change_type, direction, field_name;

CREATE OR REPLACE VIEW `aeropulseintelligence.aviation_intel.vw_penalty_reference` AS
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
FROM `aeropulseintelligence.aviation_intel.fact_penalty_snapshot`;

CREATE OR REPLACE VIEW `aeropulseintelligence.aviation_intel.vw_tax_reference` AS
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
FROM `aeropulseintelligence.aviation_intel.fact_tax_snapshot`;
