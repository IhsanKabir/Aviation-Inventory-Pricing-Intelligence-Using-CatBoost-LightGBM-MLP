-- BigQuery curated analytics tables for Aero Pulse Intelligence Platform
-- Replace __PROJECT_ID__ and __DATASET__ before execution.

CREATE SCHEMA IF NOT EXISTS `__PROJECT_ID__.__DATASET__`;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET__.dim_airline` (
  airline STRING NOT NULL,
  first_seen_at_utc TIMESTAMP,
  last_seen_at_utc TIMESTAMP,
  offer_rows INT64,
  latest_cycle_id STRING
)
CLUSTER BY airline;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET__.dim_route` (
  route_key STRING NOT NULL,
  origin STRING NOT NULL,
  destination STRING NOT NULL,
  first_seen_at_utc TIMESTAMP,
  last_seen_at_utc TIMESTAMP,
  offer_rows INT64,
  airlines_present INT64
)
CLUSTER BY origin, destination;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET__.fact_cycle_run` (
  cycle_id STRING NOT NULL,
  cycle_started_at_utc TIMESTAMP,
  cycle_completed_at_utc TIMESTAMP,
  offer_rows INT64,
  airline_count INT64,
  route_count INT64
)
PARTITION BY DATE(cycle_completed_at_utc)
CLUSTER BY cycle_id;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET__.fact_offer_snapshot` (
  cycle_id STRING NOT NULL,
  captured_at_utc TIMESTAMP NOT NULL,
  airline STRING NOT NULL,
  origin STRING NOT NULL,
  destination STRING NOT NULL,
  route_key STRING NOT NULL,
  flight_number STRING NOT NULL,
  departure_utc TIMESTAMP NOT NULL,
  departure_date DATE NOT NULL,
  cabin STRING,
  brand STRING,
  fare_basis STRING,
  total_price_bdt NUMERIC,
  base_fare_amount NUMERIC,
  tax_amount NUMERIC,
  currency STRING,
  seat_available INT64,
  seat_capacity INT64,
  load_factor_pct NUMERIC,
  booking_class STRING,
  baggage STRING,
  aircraft STRING,
  duration_min INT64,
  stops INT64,
  soldout BOOL,
  penalty_source STRING
)
PARTITION BY DATE(captured_at_utc)
CLUSTER BY airline, origin, destination, departure_date;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET__.fact_change_event` (
  cycle_id STRING,
  previous_cycle_id STRING,
  detected_at_utc TIMESTAMP NOT NULL,
  report_day DATE NOT NULL,
  airline STRING NOT NULL,
  origin STRING,
  destination STRING,
  route_key STRING,
  flight_number STRING,
  departure_day DATE,
  departure_time TIME,
  cabin STRING,
  fare_basis STRING,
  brand STRING,
  domain STRING,
  change_type STRING,
  direction STRING,
  field_name STRING,
  old_value STRING,
  new_value STRING,
  magnitude NUMERIC,
  percent_change NUMERIC,
  event_meta STRING
)
PARTITION BY report_day
CLUSTER BY airline, route_key, domain, field_name;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET__.fact_penalty_snapshot` (
  cycle_id STRING NOT NULL,
  captured_at_utc TIMESTAMP NOT NULL,
  airline STRING NOT NULL,
  origin STRING NOT NULL,
  destination STRING NOT NULL,
  route_key STRING NOT NULL,
  flight_number STRING NOT NULL,
  departure_utc TIMESTAMP NOT NULL,
  cabin STRING,
  fare_basis STRING,
  penalty_source STRING,
  penalty_currency STRING,
  fare_change_fee_before_24h NUMERIC,
  fare_change_fee_within_24h NUMERIC,
  fare_change_fee_no_show NUMERIC,
  fare_cancel_fee_before_24h NUMERIC,
  fare_cancel_fee_within_24h NUMERIC,
  fare_cancel_fee_no_show NUMERIC,
  fare_changeable BOOL,
  fare_refundable BOOL,
  penalty_rule_text STRING
)
PARTITION BY DATE(captured_at_utc)
CLUSTER BY airline, origin, destination;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET__.fact_tax_snapshot` (
  cycle_id STRING NOT NULL,
  captured_at_utc TIMESTAMP NOT NULL,
  airline STRING NOT NULL,
  origin STRING NOT NULL,
  destination STRING NOT NULL,
  route_key STRING NOT NULL,
  flight_number STRING NOT NULL,
  departure_utc TIMESTAMP NOT NULL,
  cabin STRING,
  fare_basis STRING,
  tax_amount NUMERIC,
  currency STRING
)
PARTITION BY DATE(captured_at_utc)
CLUSTER BY airline, origin, destination;
