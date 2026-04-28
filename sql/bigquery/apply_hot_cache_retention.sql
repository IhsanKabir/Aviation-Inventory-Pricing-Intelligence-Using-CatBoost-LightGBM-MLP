-- Cost-safe BigQuery retention for Aero Pulse hot-cache dataset.
-- BigQuery is a bounded website/read cache; PostgreSQL and local Parquet/backups keep history.

ALTER SCHEMA `aeropulseintelligence.aviation_intel`
SET OPTIONS (
  default_partition_expiration_days = 35,
  max_time_travel_hours = 48
);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_cycle_run`
SET OPTIONS (partition_expiration_days = 35);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_offer_snapshot`
SET OPTIONS (partition_expiration_days = 35);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_change_event`
SET OPTIONS (partition_expiration_days = 35);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_penalty_snapshot`
SET OPTIONS (partition_expiration_days = 35);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_tax_snapshot`
SET OPTIONS (partition_expiration_days = 35);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_forecast_bundle`
SET OPTIONS (partition_expiration_days = 90);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_forecast_model_eval`
SET OPTIONS (partition_expiration_days = 90);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_forecast_route_eval`
SET OPTIONS (partition_expiration_days = 90);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_forecast_next_day`
SET OPTIONS (partition_expiration_days = 90);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_backtest_eval`
SET OPTIONS (partition_expiration_days = 90);

ALTER TABLE `aeropulseintelligence.aviation_intel.fact_backtest_split`
SET OPTIONS (partition_expiration_days = 90);
