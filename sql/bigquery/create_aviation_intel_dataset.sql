-- Concrete BigQuery dataset bootstrap for Aero Pulse Intelligence.
-- Project: aeropulseintelligence
-- Dataset: aviation_intel

CREATE SCHEMA IF NOT EXISTS `aeropulseintelligence.aviation_intel`
OPTIONS (
  description = 'Bounded aviation intelligence hot cache for hosted reads, BI, and current operational reporting',
  default_partition_expiration_days = 35,
  max_time_travel_hours = 48
);
