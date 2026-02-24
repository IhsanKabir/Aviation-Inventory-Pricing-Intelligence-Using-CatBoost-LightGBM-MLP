-- Reporting views built on top of airline_intel.column_change_events
-- Run with:
--   psql -d Playwright_API_Calling -f sql/create_reporting_views.sql

CREATE SCHEMA IF NOT EXISTS airline_intel;

CREATE OR REPLACE VIEW airline_intel.vw_price_changes_daily AS
SELECT
    detected_at::date AS report_day,
    airline,
    origin,
    destination,
    cabin,
    COUNT(*) AS price_change_events,
    COUNT(*) FILTER (WHERE direction = 'up') AS price_increase_events,
    COUNT(*) FILTER (WHERE direction = 'down') AS price_decrease_events,
    COUNT(*) FILTER (WHERE change_type = 'added') AS added_events,
    COUNT(*) FILTER (WHERE change_type = 'removed') AS removed_events,
    ROUND(AVG(ABS(magnitude))::numeric, 4) AS avg_abs_magnitude,
    ROUND(AVG(ABS(percent_change))::numeric, 4) AS avg_abs_percent_change,
    ROUND(MAX(ABS(percent_change))::numeric, 4) AS max_abs_percent_change,
    MIN(detected_at) AS first_event_at,
    MAX(detected_at) AS last_event_at
FROM airline_intel.column_change_events
WHERE domain = 'price'
GROUP BY
    detected_at::date,
    airline,
    origin,
    destination,
    cabin;

CREATE OR REPLACE VIEW airline_intel.vw_availability_changes_daily AS
SELECT
    detected_at::date AS report_day,
    airline,
    origin,
    destination,
    cabin,
    COUNT(*) AS availability_change_events,
    COUNT(*) FILTER (WHERE direction = 'up') AS availability_up_events,
    COUNT(*) FILTER (WHERE direction = 'down') AS availability_down_events,
    COUNT(*) FILTER (WHERE field_name = 'soldout') AS soldout_flag_changes,
    COUNT(*) FILTER (WHERE field_name = '__row_presence__' AND change_type = 'added') AS row_added_events,
    COUNT(*) FILTER (WHERE field_name = '__row_presence__' AND change_type = 'removed') AS row_removed_events,
    ROUND(AVG(ABS(magnitude))::numeric, 4) AS avg_abs_magnitude,
    MIN(detected_at) AS first_event_at,
    MAX(detected_at) AS last_event_at
FROM airline_intel.column_change_events
WHERE domain = 'availability'
GROUP BY
    detected_at::date,
    airline,
    origin,
    destination,
    cabin;

CREATE OR REPLACE VIEW airline_intel.vw_route_airline_summary AS
SELECT
    detected_at::date AS report_day,
    airline,
    origin,
    destination,
    cabin,
    COUNT(*) AS total_change_events,
    COUNT(DISTINCT flight_number) AS flights_affected,
    COUNT(DISTINCT fare_basis) AS fare_bases_affected,
    COUNT(DISTINCT field_name) AS unique_fields_changed,
    COUNT(*) FILTER (WHERE domain = 'price') AS price_events,
    COUNT(*) FILTER (WHERE domain = 'availability') AS availability_events,
    COUNT(*) FILTER (WHERE domain = 'metadata') AS metadata_events,
    COUNT(*) FILTER (WHERE change_type = 'added') AS added_events,
    COUNT(*) FILTER (WHERE change_type = 'removed') AS removed_events,
    COUNT(*) FILTER (WHERE change_type = 'changed') AS changed_events,
    MIN(detected_at) AS first_event_at,
    MAX(detected_at) AS last_event_at
FROM airline_intel.column_change_events
GROUP BY
    detected_at::date,
    airline,
    origin,
    destination,
    cabin;
