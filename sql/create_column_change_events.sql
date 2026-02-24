-- Separate PostgreSQL store for all detailed field-level changes
-- Run manually in psql: \i sql/create_column_change_events.sql

CREATE SCHEMA IF NOT EXISTS airline_intel;

CREATE TABLE IF NOT EXISTS airline_intel.column_change_events (
    id BIGSERIAL PRIMARY KEY,

    scrape_id UUID,
    previous_scrape_id UUID,

    airline TEXT NOT NULL,
    departure_day DATE,
    departure_time TIME,
    origin TEXT,
    destination TEXT,
    flight_number TEXT,
    fare_basis TEXT,
    brand TEXT,
    cabin TEXT,

    domain TEXT NOT NULL,
    change_type TEXT NOT NULL,
    direction TEXT,

    field_name TEXT NOT NULL,
    old_value JSONB,
    new_value JSONB,

    magnitude NUMERIC,
    percent_change NUMERIC,

    event_meta JSONB,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cce_detected_at
    ON airline_intel.column_change_events (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_cce_identity
    ON airline_intel.column_change_events (
        airline,
        departure_day,
        departure_time,
        origin,
        destination,
        flight_number,
        fare_basis,
        brand,
        cabin
    );

CREATE INDEX IF NOT EXISTS idx_cce_field
    ON airline_intel.column_change_events (field_name, detected_at DESC);
