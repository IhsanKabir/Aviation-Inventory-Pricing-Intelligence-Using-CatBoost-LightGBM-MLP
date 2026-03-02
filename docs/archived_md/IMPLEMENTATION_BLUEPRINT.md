# Implementation Blueprint (Phase Plan)

Last updated: 2026-02-20

## 1) Objective

Deliver a thesis-grade multi-airline intelligence system that supports:

1. Monitoring
2. Pricing intelligence
3. Revenue/availability prediction
4. Competitor benchmarking
5. Later semi-automation

## 2) Core Architecture

- Connector Layer: airline-specific fetch modules with one strict output contract.
- Normalization Layer: map raw responses to canonical schema.
- Storage Layer:
  - PostgreSQL canonical tables (facts, snapshots, change events, predictions)
  - Raw payload archive for lineage/reparse
- Analytics Layer:
  - snapshot differ
  - trend/statistics engine
  - forecasting engine
- Delivery Layer:
  - dynamic on-demand reports (Excel/CSV/JSON)
  - public-facing summary endpoints/views (later)

## 3) Canonical Identity and Granularity

Primary product key:

- airline
- day
- time
- origin
- destination
- flight_number
- fare_basis
- brand
- cabin

Tracking levels:

- Segment level (per segment record)
- Itinerary level (joined/aggregated itinerary record)

## 4) Data Model (Minimum)

### flight_offers_core

- scrape_id
- scraped_at_utc
- airline
- origin
- destination
- departure_utc
- departure_local
- arrival_utc
- arrival_local
- flight_number
- cabin
- fare_basis
- brand
- currency
- total_amount
- fare_amount
- tax_amount
- seat_available
- seat_capacity
- aircraft
- equipment_code
- booking_class
- baggage
- itinerary_id
- segment_index
- is_itinerary_level
- is_segment_level
- identity_valid
- invalid_reason

### raw_offer_meta

- flight_offer_id
- raw_payload
- raw_offer
- source_status
- extraction_notes

### change_events

- event_id
- scrape_id
- previous_scrape_id
- identity_key_hash
- column_name
- old_value
- new_value
- change_type
- changed_at_utc

### prediction_runs

- run_id
- model_name
- model_version
- trained_at_utc
- train_window_start
- train_window_end

### predictions

- prediction_id
- run_id
- identity_key_hash
- target_type (price_delta, soldout_prob, seat_delta)
- horizon_hours
- predicted_value
- predicted_class
- confidence
- created_at_utc

### prediction_actuals

- prediction_id
- observed_value
- observed_class
- observed_at_utc
- error_metrics_blob

## 5) Connector Contract (All Airlines)

Each `modules/<airline>.py` returns:

- ok: bool
- raw: dict
- originalResponse: any
- rows: list[dict]

Required row fields:

- airline, origin, destination, departure, arrival
- flight_number, cabin, fare_basis, brand
- fare_amount, tax_amount, total_amount, currency
- seat_available or seats_remaining
- booking_class, baggage, aircraft/equipment
- raw_offer

If identity fields are missing, row is kept in raw storage but marked `identity_valid = false` and excluded from strict change-detection key matching.

## 6) Change Detection Rules

- Compare current snapshot to previous snapshot by finalized identity.
- Emit a change event for every changed column.
- No threshold suppression in core event log.
- Alert layer may apply optional filters later, but event store remains full-fidelity.

## 7) Sold-Out and Availability Rules

- Flight exists + no seats available => sold out.
- All RBDs sold out within same flight/day/time/cabin scope => sold out at product scope.
- Keep explicit status flags:
  - available
  - sold_out
  - unavailable_unknown

## 8) Scheduling Strategy

- Target frequency: every 3-4 hours.
- Adaptive scheduler:
  - if full cycle runtime > target interval, automatically stretch interval and log degradation.
- Track per-airline runtime to optimize route batching.

## 9) Reporting Pack (Dynamic)

### Operational (on demand)

- current cheapest fare by route/airline/cabin
- latest seat pressure
- fresh change events since last scrape

### Analyst report

- route-wise min/max fare movement
- fare basis migration and cabin mapping drift
- sold-out progression by departure window

### RM/Stakeholder report

- route regime, volatility, benchmark vs competitor
- top opportunities/risk flags

### Public report

- current fare
- trend summary
- prediction summary with confidence

## 10) Forecasting Plan (Daily, Rolling Window)

Primary targets:

1. Price movement
2. Availability movement

Daily training cadence with rolling window:

- Train on last W days/scrapes
- Predict next horizon(s)
- Roll forward each day
- Store forecasts and later match with observed actuals

Suggested horizons:

- 6h, 12h, 24h, 48h

Baseline models (must-have for thesis):

- persistence
- seasonal naive (same weekday/time bucket)
- moving average / EWMA

Advanced models (phase 2):

- Gradient boosting / LightGBM
- sequence model for seat depletion risk

## 11) Thesis Evaluation Framework

Use all metric families:

- Directional: accuracy, precision, recall, F1
- Magnitude: MAE, RMSE, sMAPE
- Probabilistic: Brier score/calibration (if probability outputs)
- Operational: lead-time gain, false-alert cost, missed-event cost

Backtesting protocol:

- Rolling-window backtest (selected)
- fixed reproducible splits per experiment
- versioned experiment tracking

## 12) Zero-Budget Implementation Stack

- Python + PostgreSQL (existing)
- pandas + SQLAlchemy
- open-source ML stack (scikit-learn / lightgbm optional)
- local scheduled jobs
- file-based artifact registry in project folders

## 13) Build Roadmap

### Phase 1 (Now)

- finalize canonical schema and event model
- harden run orchestration and quality checks
- onboard next airlines (NovoAir, US-Bangla, Air Astra, ...)
- stabilize dynamic reporting

### Phase 2

- daily forecasting (rolling window)
- prediction vs actual scoring pipeline
- benchmark dashboards

### Phase 3

- semi-automation policies (human-in-loop)
- public consumption layer hardening

## 14) Immediate Technical Tasks

1. Refactor `run_all.py` to write both segment and itinerary level records.
2. Add identity validation + invalid row audit table.
3. Add column-level diff writer (not only domain-level summary).
4. Add scheduled prediction run job and `prediction_actuals` matcher.
5. Add report parameterization API/CLI for fully dynamic generation.

