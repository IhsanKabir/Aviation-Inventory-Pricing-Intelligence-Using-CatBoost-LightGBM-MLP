# Airline Intelligence System Decisions (Thesis Track)

Last updated: 2026-03-20

## 1) Program Vision

Build a multi-airline intelligence platform that progresses through these outcomes:

1. Monitoring
2. Pricing intelligence
3. Revenue prediction
4. Competitor benchmarking
5. Automation (later semi-automated actions)

Target: implement as much as possible in parallel, but execute in phases when needed.

## 2) Confirmed Product Decisions

### Users

- Analysts
- Revenue Management (RM) team
- Internal stakeholders
- Public users who want current and expected price movement

### Airline Expansion Priority

1. Biman (current)
2. Novo Air
3. US Bangla
4. Air Astra
5. Indigo
6. Emirates
7. Qatar
8. Saudia
9. Singapore Airlines
10. Malaysia Airlines
11. Maldivian Air
12. Others later

### Market Scope

- All markets

### Route Scope

- Dynamic route + airline configuration (already in use)
- Bangladesh domestic operational baseline is explicitly intended to include one-way coverage for `BG`, `2A`, `BS`, and `VQ` on the currently configured DAC-linked domestic network in `config/routes.json`.
- At the moment that configured DAC-linked domestic network is:
  - `DAC-BZL`, `BZL-DAC`
  - `DAC-CGP`, `CGP-DAC`
  - `DAC-CXB`, `CXB-DAC`
  - `DAC-JSR`, `JSR-DAC`
  - `DAC-RJH`, `RJH-DAC`
  - `DAC-SPD`, `SPD-DAC`
  - `DAC-ZYL`, `ZYL-DAC`
- The operational expectation for those routes is:
  - `default_one_way_monitoring` stays active as the baseline one-way layer
  - `bangladesh_domestic_round_trip_short` may be layered on top where runtime allows
- Operational rule: a trip profile is only effective when it appears in both `market_trip_profiles` and `active_market_trip_profiles` for the route. `active_market_trip_profiles` filters the candidate list; it does not add missing profiles.
- Investigation on March 22, 2026 found that the Bangladesh domestic routes for `BG`, `2A`, `BS`, and `VQ` had a profile-membership bug: many route entries listed `default_one_way_monitoring` only in `active_market_trip_profiles`. That caused the planner to resolve only the surviving `RT` profile. The config has now been corrected so the intended one-way baseline is eligible again.
- If a Bangladesh domestic route is added in `config/routes.json` for one of those airlines, the matching route entry in `config/route_trip_windows.json` must include the intended one-way profile in both `market_trip_profiles` and `active_market_trip_profiles` so the route does not exist only on paper.

### Collection Frequency

- Use finish-driven launch semantics for operational and daily training: start the next run only after the previous run has finished and the configured completion buffer has elapsed.
- Scheduler launch policy is sequential, not overlapping:
  - do not start a new ingestion cycle while an active/fresh accumulation exists
  - enforce a configurable completion buffer after a completed accumulation before the next launch
  - prefer wrapper-owned self-rescheduling over fixed repeating scheduled-task triggers for operational and daily training lanes
  - fail fast when PostgreSQL is unavailable instead of starting a partial/broken cycle
  - current recommended defaults:
    - operational: `90` minutes
    - training: `120` minutes
    - `ACCUMULATION_COMPLETION_BUFFER_MINUTES` remains a fallback for older hosts
- Collection is now split into two planning modes:
  - `operational`: comparison-safe baseline for web freshness and cycle-to-cycle monitoring
  - `training`: core daily enrichment for forecasting/model refresh
  - `deep`: broad weekly/opportunistic enrichment for the heaviest market-movement patterns
  - training mode may include inventory-anchor departure tracking so the same future departure horizon is observed repeatedly for inventory movement learning
  - training mode is the preferred place to run daily forecasting refreshes (`CatBoost`, `LightGBM`, `MLP`) and publish those outputs to BigQuery for the hosted forecasting surfaces
  - deep mode is where the widest candidate profile set should run:
    - route-level market-prior candidates can expand
    - Bangladesh domestic Eid round-trip plus directional Eid one-way behavior can run together
    - worker outbound / return, regional round-trip, tourism, and hub-spoke / long-haul route behaviors can be layered together
  - operational remains the comparison-safe baseline; training is the core daily enrichment lane; deep is the optional weekly/opportunistic enrichment lane
  - DB-unavailable scheduler outcomes should be recorded as clean skips (`postgres_unreachable`), not treated as successful or partial collection cycles

### Required Data Fields

Mandatory for analysis (minimum set):

- Fare components: fare, tax, total, currency
- Inventory: seats available, sold-out state
- Fare structure: fare basis, booking class (RBD), brand
- Product/cabin: cabin class and fare basis-to-cabin mapping
- Flight/ops: airline, flight number, origin, destination, departure, arrival, aircraft/equipment, duration, stops
- Passenger mix dimensions: ADT/CHD/INF
- Other available fields from source responses should be preserved in raw metadata

### Change Definition

- Any column difference from last valid snapshot is a change event

### Sold-Out Logic

- If all RBDs of a flight/cabin/departure are sold out => flight instance is sold out
- If flight exists but seats are unavailable => treat as sold out (unless API explicitly labels temporary technical unavailability)

### Search Scope

- All possible search combinations over time (routes/cabins/passenger mixes/date windows)
- Round-trip search is supported as search intent layered on top of one-way fact storage
- Bangladesh domestic one-way monitoring should be treated as non-optional baseline coverage for the configured `BG`, `2A`, `BS`, and `VQ` domestic route set, and profile-membership mistakes of this kind should now be blocked by trip-config validation.

### Cabin & Fare Mapping

- Cabin-specific monitoring is required
- Track which fare basis belongs to which cabin over time

### Round-Trip Architecture

- Keep `flight_offers` as the one-way canonical fact table
- Store round-trip request and leg-link metadata in raw meta first
- Expose outbound/inbound pairing through `trip_request_id` plus leg direction/sequence
- Upgrade connectors incrementally instead of forcing an all-at-once migration
- Keep the baseline scheduled cycle one-way-first unless runtime headroom is acceptable; observed March 9, 2026 runtime overrun was already present on `OW` search, so round-trip is not the current root cause of cycle overrun

### Reporting

- Dynamic/on-demand report generation
- Alert types and thresholds must be configurable at runtime

### Forecast Priorities

1. Price-change prediction
2. Availability prediction

### Forecast Horizon

- Flexible horizon (user-defined "next X" time)

### Decision Mode

- Phase 1: human decision support
- Phase 2: semi-automated actions

### Semi-Automated Extraction Strategy (Next Phase)

- Semi-automated/manual-fragment sources should move into a dedicated human-in-the-loop extraction lane after the current ingestion/scheduler/database stack is stable.
- The preferred design is an AI-agent-assisted operator workflow:
  - an operator or AI agent handles the manual/challenge-sensitive step
  - orchestration is handled through automation tooling such as Power Automate, n8n, or a similar workflow engine
  - the workflow should produce structured outputs that feed back into the same canonical normalization and warehouse path as fully automated sources
- This lane is intended for sources where:
  - anti-bot behavior blocks safe full automation
  - a human confirmation step is still required
  - semi-structured/manual UI actions remain cheaper than building brittle scrapers
- "Google AntiGravity" or similar external agentic/browser automation capabilities may be evaluated as an R&D input for this lane, but they are exploratory only and not a current production dependency.
- This work is explicitly phase-gated:
  - do not start before the current automated system is stable
  - stabilize scheduler truth, PostgreSQL health, aggregate reporting truth, and training/deep separation first
  - only then add the semi-automated operator/agent workflow as a separate ingestion class

### Accuracy Evaluation Intent

- Compare prediction for future date/time with actual observed data when that time arrives

### External Enrichment

Include over time:

- Holidays
- Weekday effects
- Country-wise vacation calendars
- Macro/market context and future condition adaptors

### Infrastructure Constraints

- Current environment: local laptop
- Operational database: local PostgreSQL
- Hosted read warehouse: BigQuery sandbox
- Budget: zero (prefer free/open-source stack)

### Timeline

- As early as possible

### Phase-1 Must-Have

- Reports working for all target airlines first
- Then move to prediction layer

## 3) Thesis-Grade Upgrade Decisions (Added)

### Research-Quality Evaluation Pack

Use multiple evaluation families (not just one):

- Directional: up/down accuracy, F1 for rise/fall classes
- Magnitude: MAE, RMSE, MAPE/sMAPE (for fare deltas)
- Event quality: precision/recall for alerts (spikes, sell-out, schedule shock)
- Calibration: reliability plots / Brier score (if probabilistic outputs)
- Operational value: lead-time gain, false-alarm cost, missed-event cost

### Benchmarking Baselines

Always compare models against:

- Naive persistence (next = last)
- Seasonal naive (same weekday/time bucket)
- Moving-average / EWMA baseline

### Reproducibility Standard

- Versioned datasets/snapshots
- Versioned features and model configs
- Backtest windows with fixed train/validation/test splits
- Logged experiment metadata

### Explainability

- Feature importance tracking
- "Why forecast changed" summary per route/flight/cabin
- Store model confidence + uncertainty bands

## 4) Suggested System Architecture (Zero-Budget Compatible)

- Ingestion: airline-specific connectors (modular)
- Standardization: canonical schema + raw payload archive
- Storage:
  - local PostgreSQL for normalized facts/events, comparisons, and ML/DL training
  - Compressed JSON archive for raw payload lineage
  - BigQuery for curated hosted reads, forecasting outputs, and BI
- Processing layers:
  - Snapshot builder
  - Column-level diff engine
  - Event/alert engine
  - Forecast engine (ML + DL)
- Delivery:
  - On-demand report generator (Excel/CSV/JSON)
  - FastAPI + Next.js hosted monitor
  - Looker Studio dashboards over BigQuery

## 5) Data Governance and Risk Note

Use only legally permitted collection. Respect airline terms, robots/policies where
applicable, and avoid methods that violate law or contractual restrictions. Build
throttling, retry policy, and source-specific compliance controls into each connector.

## 6) Immediate Build Sequence

1. Stabilize canonical schema across airlines
2. Implement connector contract for each airline (same output contract)
3. Enable multi-airline capture orchestration + quality checks
4. Make dynamic report pack stable (hourly/daily/on-demand)
5. Add baseline forecasting pipeline (price then availability)
6. Add benchmarking and thesis evaluation framework

## 7) Open Questions (Need Answers)

1. What is the single canonical key for a "flight product": `(airline, flight_no, departure_dt, origin, destination, cabin, fare_basis)` or include brand too?
2. When fare basis is missing but brand exists, should brand become fallback identity?
3. For multi-leg itineraries, do you want segment-level tracking, itinerary-level tracking, or both?
4. Should all timestamps be stored in UTC + local airport timezone offset?
5. Do you want one global currency (e.g., BDT/USD) for all analytics plus original currency retained?
6. What maximum acceptable collection latency per full cycle (all airlines/routes) do you want?
7. For public users, which outputs are exposed: current cheapest fare only, trend chart, or predictions too?
8. What confidence threshold should gate alerts/predictions shown to public users?
9. Which report templates are mandatory for thesis submission (chapter-ready figures/tables)?
10. Do you want a formal backtesting cadence (daily retrain, weekly retrain, monthly retrain)?

## 8) Definition of Done (Phase 1)

Phase 1 is done when:

- Multi-airline collection cycles run reliably on schedule
- All mandatory fields are populated or explicitly null-coded
- Column-level change events are persisted and queryable
- On-demand reports are generated correctly for all onboarded airlines
- Data quality checks and failure logs are in place

## 9) Final Clarifications (2026-02-20)

- Data access is confirmed by project owner as authorized.
- Backtesting method selected: Rolling Window (final).
- Time standard selected: store canonical timestamps in UTC; store local timezone fields for display/ops context.
- Identity key finalized as:
  - airline, day, time, origin, destination, flight number, fare basis, brand, cabin
- Fare basis policy:
  - expected always present; if missing, mark row as invalid/incomplete (fail-safe), do not fallback identity substitution.
- Change policy:
  - any column difference is a change event (no minimum threshold filter).
- Output for public users:
  - show current fare + trend + prediction.
- Model refresh cadence:
  - daily.

## 10) Compliance Statement

All collection and usage must remain within authorized, lawful, and policy-compliant boundaries for each source.

## 11) Completion Plan (Execution Checklist)

Use this section as the single source of truth for "what remains" and "what is done".

### Phase 1 Closure (Must Complete First)

- [x] **P1-A: Final Data Quality Closure**
  - Target:
    - `adt/chd/inf` nulls = 0 for new capture rows
    - `inventory_confidence` nulls = 0 for new capture rows
    - `source_endpoint` nulls = 0 for new capture rows
    - `departure_utc` nulls = 0 for all rows where airport TZ mapping exists
  - Notes:
    - Remaining `arrival_utc` nulls are acceptable only when source arrival local timestamp is missing.
  - Verify:
    - Run data quality report and archive result under `output/reports/`.
  - Evidence (2026-02-21):
    - `output/reports/run_20260221_160815_332731_UTCp0600/data_quality_report_20260221_160815_332731_UTCp0600.csv`
    - `output/reports/run_20260221_160815_339148_UTCp0600/data_quality_report_20260221_160815_339148_UTCp0600.csv`

- [x] **P1-B: Unknown Airport TZ Reduction**
  - Target:
    - Keep `config/airport_timezones.json` aligned with active routes.
  - Action:
    - Monthly query: identify new airport codes with UTC null patterns, update timezone map, run backfill.
    - Repro command:
      - `.\\.venv\\Scripts\\python.exe tools/audit_airport_timezones.py --output-dir output/reports --timestamp-tz local`
  - Verify:
    - Backfill output shows `departure_utc` null trend not increasing for known airports.
  - Evidence (2026-02-21):
    - `config/airport_timezones.json` (`YYZ` added with `-300`)
    - `tools/audit_airport_timezones.py`
    - `output/reports/timezone_coverage_gaps_20260221_234252.csv` (pre-fix: `YYZ` gap)
    - `output/reports/timezone_backfill_verification_clean_20260221_234742.txt` (`null_departure_utc` remains `0`, `YYZ` arrival UTC null resolved to `0`)
    - `output/reports/timezone_coverage_audit_20260221_234622.csv` (post-fix audit)
    - `output/reports/timezone_coverage_gaps_20260221_234622.csv` (post-fix gaps empty)

- [x] **P1-C: Connector Stability Gates**
  - Target:
    - BG + VQ targeted checks succeed for `DAC->CXB` and at least one international BG route.
  - Verify commands:
    - `run_all.py --quick --airline BG --origin DAC --destination CXB --date <date> --cabin Economy`
    - `run_all.py --quick --airline VQ --origin DAC --destination CXB --date <date> --cabin Economy`

- [x] **P1-D: Reporting Reliability**
  - Target:
    - `price_changes_daily`, `availability_changes_daily`, `route_airline_summary`, `data_quality_report` generated every run.
    - `raw_meta_coverage_pct` = 100 for active run scope.
  - Verify:
    - `generate_reports.py` creates all report files without manual fixes.

- [x] **P1-E: Ops Hardening**
  - Target:
    - Scheduler runs every 3-4h with no crash loops.
    - Failures logged with actionable reason.
  - Verify:
    - Review `logs/` for one full day cycle.
  - Close-out status (2026-02-21):
    - `output/reports/ops_health_latest.md` is `PASS` with no non-zero pipeline runs.
    - Window currently captured: `2026-02-21 05:12:08` to `2026-02-21 17:32:23` (~12.34h).
    - Closed per project owner directive; continue scheduler logging to accumulate full-day and multi-day evidence.

### Phase 2 (Thesis/Prediction Enablement)

- [x] **P2-A: Baseline Forecast Pack**
  - Implement and compare:
    - Naive persistence
    - Seasonal naive
    - EWMA baseline
  - Metrics:
    - MAE, RMSE, MAPE/sMAPE, directional F1.
  - Evidence (2026-02-21):
    - `output/reports/prediction_eval_total_change_events_20260221_164627.csv`
    - `output/reports/prediction_eval_price_events_20260221_164625.csv`
    - `output/reports/prediction_eval_availability_events_20260221_164626.csv`

- [x] **P2-B: Backtesting Framework**
  - Rolling-window backtest with fixed splits and saved experiment metadata.
  - Evidence (2026-02-21):
    - `output/reports/prediction_backtest_eval_total_change_events_20260221_165325.csv`
    - `output/reports/prediction_backtest_splits_total_change_events_20260221_165325.csv`
    - `output/reports/prediction_backtest_meta_total_change_events_20260221_165325.json`
    - `output/reports/prediction_backtest_eval_price_events_20260221_165330.csv`
    - `output/reports/prediction_backtest_splits_price_events_20260221_165330.csv`
    - `output/reports/prediction_backtest_meta_price_events_20260221_165330.json`
    - `output/reports/prediction_backtest_eval_availability_events_20260221_165331.csv`
    - `output/reports/prediction_backtest_splits_availability_events_20260221_165331.csv`
    - `output/reports/prediction_backtest_meta_availability_events_20260221_165331.json`

- [x] **P2-C: Alert Quality Evaluation**
  - Precision/Recall for spike/sell-out alerts.
  - False alarm and missed event cost tracking.
  - Evidence (2026-02-21):
    - `output/reports/alert_quality_daily_20260221_173346.csv`
    - `output/reports/alert_quality_overall_20260221_173346.csv`
    - `output/reports/alert_quality_by_route_20260221_173346.csv`
  - Notes:
    - Spike alert metrics are computed against `total_change_events` thresholding with rolling baseline prediction.
    - Sellout alert pipeline is implemented, but current source window has zero
      positive sellout events (`support=0`), so precision/recall are pending
      future positives.

- [x] **P2-D: Thesis-Ready Output Pack**
  - Reproducible figures/tables from report + model outputs.
  - Chapter-ready summary for methodology + results.
  - Evidence (2026-02-21):
    - `tools/build_thesis_pack.py`
    - `output/reports/thesis_pack_20260221_174107/thesis_summary.md`
    - `output/reports/thesis_pack_20260221_174107/tables/table_prediction_best_models.csv`
    - `output/reports/thesis_pack_20260221_174107/tables/table_backtest_test_summary.csv`
    - `output/reports/thesis_pack_20260221_174107/tables/table_alert_quality_overall.csv`
    - `output/reports/thesis_pack_20260221_174107/tables/table_data_quality_snapshot.csv`
    - `output/reports/thesis_pack_20260221_174107/manifest.json`
    - `output/reports/thesis_pack_20260221_174107.zip`

### Weekly Completion Ritual

Every week, run and record:

1. `run_all.py` targeted checks (BG + VQ)
2. `tools/backfill_raw_meta_fields.py`
3. `generate_reports.py --format both`
4. `scheduler/maintenance_tasks.py --task both` (ensures weekly pack + restore validation + smoke snapshots)
5. Update this file:
   - Tick completed items
   - Add blockers under "Open Questions" if any new dependency appears

### Progress Snapshot (2026-02-22)

- `P1-B` unknown airport timezone reduction:
  - Added `tools/audit_airport_timezones.py` for recurring monthly timezone-gap detection.
  - Updated `config/airport_timezones.json` with `YYZ: -300` based on active data audit.
  - Backfill validation confirms `null_departure_utc` remains `0` and `YYZ` arrival UTC nulls are cleared.
- `P1-C` connector stability gates:
  - Verified via one-cycle scheduler runs for BG and VQ (`--once`) with `rc=0`.
- `P1-D` reporting reliability:
  - All core report artifacts generated successfully in current cycle.
  - `route_flight_fare_monitor` is now soft-skipped when no rows exist (no pipeline failure).
- `P1-E` ops hardening:
  - Added `tools/ops_health_check.py`.
  - Latest baseline: `output/reports/ops_health_latest.md` shows `PASS` with no non-zero pipeline runs in analyzed window.
  - Checklist item marked complete per project owner directive; scheduler remains active for additional evidence accumulation.
- Ops automation hardening extensions:
  - Added health notifier: `tools/notify_ops_health.py` (WARN/FAIL alert logic, webhook-capable, local audit log).
  - Added forced alert test controls: `--force-status` and `--test-mode` in `tools/notify_ops_health.py`.
  - Added retention cleanup: `tools/retention_cleanup.py` (default keep windows: logs 30d, reports 60d).
  - Added unified status dashboard: `tools/system_status_snapshot.py` (`system_status_latest.md/json` + timestamped snapshots).
  - Added DB backup automation: `tools/db_backup.py` (writes `.dump` + `db_backup_latest.json`).
  - Added DB restore validation: `tools/db_restore_test.py` (non-destructive `pg_restore --list` / schema render checks).
  - Added smoke gate: `tools/smoke_check.py` (deps, DB connectivity, heartbeat freshness, ops/report artifact freshness).
  - Wired into daily/weekly maintenance flow via `scheduler/maintenance_tasks.py`.
  - Added no-admin always-on fallback daemon: `scheduler/always_on_maintenance.py` and startup/pulse launchers.
  - Added setup reproducibility:
    - `requirements-lock.txt`
    - `setup_env.ps1`
    - `SETUP_QUICKSTART.md`
  - Evidence:
    - `output/reports/ops_notifications.log`
    - `output/reports/retention_cleanup_latest.json`
    - `output/reports/system_status_latest.md`
    - `output/reports/system_status_latest.json`
    - `output/reports/smoke_check_latest.md`
    - `output/reports/smoke_check_latest.json`
    - `output/backups/db_backup_latest.json`
    - `output/backups/db_restore_test_latest.json`
    - `output/reports/ops_health_20260222_002728.md`
    - `output/reports/smoke_check_20260222_002735.md`
    - `output/reports/thesis_pack_20260222_002736.zip`
    - `scheduler/install_always_on_autorun.ps1`
    - `scheduler/always_on_maintenance.py`
  - Current environment note:
    - Backup/restore tools now auto-discover PostgreSQL client binaries from common
      Windows install paths (`C:\\Program Files\\PostgreSQL\\*\\bin`) even when PATH
      is not preconfigured.
- Operational excellence upgrade pack (2026-02-22):
  - CI + commit quality gates:
    - `tools/ci_checks.py` (compile + tests + smoke + report dry run)
    - `.github/workflows/ci.yml`
    - `.githooks/pre-commit`
    - `tools/install_git_hooks.ps1`
  - DB resilience and verification:
    - `tools/db_backup.py` captures table metrics at backup time.
    - `tools/db_restore_test.py` validates dump readability.
    - `tools/db_restore_drill.py` performs full temporary-DB restore and row-count/checksum comparison.
  - SLA + drift + operator visibility:
    - `tools/data_sla_dashboard.py`
    - `tools/model_drift_monitor.py`
    - `tools/build_operator_dashboard.py`
  - Recovery + performance:
    - `tools/recover_missed_windows.py` (dry-run scan and active recovery mode)
    - `run_all.py --profile-runtime --profile-output-dir <dir>`
    - `run_pipeline.py --parallel-airlines <N>` via `tools/parallel_airline_runner.py`
  - Retention tiers:
    - `tools/retention_cleanup.py` now supports raw/aggregate/thesis retention windows.
  - Secrets hardening:
    - Removed embedded DB credentials from code/config defaults.
    - Added env-driven DB resolution helper: `core/runtime_config.py`.
    - Added `.env.example`.
  - Evidence (latest):
    - `output/reports/ci_checks_latest.json`
    - `output/reports/data_sla_latest.md`
    - `output/reports/model_drift_latest.md`
    - `output/reports/recover_missed_windows_latest.json`
    - `output/reports/operator_dashboard_latest.md`
    - `output/backups/db_restore_drill_latest.json`
    - latest parallel cycle manifest JSON under `output/reports/`
    - `output/reports/runtime_profile_latest.json`
- `P2-A` baseline forecasting:
  - Upgraded `predict_next_day.py` with seasonal naive + EWMA baselines.
  - Added RMSE and directional metrics (directional accuracy + up/down/macro F1).
  - Implemented fallback history source from `flight_offers` when route summary view has insufficient history.
- `P2-B` backtesting framework:
  - Added fixed rolling train/validation/test split execution in `predict_next_day.py`.
  - Added saved backtest artifacts: `prediction_backtest_eval_*`, `prediction_backtest_splits_*`, and `prediction_backtest_meta_*.json`.
  - Added auto-window fallback when requested split lengths exceed available history range.
- `P2-C` alert evaluation:
  - Added `tools/evaluate_alert_quality.py`.
  - Added precision/recall/F1/accuracy + false-alarm/missed-event cost outputs (overall and by route).
  - Added `run_pipeline.py --run-alert-eval` integration with configurable thresholds and cost weights.
- `P2-D` thesis-ready output pack:
  - Added `tools/build_thesis_pack.py` to auto-discover latest artifacts and assemble a reproducible thesis bundle.
  - Pack output includes copied raw evidence, consolidated thesis tables, chapter-ready markdown summary, and SHA-256 manifest.
  - Latest evidence:
    - `output/reports/thesis_pack_20260221_174107/`
    - `output/reports/thesis_pack_20260221_174107.zip`
- Dynamic search-horizon prediction/trend enhancement:
  - `run_all.py` now accepts dynamic date windows:
    - `--dates`, `--date-offsets`, `--dates-file`
  - `run_pipeline.py` now forwards dynamic collection date-window args and dynamic
    prediction args (`--prediction-series-mode`, departure bounds, optional
    backtest disable).
  - `predict_next_day.py` now supports `--series-mode search_dynamic` for search-day to search-day forecasting by `departure_day`.
  - Added trend outputs per route/cabin/departure-day:
    - `prediction_trend_<target>_<timestamp>.csv`
  - Example evidence:
    - `output/reports/prediction_next_day_min_price_bdt_20260221_170752.csv`
    - `output/reports/prediction_trend_min_price_bdt_20260221_170752.csv`
    - `output/reports/prediction_backtest_splits_min_price_bdt_20260221_170752.csv`
- Legacy historical data migration:
  - Added `tools/migrate_legacy_history.py` to import legacy archive/sqlite snapshots into current Postgres schema.
  - Apply run imported historical records into current DB:
    - `flight_offers`: +95
    - `flight_offer_raw_meta`: +95
  - Dry-run evidence:
    - `output/reports/legacy_migration_dry_run_20260221_171733.txt`

- Latest inclusions (2026-02-22, v2 integration + validation):
  - Route scope segmentation + market-country domestic logic:
    - Added shared route-scope utility: `engines/route_scope.py`
    - Added airport-country mapping config: `config/airport_countries.json`
    - `run_all.py` now supports:
      - `--route-scope all|domestic|international`
      - `--market-country <ISO2 or country-name>` (e.g., `BD`, `IN`, `Bangladesh`, `India`)
    - `generate_reports.py` and `generate_route_flight_fare_monitor.py` now support the same route-scope filters.
    - Multi-airline filters now accepted as comma-separated values in collection/report flows (e.g., `--airline BG,VQ`).
  - Dynamic date range selection (search horizon):
    - `run_all.py` now supports explicit departure-date range search:
      - `--date-start YYYY-MM-DD --date-end YYYY-MM-DD`
    - `config/dates.json` date config now supports explicit ranges in addition to lists and offsets.
    - `run_pipeline.py` forwards `--date-start/--date-end` and route-scope flags into collection + report steps.
  - Route monitor visual refinement:
    - Route blocks remain boxed top-to-bottom with thicker bottom boundary.
    - Data cells are no longer globally bold; emphasis is now kept primarily on arrows and subscript annotations for cleaner readability.
  - Unified intelligence output layer:
    - Added `tools/build_intelligence_hub.py` (forecast + competitive intelligence + ops status in one pack).
    - Added `run_pipeline.py --run-intelligence-hub` with controls:
      - `--intel-lookback-days`
      - `--intel-forecast-target`
    - Evidence:
      - `output/reports/intelligence_hub_latest.xlsx`
      - `output/reports/intelligence_overview_latest.md`
      - `output/reports/intelligence_competitive_latest.csv`
      - `output/reports/intelligence_route_summary_latest.csv`
  - Prediction ML v2 (pluggable with fallback):
    - `predict_next_day.py` now supports optional ML backends:
      - `--ml-models catboost,lightgbm`
      - `--ml-quantiles 0.1,0.5,0.9`
      - `--ml-min-history`
      - `--ml-random-seed`
    - Baseline models remain default and active fallback when ML libs are missing.
    - `run_pipeline.py` forwards ML options via:
      - `--prediction-ml-models`
      - `--prediction-ml-quantiles`
      - `--prediction-ml-min-history`
      - `--prediction-ml-random-seed`
  - Route report clarification for non-operating flights:
    - In `engines/output_writer.py`, blank cells for non-operating
      flight/date intersections are now rendered as `N/O` (plus `—` in other
      metric cells), avoiding confusion with missing data.
  - CXB-DAC 22-Feb validation note:
    - Validation against latest full cycle pair confirms fares exist on `2026-02-22` for:
      - `VQ-928` and `VQ-936` (min fare observed `4,999`).
    - If `VQ-922` appears blank on `2026-02-22`, it is treated as non-operating for that date (not missing-route data).
  - run_all runtime optimization:
    - Removed per-row DB lookup for `flight_offer_id` during raw-meta linking.
    - Replaced with one bulk ID map load per search block (legacy cycle UUID + airline + route + cabin) and in-memory key matching.
    - Added matched/unmatched diagnostics in logs:
      - `Persisted X core rows + Y raw-meta rows (matched=M unmatched=U)`
    - Added comparison prefetch cache per route+cabin:
      - Preloads latest prior snapshots for all selected dates in one DB query and reuses in-memory map during loop.
      - Excludes current cycle from baseline snapshots to avoid self-comparison drift.
    - Normalized departure identity key between current parser rows and DB snapshots to improve match hit-rate in change comparison.
  - DB storage sustainability upgrades (no-delete / no-new-storage compliant):
    - Added read-only storage monitor: `tools/db_storage_health_check.py`
      - Reports DB size, top tables, disk free space, raw-meta growth runway estimate, and bloat heuristic.
    - Added lossless raw payload fingerprint + dedupe store:
      - New table: `raw_offer_payload_store` (fingerprint-keyed payload storage).
      - `flight_offer_raw_meta` now records `raw_offer_fingerprint` and `raw_offer_storage`.
      - Future duplicate payloads can be externalized while preserving one observation row per snapshot (time-series integrity retained).
    - Added raw-meta compaction tool + scheduler maintenance hook:
      - `tools/db_compact_raw_meta.py`
      - Optional weekly maintenance-window execution via `scheduler/maintenance_tasks.py --enable-db-compact-raw-meta`
    - Compatibility note:
      - `tools/backfill_raw_meta_fields.py` now supports deduped/externalized payload lookup via fingerprint reference.

### Final Project Completion Sign-Off

Mark project complete when all are true:

1. All Phase 1 checklist items are checked, or explicitly marked as closed by project owner directive with date, rationale, and evidence-gap note.
2. Any owner-directed manual closure keeps background evidence accumulation
   active until the original evidence target is met, with latest evidence file
   paths recorded.
3. At least 2 full weeks of stable scheduled execution logs exist for final operational sign-off.
4. Data quality report shows no critical nulls in mandatory fields for active scopes.
5. Baseline forecasting + backtest evidence is generated and archived.
6. Thesis-ready report package is reproducible from repository scripts.

### Storage Sustainability Decision (2026-02-22, No-Delete / No-New-Storage Constraint)

Project-owner constraint (confirmed):

- Raw historical data will not be deleted or archived away from active use
  because it is required for ML/DL training, backtesting, and future decision
  optimization (including "which fare at which time is optimal" analyses).
- Additional paid storage is not currently an option.

Therefore, storage strategy must focus on in-place efficiency and lossless preservation:

1. In-place compaction first (no data loss)
   - Treat table/index bloat reclamation as a required maintenance task (e.g., `VACUUM FULL` during maintenance window or `pg_repack` when available).
   - Priority target: `public.flight_offer_raw_meta` (dominant storage consumer).

2. Lossless raw-payload deduplication (preserve full training fidelity)
   - Store repeated identical raw payloads once (content-hash keyed), and reference them from observation rows.
   - This keeps reconstructability while reducing duplicate storage.

3. Lossless compression of raw payload fields
   - Compress large/volatile raw payload content (e.g., JSON payloads) before persistence or in a dedicated compressed column/table design.
   - Requirement: reversible (lossless) for audit and model reproducibility.

4. Partitioning for manageability (not retention deletion)
   - Partition large fact/raw tables by capture date/time to improve maintenance operations, targeted reindexing, and future scalability.
   - Partitioning is adopted for operational control, not for deleting training history.

5. Ingestion efficiency over unnecessary duplication
   - Reduce duplicate/raw writes caused by repeated unchanged snapshots where
     possible (e.g., snapshot fingerprinting, idempotent raw-write logic), while
     preserving time-series evidence required for forecasting validation.
   - Maintain a stable validation panel of departure dates for longitudinal comparison, and use dynamic windows as additive intelligence coverage.

6. DB observability + capacity forecasting must remain active
   - Track database size, per-table growth, bloat indicators, WAL usage, and disk free space.
   - Add pre-run / daily health checks and threshold alerts before storage becomes a blocking issue.

7. Non-DB cleanup remains allowed
   - Logs/reports/temp artifacts may still use retention cleanup policies, because they do not replace the canonical training history stored in PostgreSQL.

Implementation direction (next upgrades):

- Add DB storage health monitor (size + runway estimate + bloat heuristic).
- Add raw payload fingerprinting / dedupe design.
- Add partitioning plan for `flight_offer_raw_meta` and other high-growth tables.
- Add maintenance runbook step for compaction/reindex windows.

## 12) Enhancement Flexibility Note

The project remains intentionally open to modifications required for future enhancements.

Guiding rule:

- If a change improves data quality, reliability, coverage, research quality,
  or operational usability, it is allowed and should be integrated through
  controlled updates.

Change handling expectation:

1. Document the change intent in this file (or linked implementation note).
2. Apply schema/code/report updates as needed.
3. Re-run regression checks and data-quality validation.
4. Update completion checklist items if scope/timeline shifts.

## 13) Passenger-Mix Search Basis Decision (2026-02-23)

Finding (validated):

- Search results (fares and visible inventory) can change when passenger count
  changes (for example `ADT=1` vs `ADT=2`), including NOVOAIR and potentially
  other carriers.
- Therefore, search output reflects commercial inventory state for the requested party size, not a universal single-seat truth.

Decision:

1. Keep `ADT=1, CHD=0, INF=0` as the baseline time-series for continuity.
   - This preserves comparability with existing historical data and current forecasting/backtest evidence.

2. Treat passenger mix (`ADT/CHD/INF`) as a first-class search dimension.
   - Comparisons must use the same passenger mix basis.
   - Route-monitor comparisons across different passenger mixes are considered non-like-for-like and must be flagged.

3. Support optional probe searches (additive to baseline, not replacement).
   - Recommended probes: `ADT=2` for priority routes; `ADT=4` only for selective benchmark runs.
   - Purpose: detect fare-bucket release/closure behavior and party-size sensitivity.

4. Preserve passenger mix metadata in persisted raw-meta observations.
   - `flight_offer_raw_meta.adt_count/chd_count/inf_count` are stored and used for comparison-basis checks.

5. Reporting and ops visibility must show basis.
   - Runtime heartbeat and watcher include `pax=ADT/CHD/INF`.
   - Workbook methodology note warns to compare only runs with the same passenger mix.

Implementation status (2026-02-23):

- `run_all.py`, `run_pipeline.py`, `modules/biman.py`, `modules/novoair.py`, scheduler wrappers, and parallel runner support `--adt/--chd/--inf`.
- `tools/watch_run_status.py` displays passenger mix from heartbeat.
- `generate_route_flight_fare_monitor.py` warns when compared cycles have mismatched passenger mix basis.

## 14) Delivery Architecture Decision (2026-03-07)

### Operational Application

- Primary interactive product will move to:
  - FastAPI reporting API
  - Next.js frontend
  - BigQuery-backed hosted reads for public/runtime pages
- Local PostgreSQL remains the operational write and training store on the collection machine.

### Analytics and BI

- Historical analytics layer will move to:
  - BigQuery sandbox dataset
  - Looker Studio dashboards
- Curated facts are exported from PostgreSQL to BigQuery after successful `run_pipeline.py` cycles using a rolling recent capture-date window.
- Manual/off-cycle warehouse refresh still remains available through `tools/export_bigquery_stage.py`.
- ML/DL outputs that must live in the warehouse:
  - forecast bundle summaries
  - model evaluation
  - route evaluation
  - route winner tables
  - next-day predictions
  - backtest evaluation
  - backtest route winner tables

### Public Repository Wording

- Public-facing repository/docs should prefer:
  - capture
  - collection
  - accumulation
  - snapshot
  - cycle
- Keep low-level legacy field names local/internal when required for compatibility.

### Optional Components

- `strategy_engine.py` is currently optional and experimental.
- It is not required for:
  - current Excel reporting
  - PostgreSQL reporting API
  - BigQuery export pipeline
- Keep it only as a future signal/intelligence hook unless a downstream consumer is added.

## 15) Web Product Scope Decision (2026-03-09)

Decision:

- The active hosted product scope is now tracked in [docs/WEB_PRODUCT_REQUIREMENTS.md](docs/WEB_PRODUCT_REQUIREMENTS.md).
- That document is the implementation-facing source of truth for:
  - bug-fix scope
  - UI/UX improvements
  - new web features
  - future roadmap expectations

Confirmed prioritization groups:

- Bug fixes
  - active-navigation state must be correct

- UI/UX improvements
  - departure-time-first ordering across airlines
  - scan-friendly Changes page
  - shared date-selection model
  - conditional visibility for inventory-estimation columns

- New features
  - filter-driven Excel export
  - route categorization as `DOM` / `INT`
  - round-trip support
  - Airline Operations page
  - tax monitor upgrade
  - market-level Changes dashboard

- Future roadmap
  - future penalty-model integration
  - forecasting expansion as the main advanced intelligence track

Architectural guidance:

1. Keep Excel as an export artifact, not the primary interactive surface.
2. Keep local PostgreSQL as the collection and training store.
3. Keep hosted reads BigQuery-first through FastAPI.
4. Treat filtering, date semantics, and route identity as shared contracts across pages, exports, and future forecasting surfaces.

Execution note:

- Implementation should follow the priority order defined in [docs/WEB_PRODUCT_REQUIREMENTS.md](docs/WEB_PRODUCT_REQUIREMENTS.md), starting with navigation correctness, comparison ordering, and scan-first market review UX.

## 16) Stability Remediation Roadmap (2026-03-12)

The platform has reached the point where operational reliability is a bigger risk
than raw feature breadth. The next decisions therefore prioritize:

1. one trustworthy operational status source
2. one trustworthy scheduler truth model
3. one stable PostgreSQL runtime on the operational machine
4. one explicit config-resolution model that operators can reason about quickly

### Critical This Week

These items are now the highest-priority stability work:

1. Create one authoritative aggregate cycle status artifact.
   - `run_all_status_latest.json` is not sufficient when parallel workers are active.
   - The system needs a cycle-level aggregate status file derived from the parallel runner output.
   - Health pages, route freshness, and operational decisions must prefer the aggregate artifact over per-worker last-write files.

2. Harden PostgreSQL operations on the main machine.
   - PostgreSQL service uptime is now a hard dependency for ingestion, post-run validation, and local truth.
   - Treat local service health, data-directory permissions, and startup checks as operational prerequisites.
   - The scheduler should not start a cycle unless PostgreSQL connectivity passes first.

3. Prove no-overlap behavior empirically.
   - Guarded wrapper logic is now in place, but the next validation standard is operational:
     - one launch
     - one finish or one skip
     - no duplicate `starting ingestion cycle` sequence without a finish/skip in between

4. Separate business-facing freshness from worker-level activity.
   - User-facing pages must not infer "latest healthy cycle" from partial worker artifacts.
   - Comparable-cycle selection should remain based on complete, aggregate, comparison-eligible cycles only.

### Stability Next

After the critical items above, the next stability layer is:

1. Add a config linter for trip planning.
   - Validate `dates.json`, `market_priors.json`, and `route_trip_windows.json` together.
   - Detect contradictory profile activation, missing profile names, impossible ranges, and unexpectedly large expansion factors.

2. Publish deployment/runtime identity clearly.
   - Expose current API revision, web build SHA, latest warehouse sync timestamp, and latest comparable cycle timestamp.
   - This reduces confusion between stale deploys, stale warehouse state, and true data absence.

3. Make training/deep lanes first-class run products.
   - `operational`, `training`, and `deep` now exist structurally.
   - Their next step is independent status/reporting artifacts so operators can tell which lane is healthy without reading logs.

4. Keep deep enrichment opportunistic.
   - Deep mode is broad by design and should not be treated as a guaranteed fixed-slot run on a single training laptop.
   - Core daily training remains the stable training lane; deep remains opportunistic or event-driven.

### Architecture Later

Longer-term, the preferred architectural direction is:

1. centralize operational PostgreSQL away from a fragile local single-laptop dependency
2. keep one aggregate cycle registry with explicit run kind and comparison eligibility
3. move from implicit schedule slots to finish-plus-buffer launch semantics everywhere
4. keep route and trip config rich, but expose machine-readable guardrails before execution

### Immediate Operating Rule

Until the roadmap above is complete:

- trust aggregate cycle outputs over worker-local status files
- treat PostgreSQL service health as a blocker, not a warning
- let guarded wrappers skip rather than force launches
- prefer fewer clean cycles over more ambiguous cycles

## Copilot Coding Agent Push Workflow (2026-03-20)

GitHub Copilot can push changes only when the repository is enabled for Copilot
coding agent and the acting user has write access. The recommended workflow is:

1. Keep `main` protected.
2. Let Copilot work on a feature branch.
3. Open a pull request from that branch.
4. Review and merge the PR after checks pass.

Setup checklist:

- enable Copilot coding agent at the org or enterprise level if it is controlled
  by policy
- make sure this repository is not opted out of Copilot coding agent
- grant the user or bot account `write` access to the repository
- keep branch protection rules on `main`
- if your ruleset blocks Copilot, add a ruleset bypass for the Copilot coding
  agent or use the standard branch/PR path instead of direct pushes

How to use it:

- from GitHub.com, open the Agents tab, dashboard task box, or an issue and ask
  Copilot to create a pull request
- optionally choose a base branch; Copilot creates a new branch from that base
  and pushes its work to a draft PR
- Copilot works best on branches whose names begin with `copilot/`
- Copilot does not push directly to `main` or `master`

Local IDE note:

- if you use Copilot in VS Code or another IDE, Copilot edits the files locally;
  your normal Git credentials still perform the final push to GitHub

PR note:

- if the PR triggers GitHub Actions, a user with write access must approve the
  workflow run before it executes

Firewall note:

- if Copilot reports `forceExit` and blocked addresses, the agent's firewall is
  preventing a network call inside the GitHub Actions appliance
- fix it by adding the needed host or URL in repository settings under
  `Settings` -> `Copilot` -> `coding agent` -> `Custom allowlist`
- if the blocked network call is only needed during setup, move it into
  `copilot-setup-steps.yml`, because the agent firewall does not apply to setup
  steps
- if you are using self-hosted runners, disable the integrated firewall and
  allow the standard GitHub Actions hosts plus the Copilot-required hosts
  documented by GitHub

Branch protection note:

- this error usually means a repo rule or branch protection rule blocked the
  push target
- check repository `Settings` -> `Rules` -> `Rulesets` and `Settings` ->
  `Branches` for any rule that applies to the branch Copilot is trying to push
- if the rule covers `copilot/*` branches, either exempt the Copilot coding
  agent or loosen the rule so Copilot can push the feature branch and open the
  PR
- if the rule only protects `main`, that is fine; Copilot should push to a
  feature branch and merge through the PR instead of pushing to `main`

## Trip Config Validation

Trip configuration now has a dedicated validator:
- `tools/validate_trip_config.py`

Operational rule:
- validate `config/route_trip_windows.json`, `config/market_priors.json`, and `config/routes.json` before relying on new scheduler-facing trip-profile changes

## Manual Operational Mode (2026-03-15)

For laptops that cannot stay on continuously, operational collection should run
manually instead of through the finish-driven scheduler.

Decision:

1. keep the guarded operational wrapper and buffer logic
2. disable `AirlineIntel_Ingestion4H` and `AirlineIntel_IngestionOnLogon` on
   intermittently powered laptops
3. launch operational manually with:
   - `cmd /c scheduler\run_ingestion_4h_once.bat`

Reason:

- a power-off destroys in-flight Python workers and active search requests
- the current system can skip/recover cleanly, but it does not yet support true
  query-level checkpoint resume after shutdown
- manual operational launches are more reliable than forced scheduler runs on a
  laptop that is regularly turned off

Current observed route issue:

- domestic routes such as `DAC-CXB`, `DAC-JSR`, and `DAC-SPD` are present in the
  operational trip config as one-way monitored routes
- if they do not appear on the website, that is currently because the latest
  completed offer snapshot has no collected rows for those route/date queries,
  not because the operational OW config omitted them
- website visibility also remains gated by downstream warehouse publish time

## Resume Recovery Follow-Up (2026-04-07)

Checkpoint-resume work has now been added in code, but it still needs a real
shutdown test before it can be treated as fully validated production behavior.

Current implementation state:

- `run_all.py` now writes per-airline per-cycle checkpoint files under
  `output/reports/`
- completed query scopes are remembered and should be skipped on restart for the
  same `cycle_id`
- `run_pipeline.py` now accepts and forwards `--cycle-id`
- `tools/recover_interrupted_accumulation.py` now reuses the prior `cycle_id`
  when restarting a stale/interrupted run

Still pending:

- perform one controlled interruption test:
  - start a real operational cycle
  - stop it mid-run
  - relaunch through the guarded wrapper
  - verify that completed airline queries are skipped and the cycle continues
    from checkpointed progress instead of restarting from zero

Until that validation is completed, treat resume-after-shutdown as:

- improved and likely usable
- but not yet formally verified end-to-end
