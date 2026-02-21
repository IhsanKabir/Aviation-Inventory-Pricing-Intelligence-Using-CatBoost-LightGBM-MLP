# Airline Intelligence System Decisions (Thesis Track)

Last updated: 2026-02-22

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

### Scrape Frequency

- Target every 3-4 hours (adaptive by actual runtime/load)

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

### Cabin & Fare Mapping

- Cabin-specific monitoring is required
- Track which fare basis belongs to which cabin over time

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
- Database: PostgreSQL
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
  - PostgreSQL for normalized facts/events
  - Compressed JSON archive for raw payload lineage
- Processing layers:
  - Snapshot builder
  - Column-level diff engine
  - Event/alert engine
  - Forecast engine
- Delivery:
  - On-demand report generator (Excel/CSV/JSON)
  - Optional lightweight API/dashboard later

## 5) Data Governance and Risk Note

Use only legally permitted collection. Respect airline terms, robots/policies where applicable, and avoid methods that violate law or contractual restrictions. Build throttling, retry policy, and source-specific compliance controls into each connector.

## 6) Immediate Build Sequence

1. Stabilize canonical schema across airlines
2. Implement connector contract for each airline (same output contract)
3. Enable multi-airline scrape orchestration + quality checks
4. Make dynamic report pack stable (hourly/daily/on-demand)
5. Add baseline forecasting pipeline (price then availability)
6. Add benchmarking and thesis evaluation framework

## 7) Open Questions (Need Answers)

1. What is the single canonical key for a "flight product": `(airline, flight_no, departure_dt, origin, destination, cabin, fare_basis)` or include brand too?
2. When fare basis is missing but brand exists, should brand become fallback identity?
3. For multi-leg itineraries, do you want segment-level tracking, itinerary-level tracking, or both?
4. Should all timestamps be stored in UTC + local airport timezone offset?
5. Do you want one global currency (e.g., BDT/USD) for all analytics plus original currency retained?
6. What maximum acceptable scrape latency per full cycle (all airlines/routes) do you want?
7. For public users, which outputs are exposed: current cheapest fare only, trend chart, or predictions too?
8. What confidence threshold should gate alerts/predictions shown to public users?
9. Which report templates are mandatory for thesis submission (chapter-ready figures/tables)?
10. Do you want a formal backtesting cadence (daily retrain, weekly retrain, monthly retrain)?

## 8) Definition of Done (Phase 1)

Phase 1 is done when:

- Multi-airline scrapes run reliably on schedule
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
    - `adt/chd/inf` nulls = 0 for new scrapes
    - `inventory_confidence` nulls = 0 for new scrapes
    - `source_endpoint` nulls = 0 for new scrapes
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
    - Sellout alert pipeline is implemented, but current source window has zero positive sellout events (`support=0`), so precision/recall are pending future positives.

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
    - Backup/restore tools now auto-discover PostgreSQL client binaries from common Windows install paths (`C:\\Program Files\\PostgreSQL\\*\\bin`) even when PATH is not preconfigured.
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
    - `output/reports/scrape_parallel_latest.json`
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
  - `run_pipeline.py` now forwards dynamic scrape date-window args and dynamic prediction args (`--prediction-series-mode`, departure bounds, optional backtest disable).
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

### Final Project Completion Sign-Off

Mark project complete when all are true:

1. All Phase 1 checklist items are checked, or explicitly marked as closed by project owner directive with date, rationale, and evidence-gap note.
2. Any owner-directed manual closure keeps background evidence accumulation active until the original evidence target is met, with latest evidence file paths recorded.
3. At least 2 full weeks of stable scheduled execution logs exist for final operational sign-off.
4. Data quality report shows no critical nulls in mandatory fields for active scopes.
5. Baseline forecasting + backtest evidence is generated and archived.
6. Thesis-ready report package is reproducible from repository scripts.

## 12) Enhancement Flexibility Note

The project remains intentionally open to modifications required for future enhancements.

Guiding rule:

- If a change improves data quality, reliability, coverage, research quality, or operational usability, it is allowed and should be integrated through controlled updates.

Change handling expectation:

1. Document the change intent in this file (or linked implementation note).
2. Apply schema/code/report updates as needed.
3. Re-run regression checks and data-quality validation.
4. Update completion checklist items if scope/timeline shifts.

