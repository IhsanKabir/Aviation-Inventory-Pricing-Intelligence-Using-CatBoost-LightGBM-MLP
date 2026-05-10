# Project Context - Aero Pulse Intelligence Platform

Last updated: 2026-04-27

This is the single handoff file for future chats and future work. Read this first, then use the linked source docs when deeper detail is needed. Keep this file updated whenever architecture, behavior, commands, deployment, data contracts, or product decisions change.

## Living Update Rules

- Add every meaningful project decision to **Decision History** with the date and files affected.
- Add implementation changes to **Change Log** after edits land, especially when they affect pipeline behavior, schemas, API contracts, frontend pages, scheduler behavior, or operational commands.
- Keep this file concise but current; link to detailed docs instead of duplicating whole runbooks.
- When a new Markdown file becomes authoritative, add it to **Markdown Documentation Map**.
- If a decision conflicts with older docs, this file and `PROJECT_DECISIONS.md` should be updated together.

## One-Sentence Summary

Aero Pulse is a Python + PostgreSQL airline fare/inventory intelligence pipeline with reporting, forecasting, bounded BigQuery hot-cache export, FastAPI reporting endpoints, and a Next.js operational web monitor.

Core flow:

```text
Airline/OTA connectors -> normalized PostgreSQL cycle snapshots -> comparisons/change events -> Excel reports + ML/DL forecasting -> BigQuery hot cache -> FastAPI -> Next.js + Looker Studio
```

## Current Repo State

- Worktree reviewed clean on 2026-04-26 before creating this file.
- Cleanup/verification pass completed on 2026-04-27; see **Current Known Verification Status**.
- Main runtime language: Python.
- Web app: Next.js 15, React 19, TypeScript, Vercel-ready.
- Node engine warnings during local web installs are expected if the workstation Node differs from the deployed/Vercel runtime; `apps/web/package.json` currently pins the intended engine and verification should rely on lint/build results.
- API: FastAPI, BigQuery-first hosted reads with PostgreSQL fallback/transitional endpoints.
- Operational data store: local PostgreSQL.
- Hosted analytics/read cache: BigQuery sandbox/curated hot-cache tables with bounded retention.
- BI layer: Looker Studio.
- Excel remains an export/delivery artifact, not the intended primary interactive UI.

## Current Known Verification Status

As of the 2026-04-27 cleanup pass:

- Python compile passed for the tracked source surface.
- `pytest -q` passed: 122 tests.
- `tools/validate_trip_config.py` passed with 0 warnings and 0 errors.
- `tools/ci_checks.py --allow-db-skip --reports-dir output\reports --timestamp-tz local` passed with `ci_ok=True`.
- `alembic heads` and `alembic current` both report `ce8cc3cd3452 (head)`.
- `npm run lint` and `npm run build` pass in `apps/web`; production build no longer logs `ECONNREFUSED 127.0.0.1:8000` when API env vars are absent.
- `tools/db_backup.py`, `tools/db_restore_test.py --mode toc`, and `tools/smoke_check.py` passed locally; restore TOC validation saw 90 entries.
- BigQuery cost-control implementation passed `pytest -q` with 130 tests, full compileall, retention DDL dry-run, no-load partition-refresh export staging, web lint, and web build.
- Live BigQuery follow-up on 2026-04-27 succeeded: enabled `INFORMATION_SCHEMA.TABLE_STORAGE` for `region-asia-south1`, applied 35-day retention to core scraper fact tables and 90-day retention to partitioned forecast/backtest tables, excluded tiny GDS facts and unpartitioned winner tables, and confirmed `tools/bigquery_storage_audit.py` reports `partition_retention_warnings=0`.
- Data extraction reliability focused checks passed on 2026-04-27: compileall over `core`, `models`, `modules`, `tools`, `run_all.py`, `run_pipeline.py`, and `db.py`; focused extraction/preflight/gate tests passed with 7 tests; `tools\pre_flight_session_check.py --dry-run` returned `status=PASS` for 22 enabled airlines.
- ShareTrip kill switch was verified on 2026-04-27: with `SHARETRIP_ENABLED=false`, `run_all.load_airlines()` resolves 9 active non-ShareTrip airlines (`2A,6E,AK,BG,BS,G9,OV,Q2,VQ`), strips ShareTrip fallbacks, and preflight returns `status=PASS` for those 9.
- Backup/report artifacts under `output/` remain generated runtime output and should stay uncommitted.

## Main Entry Points

- `run_pipeline.py`: main orchestrator for accumulation, reports, prediction, intelligence hub, alert evaluation, and BigQuery sync.
- `run_all.py`: main airline/OTA collection runner that normalizes offers and writes PostgreSQL cycle snapshots.
- `generate_reports.py`: standard reporting pack generator.
- `generate_route_flight_fare_monitor.py`: specialized route-flight fare monitor workbook generator, including optional macro-enabled `.xlsm`.
- `predict_next_day.py`: forecasting and backtest pipeline for event and numeric targets.
- `apps/api/app/main.py`: FastAPI app and reporting/auth/access-request endpoints.
- `apps/web/app/`: Next.js App Router pages for the hosted monitor.

## Architecture Snapshot

### Collection

- Airline/channel configuration lives in `config/airlines.json`.
- Route universe lives in `config/routes.json`.
- Date defaults live in `config/dates.json`.
- Route-specific trip activation lives in `config/route_trip_windows.json`.
- Market/holiday templates live in `config/market_priors.json` and `config/holiday_calendar.json`.
- Collection supports `operational`, `training`, and `deep` trip-plan modes.
- A shared `cycle_id`/`scrape_id` groups parallel airline runs into one comparable snapshot.
- Extraction quality is gated separately from process exit. `run_all.py` records per-query `extraction_attempts` and writes `output/reports/extraction_health_latest.{json,md,csv}` with `PASS`, `WARN`, or `FAIL`.
- `run_pipeline.py` runs `tools/pre_flight_session_check.py --dry-run` before extraction unless `--skip-preflight` is used; `--preflight-strict` makes blocking preflight findings fatal.
- `throttle_per_minute` in `config/airlines.json` is honored at query launch time. ShareTrip-backed scheduled concurrency defaults to `PARALLEL_SHARETRIP_MAX_WORKERS=1`.
- Capture/manual sources reject stale captures by default through `MAX_CAPTURE_AGE_HOURS=8`; per-source overrides exist for Air Arabia, SalamAir, Maldivian, GoZayaan, and AirAsia.

### Normalization and Storage

- Core offer facts are stored in `flight_offers` via `models/flight_offer.py`.
- Extended raw/search metadata is stored in `flight_offer_raw_meta` via `models/flight_offer_raw_meta.py`.
- Change events are stored in `change_events` via `models/change_event.py`.
- Raw payload storage and raw meta helpers preserve source detail for later audit/feature work.
- Extraction telemetry is stored in `extraction_attempts` via `models/extraction_attempt.py`; each attempt captures source family, final source, fallback use, row/insert counts, raw-meta match stats, elapsed time, error class, no-row reason, manual/retry flags, and capture/session state.
- `db.py` owns local engine/session setup, additive schema safeguards, bulk inserts, raw metadata normalization, and via-airport inference.

### Change Detection and Reporting

- Current-vs-previous cycle comparison is the operational comparison unit.
- "Any column difference from last valid snapshot is a change event" is the standing decision.
- Excel output remains important for delivery and exports.
- The route monitor workbook is operationally important but should not become the long-term interactive UI.

### Forecasting

- Prediction priorities are price-change prediction first, availability prediction second.
- `predict_next_day.py` supports event and numeric targets.
- ML/DL support includes optional CatBoost, LightGBM, and MLP pathways.
- Current improvements include holiday features, booking curve features, route characteristics, confidence bands, SHAP/explainability output, imputation, transfer learning, and prediction monitoring modules.
- Evaluation intent includes directional quality, magnitude error, event precision/recall, calibration, and operational value.

### Warehouse and Hosted Reads

- Local PostgreSQL remains the operational/training store.
- BigQuery is the hosted website/read hot cache, not the long-term source of truth.
- Curated warehouse tables include airline/route dimensions, cycle runs, offer snapshots, change events, penalties, taxes, forecast bundles, forecast evals, route winners, next-day forecasts, and backtest outputs.
- BigQuery export/loading is handled through `tools/export_bigquery_stage.py`, `tools/load_bigquery_latest.ps1`, and SQL under `sql/bigquery/`.
- Default BigQuery policy: explicit opt-in sync with `BIGQUERY_SYNC_ENABLED=1`, `BIGQUERY_SYNC_LOOKBACK_DAYS=2`, `BIGQUERY_LOAD_MODE=partition-refresh`, 35-day core scraper hot fact retention, and 90-day retention for partitioned forecast/backtest tables. GDS facts and currently unpartitioned tiny winner tables are intentionally excluded for now because their storage footprint is small.
- Automatic BigQuery sync is skipped when the extraction gate is `FAIL`, even if the scrape process returned `0`. Use `--fail-on-extraction-gate` when scheduled runs should return non-zero for that condition.
- Live dataset `aeropulseintelligence.aviation_intel` is in `asia-south1`; storage metrics collection is enabled through `region-asia-south1.enable_info_schema_storage`.
- Long-term history remains in local PostgreSQL, database backups, and ignored Parquet exports under `output/warehouse/`.

### API

- FastAPI is hosted separately from the frontend, preferably Cloud Run.
- Hosted mode should work BigQuery-first without requiring `AIRLINE_DB_URL` for the main public/runtime reporting surfaces.
- PostgreSQL remains needed for local transitional endpoints and access-request/user-session storage unless an alternate store is configured.
- Main endpoint groups:
  - health and metadata
  - reporting cycles/current snapshots/route monitor matrix
  - route-date availability and airline operations
  - changes/change dashboard
  - penalties and taxes
  - forecasting latest
  - Excel export
  - user auth and access requests
  - GDS and Travelport feedback routers

### Web

- The web app is under `apps/web`.
- Current primary pages:
  - `/`: executive overview
  - `/market`: market intelligence hub
  - `/routes`: live route monitor
  - `/operations`: airline operations
  - `/penalties`: penalty comparison
  - `/taxes`: tax monitor
  - `/changes`: market movement dashboard/event browser
  - `/forecasting`: ML/DL forecast and backtest review
  - `/downloads`: downloadable artifacts
  - `/gds`: GDS-oriented views
  - `/admin`: access request/search config/admin flows
- API base defaults to `http://127.0.0.1:8000`, override with `API_BASE_URL` or `NEXT_PUBLIC_API_BASE_URL`.

## Current Airline and Route Coverage

Enabled airline/channel config count by module:

- direct/special modules: `airarabia`, `airasia`, `airastra`, `biman`, `bs`, `indigo`, `maldivian`, `novoair`, `salamair`
- OTA/shared module: `sharetrip` currently backs 13 configured airline codes
- session/manual-related modules also exist for AMYBD, BDFare, GoZayaan, and browser/manual capture flows

Route counts from `config/routes.json` on 2026-04-26:

| Airline | Route rows |
|---|---:|
| 2A | 14 |
| 3L | 2 |
| 6E | 4 |
| 8D | 2 |
| AK | 2 |
| BG | 62 |
| BS | 62 |
| CZ | 2 |
| EK | 2 |
| FZ | 2 |
| G9 | 2 |
| MH | 2 |
| OD | 2 |
| OV | 2 |
| Q2 | 2 |
| QR | 2 |
| SQ | 2 |
| SV | 4 |
| TG | 2 |
| UL | 2 |
| VQ | 14 |
| WY | 2 |

## Trip Planning Decisions

- One-way observations remain the canonical fact model.
- Round-trip support is represented as search intent and itinerary-link metadata in `flight_offer_raw_meta`, not by replacing `flight_offers`.
- Key round-trip metadata fields include `search_trip_type`, `trip_request_id`, `requested_outbound_date`, `requested_return_date`, `trip_duration_days`, `trip_origin`, `trip_destination`, `leg_direction`, `leg_sequence`, and `itinerary_leg_count`.
- Biman has the first true round-trip connector support path.
- Other connectors may accept round-trip kwargs while remaining effectively one-way until explicitly upgraded.
- Operational mode is comparison-safe and uses active route profiles.
- Training mode adds daily enrichment profiles and inventory-anchor tracking.
- Deep mode is the broadest weekly/opportunistic market-movement expansion.
- A trip profile only becomes effective when it appears in the route's candidate list and the correct active/training/deep activation list.

## Operational Decisions

- Scheduler launches should be finish-driven and sequential, not overlapping.
- Do not start a new ingestion cycle while an active/fresh accumulation exists.
- Enforce a completion buffer after a completed accumulation before the next launch.
- Recommended buffers:
  - operational: 90 minutes
  - training: 120 minutes
  - deep: 120 minutes
- PostgreSQL unavailability should cause clean skips/fail-fast behavior, not partial/broken cycles.
- During incidents, trust order is:
  1. PostgreSQL service health
  2. aggregate parallel-run artifact
  3. guarded wrapper/lock state
  4. worker-local heartbeat files
- Runtime bottleneck is accumulation/search time, not prediction or BigQuery sync.

## Common Commands

Set up Python dependencies:

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip setuptools wheel
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Install browser runtime when browser-assisted flows are needed:

```powershell
.\.venv\Scripts\python.exe -m playwright install chromium
```

Run full pipeline:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --route-monitor --route-monitor-macro-xlsm
```

Run operational ingestion wrapper:

```powershell
cmd /c scheduler\run_ingestion_4h_once.bat
```

Run API locally:

```powershell
.\.venv\Scripts\python.exe -m uvicorn apps.api.app.main:app --reload
```

Run web locally:

```powershell
cd apps\web
npm install
npm run dev
```

Run local CI checks:

```powershell
.\.venv\Scripts\python.exe tools\ci_checks.py --allow-db-skip --reports-dir output/reports --timestamp-tz local
```

Validate trip config:

```powershell
.\.venv\Scripts\python.exe tools\validate_trip_config.py
```

Stage BigQuery export:

```powershell
.\.venv\Scripts\python.exe tools\export_bigquery_stage.py --output-dir output\warehouse\bigquery --start-date 2026-03-01 --end-date 2026-03-07
```

Apply BigQuery hot-cache retention:

```powershell
.\.venv\Scripts\python.exe tools\bigquery_apply_retention.py --project-id aeropulseintelligence --dataset aviation_intel --hot-days 35 --forecast-days 90 --time-travel-hours 48 --apply
```

Audit BigQuery storage:

```powershell
.\.venv\Scripts\python.exe tools\bigquery_storage_audit.py --project-id aeropulseintelligence --dataset aviation_intel
```

Preflight and extraction health:

```powershell
.\.venv\Scripts\python.exe tools\pre_flight_session_check.py --dry-run
.\.venv\Scripts\python.exe run_pipeline.py --skip-reports --skip-prediction --skip-bigquery-sync --limit-routes 1 --limit-dates 1
.\.venv\Scripts\python.exe run_pipeline.py --fail-on-extraction-gate --retry-missing-airlines
.\.venv\Scripts\python.exe tools\extraction_health_report.py --cycle-id YOUR-CYCLE-ID --expected-airlines BG,VQ,BS,2A
Get-Content output\reports\extraction_health_latest.md
```

Temporarily disable ShareTrip-backed collection and fallbacks:

```powershell
$env:SHARETRIP_ENABLED="false"
.\.venv\Scripts\python.exe tools\pre_flight_session_check.py --dry-run
.\.venv\Scripts\python.exe tools\audit_airline_source_plan.py
```

## Source Directory Map

- `.github/`: GitHub workflows and automation metadata.
- `.githooks/`: local git hook helpers.
- `airlines/`: older/parallel airline-specific scripts.
- `alembic/`: database migrations.
- `apps/api/`: FastAPI reporting API, repositories, routers, Cloud Run config.
- `apps/web/`: Next.js frontend.
- `cache/`, `cookies/`, `logs/`, `output/`, `reports/`: runtime or generated artifacts; be careful before committing generated files.
- `config/`: airline, route, dates, trip windows, schedules, airport/country/timezone, market prior, holiday, fleet, and output configuration.
- `core/`: shared pipeline logic for trip context/config, date utilities, parser/response helpers, retries, Playwright requester, offer identity, forecasting features, explainability, imputation, transfer learning, prediction monitoring, cookies, and Excel writing.
- `deploy/`: GCP and Oracle Cloud deployment helpers.
- `docs/`: current architecture, deployment, web, feature engineering, and migration docs.
- `engines/`: comparison, trend/time-series, route scope/intelligence, output writing, schema validation, and Excel comparison adapter.
- `evidence/`: curated model/probe/comparative evidence used for thesis-grade analysis.
- `legacy/`: older scripts and scratch artifacts retained for reference.
- `models/`: SQLAlchemy models.
- `modules/`: current connectors/parsers/request helpers for airlines and OTAs.
- `parsers/`: parser utilities.
- `scheduler/`: Windows/Linux scheduler wrappers and always-on maintenance.
- `sql/`: PostgreSQL/BigQuery schema and view SQL.
- `tests/`: pytest coverage for trip config, route classification, parsers/connectors, retry policy, extraction health/preflight gates, prediction monitor, imputation, offer identity, booking curve, route characteristics, fleet mapping, and related behavior.
- `tools/`: diagnostics, export/load, manual capture, HAR import, extraction health/preflight, training, monitoring, maintenance, deployment, hygiene, and recovery helpers.
- `warehouse/bigquery/`: BigQuery setup, Looker, forecasting dashboard, and bootstrap documentation.
- `workflows/`: n8n/manual-assisted workflow JSON.

## Markdown Documentation Map

### Root Current Docs

- `README.md`: broad project overview, data flow, commands, layout, and current status.
- `PROJECT_CONTEXT.md`: single new-chat handoff, current architecture snapshot, decisions, history, and update log.
- `PROJECT_DECISIONS.md`: authoritative strategic/product/architecture decisions.
- `OPERATIONS_RUNBOOK.md`: daily checks, incident triage, scheduler behavior, runtime baseline, and operational/training/deep mode commands.

### Current Docs Folder

- `docs/WEB_PLATFORM_PLAN.md`: web/API/BigQuery architecture and delivery order.
- `docs/WEB_PRODUCT_REQUIREMENTS.md`: product requirements for web platform.
- `docs/ROUND_TRIP_ARCHITECTURE.md`: current round-trip model and trip config behavior.
- `docs/FEATURE_ENGINEERING_GUIDE.md`: forecasting feature categories and rationale.
- `docs/DEPLOYMENT_RUNBOOK.md`: Vercel, Cloud Run, database, warehouse, and rollout guidance.
- `docs/NEON_MIGRATION_RUNBOOK.md`: optional Neon migration workflow.
- `docs/GITHUB_DEPLOY_SECRETS.md`: CI/CD secrets guidance.

### App Docs

- `apps/api/README.md`: API scope, endpoint map, hosted mode, Excel export, deployment.
- `apps/web/README.md`: web stack, local run, pages, UX guardrails, deployment.

### Warehouse and BI Docs

- `warehouse/bigquery/README.md`: BigQuery warehouse plan and curated tables.
- `warehouse/bigquery/BOOTSTRAP_CHECKLIST.md`: concrete BigQuery setup and validation sequence.
- `warehouse/bigquery/LOOKER_STUDIO_SETUP.md`: Looker setup guidance.
- `warehouse/bigquery/LOOKER_CLICK_CHECKLIST.md`: dashboard click-by-click build checklist.
- `warehouse/bigquery/FORECASTING_BACKTEST_DASHBOARD_SPEC.md`: forecast/backtest dashboard spec.

### Config, Evidence, Cookies, Deploy

- `config/TRIP_PROFILE_GUIDE.md`: route trip profile activation model and validation rules.
- `evidence/README.md`: evidence folder policy.
- `cookies/README.md`: cookie/session artifact guidance.
- `deploy/oracle_cloud/README.md`: Oracle Cloud Always Free migration and systemd/timer path.
- `evidence/*/*.md`: curated probe, comparative study, manifest, and model-summary evidence.

## Current Risks and Open Work

- Some airline-direct endpoints are protected by WAF/anti-bot systems.
- BS/2A TTInteractive remains DataDome-protected; OTA fallback chain exists but is fragile.
- Q2/Maldivian and some other sources are session/UI-state sensitive.
- AMYBD/GoZayaan sessions can expire silently; preflight contracts now exist, but route/date-level validity is still proven by extraction attempts.
- OV/SalamAir validation and route expansion remain noted in daily tracking.
- G9/Air Arabia direct connector/HAR workflow was noted as newly upgraded around 2026-04-09.
- Accumulation runtime is the main bottleneck; parallel execution should be conservative and family-aware.
- Some docs mention pending full integration tests that require environment setup.
- The web/API surface is in active transition from Excel-first to hosted read-first.
- Route-trip docs contain a duplicated "Upgrade OTA connectors" next step; clean up when editing that doc.

## Decision History

| Date | Decision | Files / Areas |
|---|---|---|
| 2026-03-09 | Keep one-way facts canonical; represent round-trip as search-intent/raw-meta link metadata first. | `docs/ROUND_TRIP_ARCHITECTURE.md`, `core/trip_context.py`, `run_all.py`, `models/flight_offer_raw_meta.py` |
| 2026-03-09 | Runtime bottleneck is accumulation/search time, not prediction or BigQuery sync. | `OPERATIONS_RUNBOOK.md`, `run_pipeline.py`, scheduler wrappers |
| 2026-03-20 | Platform vision confirmed: monitoring, pricing intelligence, revenue prediction, benchmarking, later semi-automation. | `PROJECT_DECISIONS.md` |
| 2026-03-20 | Local PostgreSQL remains operational/training store; BigQuery became the hosted read layer; superseded on 2026-04-27 to bounded hot-cache scope. | `README.md`, `PROJECT_DECISIONS.md`, `warehouse/bigquery/README.md` |
| 2026-03-20 | Scheduler should be finish-driven/sequential with completion buffers and DB fail-fast behavior. | `PROJECT_DECISIONS.md`, `OPERATIONS_RUNBOOK.md`, `scheduler/` |
| 2026-03-22 | Bangladesh domestic trip-profile membership bug identified and corrected; OW baseline must appear in both candidate and active profile lists. | `PROJECT_DECISIONS.md`, `config/route_trip_windows.json`, `config/TRIP_PROFILE_GUIDE.md` |
| 2026-03-23 | Six quick-win forecasting improvements verified; Phase 2 integration path defined. | Historical status docs later consolidated into `PROJECT_CONTEXT.md`. |
| 2026-04-09 | Hard-source reliability plan added for session-dependent OTA/manual/browser-capture sources. | Historical tracking docs later consolidated into `PROJECT_CONTEXT.md`. |
| 2026-04-09 | Parallel execution strategy should group source families and profile before expansion. | `tools/parallel_airline_runner.py`, `run_pipeline.py`, and historical roadmap notes consolidated here. |
| 2026-04-26 | Created this single new-chat handoff and living update file. | `PROJECT_CONTEXT.md` |
| 2026-04-27 | Repair Alembic history in place when autogenerated migrations contain accidental destructive operations, while preserving revision IDs. | `alembic/versions/`, `alembic/env.py`, `models/__init__.py` |
| 2026-04-27 | Web data helpers must skip production static-build API fetches when no API base URL is configured, while retaining localhost fallback for local development. | `apps/web/lib/api.ts`, `apps/web/lib/gds.ts` |
| 2026-04-27 | BigQuery is a bounded hosted hot cache, not the long-term history store; history remains in PostgreSQL, backups, and Parquet exports. | `run_pipeline.py`, `tools/export_bigquery_stage.py`, `tools/bigquery_apply_retention.py`, `tools/bigquery_storage_audit.py`, `warehouse/bigquery/README.md` |
| 2026-04-27 | Live BigQuery retention was applied manually through BigQuery SQL console because the local loader service account lacked `bigquery.datasets.update`. | `aeropulseintelligence.aviation_intel`, BigQuery SQL console |
| 2026-04-27 | Extraction quality is a first-class gate separate from process exit; BigQuery auto-sync must not publish when extraction health is `FAIL`. | `run_all.py`, `run_pipeline.py`, `models/extraction_attempt.py`, `core/extraction_health.py`, `tools/pre_flight_session_check.py` |
| 2026-04-30 | Capture-summary, session, headers, cookies, and rate-limit-state writes must be atomic (`tempfile + os.replace`) to prevent half-written `*_latest.json` files from corrupting the next pipeline cycle on crash/power-loss. | `core/atomic_write.py`, `tools/import_*_har.py`, `tools/refresh_*_session.py`, `tools/refresh_*_token.py` |
| 2026-04-30 | `extraction_attempts.error_class` is a closed vocabulary (`ALL_ERROR_CLASSES`); new values must be added to the constants block in `core/extraction_health.py` before being emitted by `classify_attempt`. Operator dashboards depend on this set being stable. | `core/extraction_health.py` |
| 2026-04-30 | GoZayaan inter-query throttle is a first-class env var (`GOZAYAAN_INTER_QUERY_SLEEP`, default 3.0s) applied at the top of `fetch_flights_for_airline`. Lowering it risks the 15-minute 429 cooldown that wipes the rest of the cycle. | `modules/gozayaan.py`, `.env.example` |
| 2026-04-30 | `--retry-missing-airlines` filters out airlines whose primary module is currently disabled in `source_switches.json`. Retrying a dead path produces the same zero-row result and burns rate-limit budget. | `run_pipeline.py` |
| 2026-04-30 | `tools/audit_airline_source_plan.py --strict` exits 3 when any enabled airline has an empty effective source chain (primary disabled and no enabled fallbacks). Opt-in gate for scheduled runs against broken `source_switches.json` configurations. | `tools/audit_airline_source_plan.py` |

## Change Log

| Date | Change | Notes |
|---|---|---|
| 2026-04-26 | Added `PROJECT_CONTEXT.md`. | Consolidates repo architecture, docs map, operational decisions, commands, risks, decision history, and update rules for future chats. |
| 2026-04-26 | Linked `PROJECT_CONTEXT.md` from `README.md`. | Makes the new handoff file discoverable from the main project entry point. |
| 2026-04-26 | Removed redundant/archived Markdown files after consolidating context. | Kept authoritative manuals and current app/API/warehouse/config docs; removed old status, roadmap, checklist, and archived Markdown files. |
| 2026-04-26 | Cleaned generated local artifacts. | Removed Python bytecode caches outside `.venv`, pytest cache, CatBoost training cache, temporary scrape folders/files, local verification logs, and one `.bak` file. |
| 2026-04-27 | Completed remaining repo cleanup and verification pass. | Added ESLint CLI setup, prevented production build-time localhost API fetches, repaired Alembic migration history, imported `StrategySignal` for metadata, de-duplicated BG trip profiles, refreshed `requirements-lock.txt`, removed tracked runtime artifacts, generated local backup/restore sentinels, and confirmed smoke `PASS`. |
| 2026-04-27 | Added BigQuery cost controls. | Added explicit sync opt-in, 2-day default sync window, partition-refresh loading, hot-cache retention tooling, storage audit tooling, daily scheduled sync cadence, docs for 35-day core scraper hot facts, and 90-day partitioned forecast/backtest retention. GDS facts and unpartitioned tiny winner tables are excluded for now due to low storage footprint. |
| 2026-04-27 | Applied live BigQuery cost-control settings. | Enabled storage metrics for `region-asia-south1`, applied retention in BigQuery console, and verified `partition_retention_warnings=0`. Current largest table remains `fact_change_event`, now bounded by retention. |
| 2026-04-27 | Added extraction reliability telemetry and gates. | Added `extraction_attempts`, per-query health classification, latest JSON/MD/CSV health reports, connector health contracts, preflight tool, stale-capture rejection, throttled query launch, one retry pass for retryable source failures, worker-scoped output aggregation, protected-source manual queues for `Q2`, `G9`, `OV`, `BS`, and `2A`, stale-date filtering for manual queues, ShareTrip kill-switch support, and BigQuery skip-on-`FAIL` behavior. |
| 2026-04-30 | Tier 1 reliability hardening pass landed (5 items). | (1) `core/atomic_write.py` adds `atomic_write_json` / `atomic_write_text` helpers; HAR-import + session-refresh tools rerouted through them. (2) `core/extraction_health.py` formalizes the closed `ALL_ERROR_CLASSES` vocabulary with `is_known_error_class` helper and a `classify_attempt` docstring listing every emitted value. (3) `modules/gozayaan.py` honours `GOZAYAAN_INTER_QUERY_SLEEP` (default 3.0s) at the top of every `fetch_flights_for_airline` call; `.env.example` documents it. (4) `run_pipeline.py` filters `--retry-missing-airlines` against `source_switches.json` runtime status. (5) `tools/audit_airline_source_plan.py --strict` exits 3 on empty effective chains. Behaviour-preserving for cycles that don't touch the new flags. Verified by `pytest -q` (146 passed) and `tools/ci_checks.py` (`ci_ok=True`). |

## How Future Chats Should Start

1. Read `PROJECT_CONTEXT.md`.
2. Check `git status --short`.
3. For product/architecture decisions, read `PROJECT_DECISIONS.md` next.
4. For operational/debugging work, read `OPERATIONS_RUNBOOK.md` next.
5. For web/API work, read `docs/WEB_PLATFORM_PLAN.md`, `apps/api/README.md`, and `apps/web/README.md`.
6. For trip planning or round-trip changes, read `docs/ROUND_TRIP_ARCHITECTURE.md` and `config/TRIP_PROFILE_GUIDE.md`.
7. For warehouse/BI work, read `warehouse/bigquery/README.md` and `warehouse/bigquery/BOOTSTRAP_CHECKLIST.md`.
8. After making changes, update this file's **Decision History** or **Change Log** before finishing.

---

> **Codex will review your output once you are done.**
