# Operations Runbook

## Daily Checks (5-10 minutes)

1. Confirm automation heartbeat files are updating:
   - `logs/always_on_maintenance.log`
   - `logs/maintenance_pulse.log`
   - `output/reports/always_on_maintenance_state.json`
2. Confirm daily ops health archive exists for today:
   - `output/reports/ops_health_YYYYMMDD_*.md`
3. Confirm latest system status snapshot exists:
   - `output/reports/system_status_latest.md`
   - `output/reports/system_status_latest.json`
4. Confirm smoke check status:
   - `output/reports/smoke_check_latest.md`
   - `output/reports/smoke_check_latest.json`
5. Confirm SLA + drift monitors:
   - `output/reports/data_sla_latest.md`
   - `output/reports/model_drift_latest.md`
6. Confirm latest ops status is healthy:
   - `Status: PASS` in `output/reports/ops_health_latest.md`
7. Confirm DB protection artifacts are current:
   - `output/backups/db_backup_latest.json`
   - `output/backups/db_restore_test_latest.json`
   - `output/backups/db_restore_drill_latest.json`
8. Confirm unified operator view:
   - `output/reports/operator_dashboard_latest.md`
9. Confirm latest extraction health:
   - `output/reports/extraction_health_latest.md`
   - `output/reports/extraction_health_latest.json`
   - Expected status: `PASS` or explainable `WARN`; `FAIL` blocks BigQuery auto-sync.
10. Confirm scheduled task entries are still present:
   - `AirlineIntel_DailyOps`
   - `AirlineIntel_WeeklyPack`
   - `AirlineIntel_MaintenancePulse`
   - `AirlineIntel_Ingestion4H`
   - `AirlineIntel_IngestionOnLogon`
- Operational and daily training launchers are now finish-driven.
- Installers create the initial one-shot trigger only; the wrapper reschedules the next run after finish plus buffer.
- Ingestion launch policy is sequential: never start a new cycle while an active/fresh accumulation exists, and enforce a configurable completion buffer after the last completed accumulation.
- Current configured completion buffers are controlled by:
  - `OPERATIONAL_COMPLETION_BUFFER_MINUTES`
  - `TRAINING_COMPLETION_BUFFER_MINUTES`
  - `DEEP_COMPLETION_BUFFER_MINUTES`
- `ACCUMULATION_COMPLETION_BUFFER_MINUTES` remains a compatibility fallback.
- Recommended settings:
  - operational: `90` minutes
  - training: `120` minutes
  - deep: `120` minutes

## Exact Verification Commands

Quick accumulation runtime verifier (scheduler + active accumulation + heartbeat freshness):
`powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools\verify_accumulation_runtime.ps1`

```powershell
Get-Date -Format "yyyy-MM-dd HH:mm:ss zzz"
Get-ChildItem logs\always_on_maintenance.log,logs\maintenance_pulse.log | Select-Object Name,Length,LastWriteTime
Get-Content output\reports\always_on_maintenance_state.json
Get-Content output\reports\ops_health_latest.md | Select-Object -First 30
Get-Content output\reports\system_status_latest.md
Get-Content output\reports\smoke_check_latest.md
Get-Content output\reports\data_sla_latest.md
Get-Content output\reports\model_drift_latest.md
Get-Content output\reports\operator_dashboard_latest.md
Get-Content output\reports\extraction_health_latest.md
Get-Content output\backups\db_backup_latest.json
Get-Content output\backups\db_restore_test_latest.json
Get-Content output\backups\db_restore_drill_latest.json
schtasks /Query /TN AirlineIntel_DailyOps /FO LIST /V | findstr /I /C:"Status:" /C:"Next Run Time" /C:"Task To Run"
schtasks /Query /TN AirlineIntel_WeeklyPack /FO LIST /V | findstr /I /C:"Status:" /C:"Next Run Time" /C:"Task To Run"
schtasks /Query /TN AirlineIntel_MaintenancePulse /FO LIST /V | findstr /I /C:"Status:" /C:"Next Run Time" /C:"Repeat: Every"
schtasks /Query /TN AirlineIntel_Ingestion4H /FO LIST /V | findstr /I /C:"Status:" /C:"Next Run Time" /C:"Repeat: Every" /C:"Task To Run"
schtasks /Query /TN AirlineIntel_IngestionOnLogon /FO LIST /V | findstr /I /C:"Status:" /C:"Task To Run"
```

## Expected Good State

- `always_on_maintenance_state.json` shows recent:
  - `last_cycle_at`
  - `last_daily_ok_at`
- `ops_health_latest.md` shows:
  - `Status: PASS`
  - `Non-zero Pipeline RC: none`
- `smoke_check_latest.md` shows:
  - `Overall status: PASS` (or acceptable WARN with clear reason)
- `db_backup_latest.json`:
  - `"ok": true`
- `db_restore_test_latest.json`:
  - `"ok": true`
- `db_restore_drill_latest.json`:
  - `"ok": true` (or minor count drift within allowed threshold)
- `data_sla_latest.md`:
  - `Status: PASS` (or explain WARN explicitly)
- `model_drift_latest.md`:
  - Drift groups reviewed when status is WARN/FAIL
- `system_status_latest.md` points to current-day `ops_health_*` file.
- `extraction_health_latest.md` shows `status: PASS` for a healthy publishable cycle, with no manual-action or retry-required source failures.
- Pulse task repeats every 30 minutes.
- Ingestion wrapper skips launches when:
  - a pipeline process is still active
  - heartbeat state is still `running` and fresh
  - the last completed accumulation is less than the configured completion buffer old
  - PostgreSQL is unreachable at launch time

## Current Runtime Baseline

Observed on Monday, March 9, 2026:

- Accumulation runtime:
  - `3cef2491...` started `2026-03-09 12:45 UTC`
  - completed `2026-03-09 17:16 UTC`
  - duration: about `4h 31m`
- Post-accumulation runtime:
  - prediction: about `3m`
  - BigQuery sync: about `3.5m`
- Operational conclusion:
  - the main bottleneck is accumulation/search time, not reporting, prediction, or warehouse sync
  - round-trip search was not active in this baseline (`trip_type=OW`)

Immediate runtime-reduction priorities:

1. Prevent overlap and duplicate work first.
2. Use `run_all.py --profile-runtime` to identify slow airline/route/date segments.
3. Use `tools/parallel_airline_runner.py` conservatively for safe airline-level parallelism.
4. Keep prediction and sync enabled; they are not the primary runtime cost.
5. Only expand round-trip coverage after one-way baseline runtime is under control.

## Current Stability Priorities

Until the platform is further hardened, use this order of trust during incidents:

1. PostgreSQL service health
2. aggregate parallel-run artifact
3. guarded wrapper / lock state
4. worker-local heartbeat files

Meaning:

- if PostgreSQL is down, do not trust post-run coverage summaries
- if worker-local status disagrees with aggregate parallel output, trust the aggregate artifact
- if a wrapper lock exists and heartbeat is fresh, do not force another launch

### Incident Triage Order

1. Check PostgreSQL service and connectivity first.
2. Check aggregate parallel output for the last cycle.
3. Check guarded-wrapper/recovery state.
4. Only then inspect per-worker heartbeat files.

Recommended checks:

```powershell
Get-Service *postgres*
.\.venv\Scripts\python.exe -c "import db; s=db.get_session(); print('db_ok'); s.close()"
.\.venv\Scripts\python.exe tools\pre_flight_session_check.py --dry-run
Get-Content output\reports\scrape_parallel_latest.json -TotalCount 120
Get-Content output\reports\extraction_health_latest.md -TotalCount 120
Get-Content output\reports\run_all_status_latest.json -TotalCount 80
Get-Content output\reports\accumulation_wrapper_lock.json
Get-Content output\reports\run_all_accumulation_status_latest.json -TotalCount 80
```

Interpretation:

- `scrape_parallel_latest.json` is the better source for whole-cycle airline coverage and worker outcomes.
- `extraction_health_latest.json/md/csv` is the best source for source quality. It records per-query outcomes and should be checked before trusting a cycle for publication.
- `run_all_status_latest.json` may only reflect the last worker heartbeat, not the full parallel cycle.
- `run_all_accumulation_status_latest.json` is the wrapper/accumulation heartbeat, not the final source of whole-cycle truth.
- `postgres_unreachable` from the recovery helper means do not launch or recover yet; restore PostgreSQL service health first.

## Scheduler Timing

Main timing config lives in `config/schedule.json` under `scheduler_timing`.

- `global`: normal full ingestion timing.
- `sources`: source/supplier timing for all airlines whose primary module is that source.
- `airlines`: airline-specific timing.
- `routes`: route-specific timing with `airline`, `origin`, and `destination`.

Preview current timing:

```powershell
.\.venv\Scripts\python.exe tools\scheduler_timing_plan.py
Get-Content output\reports\scheduler_timing_plan_latest.md
```

Install/update tasks:

```powershell
# Global all-source ingestion task
powershell -ExecutionPolicy Bypass -File scheduler\install_ingestion_autorun.ps1

# Optional source/airline/route scoped tasks
powershell -ExecutionPolicy Bypass -File scheduler\install_scoped_ingestion_autorun.ps1 -WhatIf
powershell -ExecutionPolicy Bypass -File scheduler\install_scoped_ingestion_autorun.ps1
```

Scoped source timing uses the airline's primary `module` in `config/airlines.json`; fallback-only suppliers are controlled with airline or route timing plus source modes/switches.

## Operational vs Training Cycles

The collection stack now supports two trip-planning modes:

- `operational`
  - comparison-safe baseline for web freshness, health, and default cycle-to-cycle views
  - uses only `active_market_trip_profiles` from [`config/route_trip_windows.json`](config/route_trip_windows.json)
- `training`
  - forecasting/model core-enrichment pass
  - uses operationally active profiles plus `training_market_trip_profiles`
  - may also include training-only inventory anchor profiles so repeated observations of the same departure horizon are collected for inventory movement modeling
  - should own the richer forecasting refresh (`CatBoost + LightGBM + MLP`) and publish updated forecast outputs to BigQuery
- `deep`
  - broad weekly/opportunistic enrichment pass
  - starts from the fuller route-level `market_trip_profiles` candidate set
  - then adds `training_market_trip_profiles`
  - then adds `deep_market_trip_profiles`
  - intended for the heaviest holiday, tourism, worker-return, and long-haul market-movement expansions

Current intent:

- operational cycle = basic monitoring/comparison
- training enrichment cycle = daily forecasting/training signal expansion
- deep enrichment cycle = weekly/opportunistic broad market-movement expansion
- holiday overlays belong in training or deep enrichment, not the default operational cycle
- inventory-anchor tracking belongs in training enrichment, not the default operational cycle

Current wrappers:

- operational:
  - [`scheduler/run_ingestion_4h_once.bat`](scheduler/run_ingestion_4h_once.bat)
  - [`scheduler/run_ingestion_4h_once.sh`](scheduler/run_ingestion_4h_once.sh)
- training enrichment:
  - [`scheduler/run_training_enrichment_once.bat`](scheduler/run_training_enrichment_once.bat)
  - [`scheduler/run_training_enrichment_once.sh`](scheduler/run_training_enrichment_once.sh)
- deep enrichment:
  - [`scheduler/run_training_deep_once.bat`](scheduler/run_training_deep_once.bat)
  - [`scheduler/run_training_deep_once.sh`](scheduler/run_training_deep_once.sh)

Notes:

- the training wrapper sets `RUN_ALL_TRIP_PLAN_MODE=training`
- the deep wrapper sets `RUN_ALL_TRIP_PLAN_MODE=deep`
- `run_pipeline.py` now accepts `--trip-plan-mode operational|training|deep`
- the operational wrapper remains the default comparison-safe scheduler path

## Home Laptop Scheduler

Yes, the long-running training scheduler can be moved to another laptop.

Recommended split:

- primary machine:
  - operational cycle only
  - shorter, comparison-safe cadence
- home / always-on laptop:
  - training enrichment cycle
  - long-running holiday/return-window expansion for forecasting
  - optional deep enrichment attempts when the training lane is free
  - if both operational and training are moved there, the machine becomes the single scheduler host

Requirements for the home laptop:

1. Clone the same repo revision.
2. Create and populate `.venv`.
3. Copy `.env` with:
   - database connection
   - `BIGQUERY_PROJECT_ID`
   - `BIGQUERY_DATASET`
   - `GOOGLE_APPLICATION_CREDENTIALS`
4. Ensure network access to the same PostgreSQL instance.
5. Ensure the machine does not sleep during the scheduled window.
6. Register only the training scheduler there, not the operational 4-hour task.

Recommended training launch command on Windows:

```powershell
powershell -ExecutionPolicy Bypass -File scheduler\run_training_enrichment_once.bat
```

Default training model configuration:

- `TRAINING_PREDICTION_ML_MODELS=catboost,lightgbm`
- `TRAINING_PREDICTION_DL_MODELS=mlp`
- `TRAINING_SKIP_BIGQUERY_SYNC=0`

Default deep model configuration:

- `DEEP_PREDICTION_ML_MODELS=catboost,lightgbm`
- `DEEP_PREDICTION_DL_MODELS=mlp`
- `DEEP_SKIP_BIGQUERY_SYNC=0`

Meaning:

- the home-laptop training scheduler is expected to refresh forecasting outputs
- BigQuery forecast tables should be updated from that run
- the hosted forecasting page should then read the refreshed outputs
- a deep run uses the same training lane and should be treated as opportunistic; it should not overlap with daily core training

Recommended training schedule:

- once daily, or
- every 12-24 hours depending on runtime and model appetite

Do not schedule training every 4 hours unless the runtime is proven safe on that second machine.

Recommended deep schedule:

- weekly attempt, or
- manual/event-triggered run

Do not treat deep as a fixed guaranteed weekly slot unless you are comfortable with it delaying core daily training on the same laptop.

## Recommended Home Laptop Setup

Use a frequent lightweight launcher plus the preflight lock/buffer. That is more future-proof than trying to calculate exact next-start times manually.

Suggested settings:

- `.env`
  - `OPERATIONAL_COMPLETION_BUFFER_MINUTES=90`
  - `TRAINING_COMPLETION_BUFFER_MINUTES=120`
  - `DEEP_COMPLETION_BUFFER_MINUTES=120`
  - `ACCUMULATION_COMPLETION_BUFFER_MINUTES=90` (fallback only)
  - `TRAINING_PREDICTION_ML_MODELS=catboost,lightgbm`
  - `TRAINING_PREDICTION_DL_MODELS=mlp`
  - `TRAINING_SKIP_BIGQUERY_SYNC=0`
- operational launcher task
  - one initial trigger only
  - wrapper schedules the next run after completion + buffer
- training enrichment task
  - once daily or every `1440` minutes
  - recommended start: `01:30` local time
- deep enrichment task
  - weekly attempt or manual trigger
  - should use the same lock/buffer behavior as training
  - recommended as opportunistic, not guaranteed, on a single-laptop training lane

Why this is sensible:

- the launcher itself is cheap
- the preflight lock prevents overlap
- separate operational/training completion buffers prevent immediate back-to-back restarts while keeping each run family independently tunable
- hourly operational checks recover sooner after long runs than a rigid 6-hour trigger

Important capacity note:

- one laptop can host both schedulers
- but with current estimated runtimes it is not realistic to expect many operational cycles plus a full training cycle every day from a single machine
- realistic near-term expectation on one always-on laptop is:
- one comparison-safe operational run per day, and
- one training enrichment run per day
- deep enrichment should be considered optional and opportunistic on a single home laptop
- if you need more operational freshness than that, runtime must be reduced further or collection must be split across machines

Windows install commands:

```powershell
powershell -ExecutionPolicy Bypass -File scheduler\install_ingestion_autorun.ps1
powershell -ExecutionPolicy Bypass -File scheduler\install_training_enrichment_autorun.ps1 -StartTime 01:30
powershell -ExecutionPolicy Bypass -File scheduler\install_training_deep_autorun.ps1 -StartTime 02:00
```

## Manual Operational Mode

For laptops that are not reliably always-on, operational collection should be run
manually instead of through Task Scheduler.

Disable the operational autorun tasks:

```powershell
powershell -ExecutionPolicy Bypass -File scheduler\disable_ingestion_autorun.ps1
```

Manual operational launch:

```powershell
cmd /c scheduler\run_ingestion_4h_once.bat
```

Recommended use:

- start operational only when you have enough uninterrupted runtime
- keep daily training/deep scheduling on a more stable machine if needed
- treat website freshness lag separately from collection completeness; current
  warehouse publish time can still add substantial delay after scrape completion
## If Daily Ops File Did Not Update

1. Run one manual cycle:

```powershell
.\.venv\Scripts\python.exe scheduler\always_on_maintenance.py --python-exe .\.venv\Scripts\python.exe --reports-dir output\reports --run-on-start --once
```

2. Re-check:
   - `output/reports/ops_health_latest.md`
   - `output/reports/ops_health_YYYYMMDD_*.md`
   - `output/reports/system_status_latest.md`

3. Check logs:
   - Last 60 lines of `logs/always_on_maintenance.log`
   - Last 60 lines of `logs/maintenance_pulse.log`

```powershell
Get-Content logs\always_on_maintenance.log -Tail 60
Get-Content logs\maintenance_pulse.log -Tail 60
```

## If Weekly Thesis Pack Did Not Update (on weekly day)

1. Trigger manually:

```powershell
.\.venv\Scripts\python.exe scheduler\maintenance_tasks.py --task weekly_pack --reports-dir output\reports --logs-dir logs --timestamp-tz local
```

2. Verify:
   - New `output/reports/thesis_pack_*.zip`
   - `output/reports/system_status_latest.md` updated with new pack path.

## If Backup/Restore Checks Stay WARN

Symptom in `smoke_check_latest.md`:
- `db_backup_latest` warns `pg_dump_not_found_on_path`
- `db_restore_test_latest` warns `pg_restore_not_found_on_path`

Action:
1. Install PostgreSQL client tools and ensure `pg_dump` / `pg_restore` are on PATH.
2. Re-run one full maintenance pass:

```powershell
.\.venv\Scripts\python.exe scheduler\maintenance_tasks.py --task both --reports-dir output\reports --logs-dir logs --timestamp-tz local
```

3. Confirm:
   - `output/backups/db_backup_latest.json` => `"ok": true`
   - `output/backups/db_restore_test_latest.json` => `"ok": true`
   - `output/backups/db_restore_drill_latest.json` => `"ok": true`

## If Task Scheduler Entries Are Missing

Reinstall no-admin autorun setup:

```powershell
powershell -ExecutionPolicy Bypass -File scheduler\install_always_on_autorun.ps1
powershell -ExecutionPolicy Bypass -File scheduler\install_ingestion_autorun.ps1
```

Then confirm:
- Startup shortcut exists:
  - `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\AirlineIntel AlwaysOn.lnk`
  - `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\AirlineIntel Ingestion Kickoff.lnk`
- Pulse task exists:
  - `AirlineIntel_MaintenancePulse`

## Alerting Check

If webhook configured, verify notification audit trail:

```powershell
Get-Content output\reports\ops_notifications.log -Tail 20
```

`send_ok=false` indicates webhook delivery issue or missing webhook config.

## Weekly Maintenance (Recommended)

1. Review latest:
   - `output/reports/retention_cleanup_latest.json`
   - `output/reports/system_status_latest.md`
2. Run one manual full maintenance pass:

```powershell
.\.venv\Scripts\python.exe scheduler\maintenance_tasks.py --task both --reports-dir output\reports --logs-dir logs --timestamp-tz local
```

3. Confirm new artifacts:
   - `ops_health_*.md`
   - `thesis_pack_*.zip`
   - `data_sla_*.md/json`
   - `model_drift_*.md/json`
   - `operator_dashboard_*.md/html/json`
   - `system_status_*.md/json`

## Recovery Scan (Missed Windows)

Dry-run scan for stale routes/cabins:

```powershell
.\.venv\Scripts\python.exe tools\recover_missed_windows.py --dry-run --output-dir output\reports --timestamp-tz local
```

Active recovery run (executes targeted capture runs):

```powershell
.\.venv\Scripts\python.exe tools\recover_missed_windows.py --output-dir output\reports --timestamp-tz local --max-routes 8
```

## Runtime Profiling / Safe Parallel Accumulation

Single-airline runtime profile:

```powershell
.\.venv\Scripts\python.exe run_all.py --quick --airline BG --origin DAC --destination CXB --cabin Economy --limit-routes 1 --limit-dates 1 --profile-runtime --profile-output-dir output\reports
```

Safe parallel-by-airline run:

```powershell
.\.venv\Scripts\python.exe tools\parallel_airline_runner.py --python-exe .\.venv\Scripts\python.exe --max-workers 2 --quick --limit-routes 1 --limit-dates 1 --output-dir output\reports
```

Check outputs:
- `output/reports/runtime_profile_latest.json`
- latest parallel cycle manifest JSON under `output/reports/`

Cycle-based parallel run (explicit shared snapshot id + timeout guard):

```powershell
$cycle = [guid]::NewGuid().ToString()
.\.venv\Scripts\python.exe tools\parallel_airline_runner.py --python-exe .\.venv\Scripts\python.exe --max-workers 2 --cycle-id $cycle --query-timeout-seconds 120 --quick --limit-routes 1 --limit-dates 1 --output-dir output\reports
Get-ChildItem output\reports\*.json | Sort-Object LastWriteTime -Descending | Select-Object -First 5
```

Notes:
- `cycle_id` groups all airline worker runs into one comparable snapshot cycle.
- `run_pipeline.py` coverage checks prefer DB rows by `cycle_id` (fallback: `combined_results.csv`).
- Start with `--max-workers 2`; increase only after stable anti-bot/rate-limit behavior.

## Passenger-Mix Basis (ADT/CHD/INF) Checks

Rule:
- Compare like-for-like only. Do not compare `ADT=1` runs to `ADT=2+` runs unless explicitly treating it as a probe analysis.

Default baseline:
- `ADT=1 CHD=0 INF=0`

Live run monitoring (heartbeat + watcher):

```powershell
.\.venv\Scripts\python.exe tools\watch_run_status.py
```

Look for:
- `pax=1/0/0` for baseline runs
- `pax=2/0/0` (or other probe mix) when intentionally probing party-size sensitivity

Direct heartbeat file check:

```powershell
Get-Content output\reports\run_all_status_latest.json
```

Verify:
- `search_passengers.adt/chd/inf`
- `overall_query_completed` is increasing during active runs
- `state` is not `STALE` for long intervals in the watcher

Route monitor comparison basis check:
- `generate_route_flight_fare_monitor.py` now warns if current/previous compared cycles have mismatched passenger mix (`ADT/CHD/INF`).
- If warning appears, regenerate using a like-for-like accumulation pair for valid change analysis.

## ML/DL Forecast Workflow (Daily)

Goal:
- Train on previously captured dates and validate using future realized runs.

1. Run prediction with ML + DL enabled:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --skip-scrape --skip-reports --run-prediction --prediction-target total_change_events --prediction-series-mode event_daily --report-start-date 2026-02-01 --report-end-date 2026-03-01 --prediction-ml-models catboost,lightgbm --prediction-dl-models mlp
```

2. Check model activation in console/log output:
- `ml_active_models=[...]`
- `dl_active_models=[...]`

3. Compare latest overall metrics:

```powershell
Get-ChildItem output\reports\prediction_eval_total_change_events_*.csv | Sort-Object LastWriteTime -Descending | Select-Object -First 1 -ExpandProperty FullName
```

4. Forward validation rule:
- Do not backfill targets from future.
- Let new pipeline runs append actual outcomes, then re-run prediction and compare metric trend.

5. Tuning order:
- Keep baseline metrics as reference.
- Tune ML/DL quantiles and min-history only when route-level + overall metrics improve consistently.

## Local CI Guard (Every Commit)

Install pre-commit hook once:

```powershell
powershell -ExecutionPolicy Bypass -File tools\install_git_hooks.ps1
```

Manual CI check run:

```powershell
.\.venv\Scripts\python.exe tools\ci_checks.py --reports-dir output\reports --timestamp-tz local
```

## OTA Penalty Extraction (Gozayaan HAR)

Use this when you want OTA-side fare/policy comparison and penalty benchmarking.

```powershell
.\.venv\Scripts\python.exe tools\extract_gozayaan_har.py --har "C:\Users\TLL-90134\Downloads\gozayaan.com.har" --output-dir output\reports --timestamp-tz local
```

Expected outputs:
- `output/reports/gozayaan_fares_*.csv`
- `output/reports/gozayaan_policies_*.csv`
- `output/reports/gozayaan_extract_latest.json`

Optional BG fare-rule penalty extraction from saved GraphQL response:

```powershell
.\.venv\Scripts\python.exe tools\extract_bg_fare_rules.py --input output\reports\bg_getBookingFareRules_sample.json --output-dir output\reports
```

## OTA Live Connector (ShareTrip)

BS and 2A use OTA feed by default in `run_all.py` through `modules/sharetrip.py`.
Additional interim OTA carriers are enabled through the same ShareTrip connector (`module=sharetrip`):
`SV, G9, 3L, FZ, EK, QR, WY, CZ, 8D, UL, MH, AK, OD, SQ, TG, 6E`.

Baseline ShareTrip mode (recommended):

```powershell
$env:BS_SOURCE_MODE="sharetrip"
$env:AIRASTRA_SOURCE_MODE="sharetrip"
# Optional ShareTrip overrides:
# $env:SHARETRIP_ACCESS_TOKEN="<token>"   # defaults to known working token in code
# $env:SHARETRIP_PAGE_LIMIT="50"
# $env:SHARETRIP_POLL_MAX_ATTEMPTS="8"
# $env:SHARETRIP_POLL_SLEEP_SEC="1"
# $env:SHARETRIP_ADAPTIVE_POLL_STOP="1"   # default on
# $env:SHARETRIP_EARLY_STOP_MIN_PROGRESS="0.90"
# $env:SHARETRIP_MULTI_PAGE_STABLE_POLLS="2"
# Optional BS/2A fallback behavior:
# default: empty ShareTrip result does NOT call BDFare (faster)
# $env:BS_BDFARE_FALLBACK_ON_EMPTY="1"
# $env:AIRASTRA_BDFARE_FALLBACK_ON_EMPTY="1"
```

Quick connector validation:

```powershell
.\.venv\Scripts\python.exe -m modules.sharetrip --airline BS --origin DAC --destination CGP --date 2026-03-27 --cabin Economy
.\.venv\Scripts\python.exe -m modules.sharetrip --airline 2A --origin DAC --destination CGP --date 2026-03-27 --cabin Economy
.\.venv\Scripts\python.exe -m modules.sharetrip --airline SV --origin DAC --destination JED --date 2026-03-27 --cabin Economy
.\.venv\Scripts\python.exe -m modules.sharetrip --airline EK --origin DAC --destination DXB --date 2026-03-27 --cabin Economy
```

Targeted run examples:

```powershell
.\.venv\Scripts\python.exe run_all.py --quick --airline BS --origin DAC --destination CGP --cabin Economy
.\.venv\Scripts\python.exe run_all.py --quick --airline 2A --origin DAC --destination CGP --cabin Economy
```

If AMYBD-specific follow-up is needed later (currently can return `Invalid Login`), switch mode:

```powershell
$env:BS_SOURCE_MODE="amybd"
$env:AIRASTRA_SOURCE_MODE="amybd"
.\.venv\Scripts\python.exe tools/refresh_amybd_session.py --wait-seconds 240
. .\output\manual_sessions\amybd_env_latest.ps1
```

AMYBD direct connector checks:

```powershell
.\.venv\Scripts\python.exe -m modules.amybd --airline BS --origin DAC --destination CGP --date 2026-03-27 --cabin Economy
.\.venv\Scripts\python.exe -m modules.amybd --airline 2A --origin DAC --destination CGP --date 2026-03-27 --cabin Economy
```

Optional fallback to Gozayaan mode:

```powershell
$env:BS_SOURCE_MODE="gozayaan"
$env:AIRASTRA_SOURCE_MODE="gozayaan"
$env:GOZAYAAN_TOKEN_AUTO_REFRESH="1"
$env:GOZAYAAN_TOKEN_CACHE_FILE="output/manual_sessions/gozayaan_token_latest.json"
$env:GOZAYAAN_COOKIES_PATH="output/manual_sessions/gozayaan_cookies.json"
$env:GOZAYAAN_HEADERS_FILE="output/manual_sessions/gozayaan_headers_latest.json"
$env:GOZAYAAN_TOKEN_REFRESH_CMD='.\.venv\Scripts\python.exe tools/refresh_gozayaan_token.py --out {cache_file} --cookies-out {cookies_file} --headers-out {headers_file} --non-interactive --origin {origin} --destination {destination} --date {date} --cabin {cabin} --adt {adt} --chd {chd} --inf {inf}'
```

Manual override token for Gozayaan emergency fallback:

```powershell
$env:GOZAYAAN_X_KONG_SEGMENT_ID="<fresh token>"
```

If direct airline fallback is needed temporarily:

```powershell
$env:BS_SOURCE_MODE="ttinteractive"
$env:AIRASTRA_SOURCE_MODE="ttinteractive"
```

## Known Constraints

- Jobs do not run when laptop is fully powered off.
- Current-user tasks require user session context.
- Wake-from-sleep works only if OS wake timers are enabled and device is sleeping (not shut down).

---

## New Connector Onboarding Checklist (G9 / OV pattern)

Use this checklist when adding a new airline that requires browser capture (anti-bot/WAF protected).

### Step 1 — Validate capture parsing

For Air Arabia (G9):

```powershell
# Export a HAR file from browser DevTools while browsing airarabia.com
.\.venv\Scripts\python.exe tools\import_airarabia_har.py path\to\www.airarabia.com.har
# Check output in output\manual_sessions\runs\g9_*\
```

For SalamAir (OV):

```powershell
# Option A — Playwright live capture (may be blocked by WAF)
.\.venv\Scripts\python.exe tools\capture_salamair_live.py --origin DAC --destination JED --date 2026-04-20

# Option B — Manual browser intercept (reliable)
.\.venv\Scripts\python.exe tools\capture_salamair_manual.py --origin DAC --destination JED
# Follow prompts: open browser, search flights, wait for intercept

# Option C — HAR file import
.\.venv\Scripts\python.exe tools\import_salamair_har.py path\to\www.salam.aero.har
```

### Step 2 — Inspect capture output

```powershell
# Confirm rows extracted
Get-Content output\manual_sessions\runs\*\*_capture_summary.json | ConvertFrom-Json
Get-Content output\manual_sessions\runs\*\*_rows.json | ConvertFrom-Json | Measure-Object
```

### Step 3 — Set source mode and enable airline

```powershell
# For G9
$env:AIRARABIA_SOURCE_MODE="capture"   # or "sharetrip" for OTA fallback
$env:AIRARABIA_CAPTURE_ROOT="output/manual_sessions"

# For OV
$env:SALAMAIR_SOURCE_MODE="capture_then_browser"
$env:SALAMAIR_CAPTURE_ROOT="output/manual_sessions"
```

In `config/airlines.json`, set `"enabled": true` for the airline.

### Step 4 — Add routes

In `config/routes.json`, add route entries. SalamAir full DAC network:

```
DAC→JED, DAC→MCT, DAC→DXB, DAC→SHJ, DAC→RUH, DAC→KWI, DAC→BAH, DAC→AMM
```

### Step 5 — Test run

```powershell
.\.venv\Scripts\python.exe run_all.py --airline G9 --dry-run
.\.venv\Scripts\python.exe run_all.py --airline OV --dry-run
```

### Step 6 — Smoke check

```powershell
.\.venv\Scripts\python.exe tools\smoke_check.py --airline G9
.\.venv\Scripts\python.exe tools\smoke_check.py --airline OV
```

---

## OV (SalamAir) Operational Notes

SalamAir has two capture modes — choose based on environment:

| Mode | Command | When to use |
|------|---------|------------|
| Playwright live | `capture_salamair_live.py` | When WAF allows automated browser |
| Manual intercept | `capture_salamair_manual.py` | When Playwright is blocked (default/reliable) |
| HAR import | `import_salamair_har.py` | One-off / debugging from saved HAR file |

**Capture staleness**: The module rejects captures older than `SALAMAIR_MAX_CAPTURE_AGE_HOURS`
(default: 8h). If pipeline picks up a stale capture, it will log a warning and skip the route.

**Recommended workflow** until `scheduler/run_capture_sessions.py` is built:

1. Run manual capture 30 min before the main pipeline: `capture_salamair_manual.py --origin DAC --destination MCT`
2. Launch pipeline: `python run_pipeline.py`
3. OV module reads the cached capture — no live search attempted during pipeline run

---

## Pre-Flight Session Check

Before a pipeline run, validate connector/session readiness:

```powershell
.\.venv\Scripts\python.exe tools\pre_flight_session_check.py --dry-run
```

The pipeline runs this preflight by default. Use `--skip-preflight` only for manual debugging, and `--preflight-strict` when blocking findings should fail immediately.

## Extraction Health Gate

Extraction health is separate from process return code. A cycle can complete as a process but still be a bad data cycle.

Key artifacts:

- `output/reports/extraction_health_latest.json`
- `output/reports/extraction_health_latest.md`
- `output/reports/extraction_health_latest.csv`

Useful commands:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --fail-on-extraction-gate --skip-bigquery-sync
.\.venv\Scripts\python.exe run_pipeline.py --retry-missing-airlines
.\.venv\Scripts\python.exe tools\extraction_health_report.py --cycle-id YOUR-CYCLE-ID --expected-airlines BG,VQ,BS,2A
```

Gate interpretation:

- `PASS`: publishable, assuming downstream report/prediction checks pass.
- `WARN`: publishable with review; usually clean no-inventory/zero-row attempts.
- `FAIL`: do not publish to BigQuery. Review `Manual Action Needed` and `Retry Recommended` sections in the health report.

Capture/session controls:

- Default stale-capture limit: `MAX_CAPTURE_AGE_HOURS=8`.
- Per-source overrides: `AIRARABIA_MAX_CAPTURE_AGE_HOURS`, `SALAMAIR_MAX_CAPTURE_AGE_HOURS`, `MALDIVIAN_MAX_CAPTURE_AGE_HOURS`, `GOZAYAAN_MAX_CAPTURE_AGE_HOURS`, `AIRASIA_MAX_CAPTURE_AGE_HOURS`.
- ShareTrip-backed scheduled concurrency defaults to `PARALLEL_SHARETRIP_MAX_WORKERS=1`.
- To temporarily remove any supplier/source from a run, edit `config/source_switches.json` and set that source's `"enabled": false`.
- The source switch file disables primary airline modules and nested fallback suppliers. `SHARETRIP_ENABLED=false` still works as a legacy ShareTrip-only override.

Manual session checks:

```powershell
# AMYBD session check
.\.venv\Scripts\python.exe -m modules.amybd --airline BS --origin DAC --destination CGP --date 2026-04-20 --cabin Economy

# GoZayaan token check
Get-Content output\manual_sessions\gozayaan_token_latest.json
# If token is stale, refresh:
.\.venv\Scripts\python.exe tools\refresh_gozayaan_token.py --out output/manual_sessions/gozayaan_token_latest.json
```

---

## Parallel Execution — Tuning Guide

Current baseline: ~4h31m accumulation. Target: ~1h15m via family-aware parallelism.

### Current safe settings (validated)

```powershell
.\.venv\Scripts\python.exe tools\parallel_airline_runner.py --max-workers 2
```

### Target settings (pending implementation of route-workers flag)

```powershell
# Direct-API family (BG, VQ, Q2, G9) — safe to run 3 routes in parallel
.\.venv\Scripts\python.exe run_all.py --airline BG --route-workers 3

# ShareTrip / OTA families — keep at 1
.\.venv\Scripts\python.exe run_all.py --airline BS --route-workers 1

# GoZayaan — 1 route at a time + inter-query sleep
.\.venv\Scripts\python.exe run_all.py --airline BS --route-workers 1 --gozayaan-inter-query-sleep 3.0
```

### Profiling a run to find bottlenecks

```powershell
.\.venv\Scripts\python.exe run_all.py --profile-runtime
# Output: output\reports\runtime_profile_*.json
```
