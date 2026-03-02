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
9. Confirm scheduled task entries are still present:
   - `AirlineIntel_DailyOps`
   - `AirlineIntel_WeeklyPack`
   - `AirlineIntel_MaintenancePulse`
   - `AirlineIntel_Ingestion4H`
   - `AirlineIntel_IngestionOnLogon`
   - Current default ingestion cadence is every 6 hours (`RepeatMinutes=360`).

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
- Pulse task repeats every 30 minutes.

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

Active recovery run (executes targeted scrapes):

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
- `output/reports/scrape_parallel_latest.json`

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
- `generate_route_flight_fare_monitor.py` now warns if current/previous compared scrapes have mismatched passenger mix (`ADT/CHD/INF`).
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
