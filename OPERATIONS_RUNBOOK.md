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

## Exact Verification Commands

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
```

Then confirm:
- Startup shortcut exists:
  - `%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\AirlineIntel AlwaysOn.lnk`
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

## Runtime Profiling / Safe Parallel Scrape

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

## Local CI Guard (Every Commit)

Install pre-commit hook once:

```powershell
powershell -ExecutionPolicy Bypass -File tools\install_git_hooks.ps1
```

Manual CI check run:

```powershell
.\.venv\Scripts\python.exe tools\ci_checks.py --reports-dir output\reports --timestamp-tz local
```

## Known Constraints

- Jobs do not run when laptop is fully powered off.
- Current-user tasks require user session context.
- Wake-from-sleep works only if OS wake timers are enabled and device is sleeping (not shut down).
