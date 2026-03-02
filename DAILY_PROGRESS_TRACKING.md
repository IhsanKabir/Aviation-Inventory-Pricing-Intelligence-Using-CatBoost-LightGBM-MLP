# Daily Progress Tracking (Operational)

Last updated: 2026-02-27

## Current System State

- Automated accumulation in production: `BG`, `VQ`
- Manual-assisted accumulation stack available: `BS`, `2A`, `Q2`
- Main blocker for full automation on `BS`/`2A`: anti-bot / challenge behavior on TTInteractive
- Main blocker for full automation on `Q2`: session/UI-state sensitivity in PLNext flow

## Daily Snapshot Template

Use one entry per day.

```md
## YYYY-MM-DD

- Accumulation state:
  - run_all heartbeat: running/completed/stale
  - active scrape_id:
  - overall query progress:
- Route monitor:
  - latest current_scrape:
  - latest previous_scrape:
  - report path:
- Manual-assisted:
  - BS/2A queue run:
  - Q2 queue run:
  - retry queue generated:
- Data quality:
  - smoke status:
  - top warning/failure:
- Git hygiene:
  - tracked runtime files count:
- Notes:
  - blockers:
  - decisions:
```

## Operator Commands (Daily)

```powershell
# 1) Accumulation heartbeat
Get-Content output\reports\run_all_accumulation_status_latest.json

# 2) Route monitor report cycle log
Get-Content logs\route_monitor_report.log -Tail 80

# 3) Manual-assisted queue runs
Get-ChildItem output\manual_sessions\queue_runs -File | Sort-Object LastWriteTime -Descending | Select-Object -First 10 Name,LastWriteTime

# 4) Git hygiene
.\.venv\Scripts\python.exe tools\check_git_hygiene.py

# 5) Smoke/ops quick check
Get-Content output\reports\smoke_check_latest.json
```

## Weekly Targets

1. Keep BG/VQ automated ingestion stable.
2. Improve BS/2A success ratio under the manual-assisted stack.
3. Keep Q2 capture + ingest repeatable and low-touch.
4. Reduce retry queues by tightening browser/session orchestration.
5. Keep runtime artifacts out of git.

## Definition of Healthy Day

- `run_all_accumulation_status_latest.json` is fresh and progressing.
- No stale route-monitor scrape selection.
- Manual-assisted queue outputs are written with clear success/failure counts.
- `tools/check_git_hygiene.py` reports zero tracked runtime artifacts.
