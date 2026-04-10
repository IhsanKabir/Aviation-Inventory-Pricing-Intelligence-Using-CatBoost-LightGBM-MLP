# Daily Progress Tracking (Operational)

Last updated: 2026-04-09

## Current System State

| Airline | Code | Status | Notes |
|---------|------|--------|-------|
| Biman | BG | Automated | Stable |
| Novoair | VQ | Automated | Stable |
| US Bangla | BS | Manual-assisted | DataDome blocks TTInteractive; OTA fallback chain active |
| Air Astra | 2A | Manual-assisted | Same as BS |
| Maldivian | Q2 | Manual-assisted | Session/UI-state sensitivity in PLNext flow |
| Air Arabia | G9 | Direct (new) | Module upgraded to `airarabia.py`; HAR import workflow; enabled |
| SalamAir | OV | Pending validation | Module `salamair.py` added; **disabled**; routes need expansion |

**Current blockers:**
- BS/2A: TTInteractive protected by DataDome — OTA fallback (BDFare → ShareTrip → GoZayaan) working but fragile
- OV: Playwright blocked by WAF; requires manual HAR import or browser intercept capture
- AMYBD/GoZayaan sessions: expire silently between runs — pre-flight session check needed

**New since 2026-02-27:**
- `modules/airarabia.py` — G9 direct connector (HAR + ShareTrip fallback) — untracked, needs commit
- `modules/salamair.py` — OV direct connector (HAR + Playwright live/manual) — untracked, needs commit
- Capture tools: `capture_salamair_live.py`, `capture_salamair_manual.py` — untracked
- HAR importers: `import_airarabia_har.py`, `import_salamair_har.py` — untracked
- Resume/checkpoint recovery implemented (2026-04-07) — shutdown-test validation pending

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

## Next-Phase Note

- After the current system is stable, semi-automated/manual-fragment sources (BS, 2A, Q2) should move to an AI-agent-assisted operator lane.
- Candidate orchestration tools: Power Automate, n8n, similar workflow/desktop automation tools
- Intended pattern: operator or AI agent completes the challenge-sensitive action → captures normalized structured output → re-enters same downstream ingestion/reporting path
- For anti-bot protected captures (OV, G9): decouple browser capture into `scheduler/run_capture_sessions.py` running 30 min before main ingestion window

## Immediate Next Actions (2026-04-09)

1. Commit all untracked files: `modules/airarabia.py`, `modules/salamair.py`, capture tools, HAR importers, config changes
2. Test G9 direct capture: `python tools/import_airarabia_har.py <har_file>`; then `python run_all.py --airline G9 --dry-run`
3. Expand OV routes in `config/routes.json`: add DAC→MCT, DAC→DXB, DAC→SHJ, DAC→RUH, DAC→KWI, DAC→BAH, DAC→AMM
4. Enable OV in `config/airlines.json` after route validation
5. Validate resume-recovery: run pipeline, kill mid-run, restart, confirm checkpoint resume works
6. Build `tools/pre_flight_session_check.py` for AMYBD + GoZayaan session pre-validation
