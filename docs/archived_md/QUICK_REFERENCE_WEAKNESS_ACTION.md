# Quick Reference: Current Weaknesses and Actions

Last updated: 2026-02-27

## P0 (Immediate)

### 1) Runtime outputs tracked in git

- Risk: huge commits, noisy history, accidental data leaks.
- Current action:
  - keep runtime paths ignored in `.gitignore`
  - untrack already-tracked runtime files
  - run `tools/check_git_hygiene.py` before commits

Command:

```powershell
.\.venv\Scripts\python.exe tools\check_git_hygiene.py
```

### 2) n8n `executeCommand` unavailable

- Risk: n8n workflow cannot run local batch/python commands.
- Current action:
  - use webhook + local worker pattern (implemented under `tools/`)
  - let n8n trigger HTTP requests only

## P1 (High)

### 3) BS/2A anti-bot instability

- Risk: high 403/DataDome rate; large retry queues.
- Current action:
  - use manual-assisted batch with retry queue outputs
  - run focused route/date windows first (`--limit-dates`)
  - track success rate by queue summary JSON

### 4) Q2 UI/session sensitivity

- Risk: flow depends on exact browser state; can land on `Bad Request`.
- Current action:
  - use `--open-home` default for batch runner
  - preserve/refresh CDP browser profile strategy
  - keep queue retries small and frequent

## P2 (Medium)

### 5) Stale planning docs vs current architecture

- Risk: operator confusion and wrong procedures.
- Current action:
  - keep docs aligned with current state:
    - BG/VQ automated
    - BS/2A/Q2 manual-assisted
    - route-monitor completeness selection rules

## Standard Operating Commands

```powershell
# Build queues from routes + schedule
tools\build_manual_capture_queues.bat --limit-dates 6

# One-command manual-assisted orchestration
tools\run_all_manual_assisted.bat --limit-dates 1 --ingest

# Route monitor one-shot
scheduler\run_route_monitor_report_once.bat

# Git hygiene check
.\.venv\Scripts\python.exe tools\check_git_hygiene.py
```

## Exit Criteria for “Stable Manual-Assisted”

- Retry queue size trending down week-over-week.
- BS/2A/Q2 successful capture+ingest runs reproducible in normal operator flow.
- No tracked runtime artifacts in git.
- Route-monitor uses expected current scrape selection and emits no partial-scrape surprises.
