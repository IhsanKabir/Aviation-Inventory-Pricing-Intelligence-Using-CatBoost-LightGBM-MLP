---
name: aero-extraction-sessions-preflight
description: Lifecycle of session/token/capture artifacts across all extraction sources — refresh tools, the pre-flight gate, staleness contracts, and the extraction_health gate that protects BigQuery from publishing bad cycles. Use when a source returns silent zero rows, when adding a new session-dependent module, or when changing scheduler timing.
type: project-skill
scope: project
---

# Session Lifecycle, Pre-Flight, and Health Gating

Every Aero Pulse source that depends on a session, token, or capture must answer three questions before each pipeline run:

1. **Is the session/capture present?**
2. **Is it fresh enough?**
3. **Does it actually work right now?**

The pre-flight subsystem and the extraction-health gate together enforce these. Read `aero-extraction-overview` first.

## Components

| Component | Responsibility |
|-----------|----------------|
| `modules/<source>.check_session()` | Source-specific health probe — returns `{ok, reason, expires_at}` |
| [tools/pre_flight_session_check.py](../../../tools/pre_flight_session_check.py) | Discovers and runs every `check_session()`; emits `PASS / WARN / FAIL` |
| `tools/refresh_<source>_session.py` | One per refreshable source — captures fresh state via Playwright |
| [core/extraction_health.py](../../../core/extraction_health.py) | Per-cycle gate logic; computes status from `extraction_attempts` |
| [tools/extraction_health_report.py](../../../tools/extraction_health_report.py) | Regenerates `output/reports/extraction_health_latest.{json,md,csv}` |
| `models/extraction_attempt.py` | One row per query attempt — source, fallback used, no-row reason, capture age |

## The pre-flight contract

`run_pipeline.py` invokes pre-flight automatically:

```powershell
.\.venv\Scripts\python.exe tools\pre_flight_session_check.py --dry-run
```

Returns a structured report. Behavior in `run_pipeline.py`:

- Default: log warnings, continue
- `--preflight-strict`: any FAIL aborts the run
- `--skip-preflight`: skip entirely (only when iterating connector code)

### Status semantics

| Status | Meaning | Pipeline action |
|--------|---------|-----------------|
| `PASS` | Session valid, capture fresh, probe succeeded | Continue normally |
| `WARN` | Session valid but expiring soon, or capture older than 80% of MAX_AGE | Log, continue |
| `FAIL` | Session expired / missing, or probe rejected | Continue with fallback chain only; flag in report |

### `check_session()` contract

Any module exposing this should return:

```python
{
    "ok": bool,                         # True if usable right now
    "reason": str,                      # human-readable for ops
    "expires_at": str | None,           # ISO 8601 or None if not known
    "capture_age_hours": float | None,  # for capture-based sources
    "max_age_hours": float | None,      # configured threshold
}
```

**Reference implementation**: [modules/maldivian.py::check_session](../../../modules/maldivian.py) — probes the bootstrap page, looks for PLNext readiness, looks for reCAPTCHA presence.

**Modules that should expose `check_session` but currently may not**:

- `modules/amybd.py` — silent session expiry is the AMYBD failure mode; this is the most important one
- `modules/gozayaan.py` — token + rate-limit state
- `modules/sharetrip.py` — when re-enabled
- `modules/airasia.py` — JWT TTL probe
- `modules/indigo.py` — header staleness probe

When asked to "harden source X," exposing `check_session` is the first concrete action.

## The extraction-health gate

After every cycle, `run_all.py` writes `extraction_attempts` rows and `core/extraction_health.py` computes a per-airline / overall status. The gate prevents BigQuery sync from publishing a bad cycle:

```
collection complete →
  per-airline: PASS / WARN / FAIL based on row floor and no_row_reason mix
overall: worst per-airline status (with critical-airline weighting)
↓
if overall == FAIL and not --fail-on-extraction-gate:
    skip BigQuery sync, log, continue
elif overall == FAIL and --fail-on-extraction-gate:
    exit 1 (scheduled runs)
```

This is why the cycle can show `process_exit=0` but `bigquery_sync_skipped=true` in `output/reports/run_pipeline_summary.json`. Do not interpret the skip as a bug — see PROJECT_DECISIONS.md (2026-04-27 extraction reliability gate).

## Refresh tool conventions

Every `tools/refresh_<source>_*.py` follows the same skeleton:

```
1. Open Playwright (headed by default for first-run / debugging; --headless for ops)
2. Navigate to the booking surface
3. Wait for the right XHR / token to be observed
4. Extract from network or storage
5. Persist to output/manual_sessions/<source>_*_latest.json
6. Log the new expires_at and write an audit line
```

**Run order at the start of an operational day**:

1. `refresh_gozayaan_token.py` — token + rate-limit state
2. `refresh_amybd_session.py` — captures `authid`/`chauth`
3. `refresh_airasia_session.py` — JWT
4. `refresh_indigo_session.py` — headers
5. (Q2 / G9 / OV — operator runs the matching capture/import tool only when those routes are scheduled)

Then `python tools/pre_flight_session_check.py` to confirm `PASS` for each.

## Staleness across the system — single source of truth

```
.env / config/airlines.json
        ↓
MAX_CAPTURE_AGE_HOURS (global) + per-source overrides
        ↓
core/extraction_health.py reads age via os.path.getmtime
        ↓
modules/<source>.fetch_flights checks age before replay
        ↓
extraction_attempts records age at attempt time
```

When changing a staleness threshold, change **only** the env var. Do not duplicate the constant in module code.

## Operational contract for adding session checks

When the user says "make X harder against silent zero rows":

1. **Expose `check_session()`** in `modules/<x>.py`. Mirror the Maldivian shape.
2. **Wire it into preflight** — `tools/pre_flight_session_check.py` discovers via `getattr(mod, "check_session", None)`. Just dropping the function in is enough; no registration step.
3. **Map likely no-row causes**: capture missing, capture stale, session expired, token TTL expired, rate-limit cooldown, anti-bot challenge.
4. **Return `no_row_reason`** in `fetch_flights` when `ok=False` so the extraction_health report can categorize the failure.
5. **Add a refresh tool** if the source has a recoverable auth artifact.

## Scheduling pre-flight properly

The roadmap calls for a 30-minute pre-flight + capture window before the main pipeline. Until that scheduler exists ([scheduler/run_capture_sessions.py](../../../scheduler/) — planned), the practical setup is:

```
Operational mode:
   T-30 min: refresh_*_session.py (chained)
   T-15 min: tools/pre_flight_session_check.py
   T+0:     run_pipeline.py
```

For pure "rerun once" cycles use `run_pipeline.py --skip-preflight` — but only when the previous cycle's preflight was `PASS` within the last 4 hours.

## Common debugging starts

| Symptom | Run this first |
|---------|----------------|
| Source X has zero rows on every query | `python tools/pre_flight_session_check.py --dry-run --source X` |
| BigQuery skipped sync silently | Look at `output/reports/extraction_health_latest.md` overall status |
| Captures look fresh but module says stale | Check timezone — `os.path.getmtime` is local; `MAX_CAPTURE_AGE_HOURS` is hours |
| Refresh tool hangs | Headless mode masking captcha — re-run with `--headed` |
| AMYBD valid 200 OK but zero offers | The classic — refresh session, then re-run; add `check_session` if it's missing |

## When the user asks "what's the SLA on each source's session?"

Approximate live values to confirm against captures:

| Source | Session lifetime | Refresh cost |
|--------|------------------|--------------|
| AMYBD | ~6h (silent) | Playwright login, ~30s |
| GoZayaan | Token ~2h; rate-limit cooldown 15min on 429 | Playwright capture, ~45s |
| AirAsia | JWT ~1h, refresh-token longer | Token endpoint POST or full Playwright |
| IndiGo | Headers stale within ~4h | Playwright header capture, ~30s |
| Maldivian | reCAPTCHA-bound; manual per-route | Operator capture, several minutes |
| G9 | Per-query HAR; no session per se | Operator HAR export, minute-level |
| OV | Per-query HAR or browser; cookies ~2h | Manual capture or auto-trigger subprocess |

These should be tracked in `extraction_attempts.expires_at` once `check_session()` returns it — that's the canonical place, not this skill.
