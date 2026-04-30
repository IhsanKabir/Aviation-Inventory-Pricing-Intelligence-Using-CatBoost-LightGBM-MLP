---
name: aero-extraction-capture-replay
description: Playbook for anti-bot/WAF airlines extracted via HAR import + Playwright capture (G9, OV, Q2). Covers capture lifecycle, staleness, manual vs automated capture flows, and integration with the dispatcher. Use when adding capture-based sources or debugging capture-related zero-row failures.
type: project-skill
scope: project
---

# Capture / Replay Connector Playbook

For airlines whose direct endpoint is protected by WAF / anti-bot (DataDome, reCAPTCHA, Cloudflare bot mode, custom challenge pages), Aero Pulse uses a **decoupled capture** model: a browser-driven session is captured *before* the pipeline runs, and the connector replays the captured request against the captured response or refreshes it minimally with the captured cookies.

This skill covers the three capture-replay sources currently in production. Read `aero-extraction-overview` first.

## Connectors covered

| Code | Module | Capture mechanism | Why |
|------|--------|-------------------|-----|
| G9 | [airarabia.py](../../../modules/airarabia.py) | HAR replay → fallback to ShareTrip | Session-pinned protected endpoint |
| OV | [salamair.py](../../../modules/salamair.py) | HAR replay or live Playwright capture | WAF blocks headless / public CAPTCHA |
| Q2 | [maldivian.py](../../../modules/maldivian.py) | HAR replay only (PLNext bootstrap) | reCAPTCHA blocks live; no public API |

## The capture-replay invariant

```
            ┌───────────────────┐
            │   Pre-pipeline    │
            │   capture step    │  ← Playwright or manual browser
            └─────────┬─────────┘
                      │ writes cookies + body to
                      ▼
   output/manual_sessions/<source>_*_latest.json
                      │
            ┌─────────┴─────────┐
            │  fetch_flights()  │  ← run_all.py inline, fast, no browser
            └───────────────────┘
                      │
                      ▼
                normalized rows
```

**The connector NEVER spawns a browser inline by default.** Captures must be fresh enough to replay. If the capture is stale, the connector reports `ok=False` with `no_row_reason="capture_stale"`, the dispatcher tries the next fallback, and the next pipeline run picks up the refreshed capture.

## Staleness gating

All capture-replay sources honor `MAX_CAPTURE_AGE_HOURS` (default **8h**), with per-source overrides via env. See [tools/pre_flight_session_check.py](../../../tools/pre_flight_session_check.py).

| Var | Default | Purpose |
|-----|---------|---------|
| `MAX_CAPTURE_AGE_HOURS` | 8 | Global cap |
| `AIRARABIA_MAX_CAPTURE_AGE_HOURS` | (override) | G9 |
| `SALAMAIR_MAX_CAPTURE_AGE_HOURS` | (override) | OV |
| `MALDIVIAN_MAX_CAPTURE_AGE_HOURS` | (override) | Q2 |
| `GOZAYAAN_MAX_CAPTURE_AGE_HOURS` | (override) | GoZayaan token |
| `AIRASIA_MAX_CAPTURE_AGE_HOURS` | (override) | AK |

Stale captures are **silently rejected** (logged, not error). This is a deliberate design choice — see PROJECT_DECISIONS.md (2026-04-27 extraction reliability telemetry entry).

## Per-source workflows

### G9 — Air Arabia

**Mode env**: `AIRARABIA_SOURCE_MODE` ∈ {`capture_then_browser`, `har_only`, `sharetrip_only`}

**Workflow**:
1. Operator opens airarabia.com, performs target search, exports HAR.
2. `python tools/import_airarabia_har.py <har_file>` → produces `output/manual_sessions/runs/g9_<route>_<date>_<ts>/airarabia_capture_summary.json`.
3. On pipeline run, [airarabia.py](../../../modules/airarabia.py) discovers the most recent capture matching `g9_<origin>_<dest>_<date>_*.json`, decodes the base64 response body, and normalizes through the parser.
4. Currency conversion: `_FX_TO_BDT` table inside the module — keep this in sync when AED/USD rates drift materially.

**Failure modes**:
- No matching capture for query → `no_row_reason="capture_missing"`, falls to ShareTrip (currently disabled, so the query becomes `FAIL`)
- Capture older than 8h → `capture_stale`, same fallback path
- HAR has multiple search responses → connector picks the one whose request body matches the query; ambiguous matches log a warning

### OV — SalamAir

**Mode env**: `SALAMAIR_SOURCE_MODE` ∈ {`capture_then_browser`, `browser`, `har_only`}

**Workflow**:
- Manual route: `python tools/capture_salamair_manual.py` opens an instrumented browser and waits for the operator to perform the search.
- Automated route: `python tools/capture_salamair_live.py` runs Playwright headlessly (only viable when WAF allows the workstation IP).
- HAR import route: `python tools/import_salamair_har.py <har_file>` for purely offline playback.

The connector accepts a `SALAMAIR_BROWSER_CAPTURE_CMD` env var to **auto-trigger a capture** when the most recent capture is stale or missing. This is the only place in the pipeline where browser invocation is permitted from the inline path — and it runs as a subprocess with a strict timeout to avoid blocking the cycle.

**Rate-limit state**: `SALAMAIR_RATE_LIMIT_STATE_FILE` records cooldown windows after upstream throttling.

### Q2 — Maldivian (PLNext)

**Live not implemented.** PLNext requires a live browser session passing reCAPTCHA. The current connector replays HAR captures only.

**Workflow**:
1. Operator runs `python tools/maldivian_manual_ingest.py` — opens browser, completes search + reCAPTCHA, exports artifacts.
2. `python tools/import_maldivian_har.py <har_file>` parses the search response.
3. [maldivian.py](../../../modules/maldivian.py) replays from the captured `FARE_SOURCE_ENDPOINT: AjaxCall.action?UID=FARE` body.

**Health hook**: `check_session()` exists and probes the bootstrap page for PLNext readiness / reCAPTCHA presence. This is the canonical example for new captured-session modules.

**Quirk — PLNEXT_FORM_DEFAULTS**: bootstrap page issues `LANGUAGE`, `OFFICE_ID`, etc. that must be replayed verbatim. The constants live in the module; do **not** invent new ones.

## Capture file layout (canonical)

```
output/manual_sessions/
├── runs/
│   ├── g9_DAC_SHJ_2026-05-01_20260430T120000Z/
│   │   ├── search.har
│   │   └── airarabia_capture_summary.json
│   ├── ov_DAC_JED_2026-05-01_20260430T120000Z/
│   │   └── salamair_capture_summary.json
│   └── q2_DAC_MLE_2026-05-01_20260430T120000Z/
│       └── maldivian_capture_summary.json
├── <source>_session_latest.json    # most recent session blob
├── <source>_headers_latest.json    # most recent header snapshot
└── <source>_cookies.json           # cookie jar
```

Each `*_capture_summary.json` is the canonical replay artifact. The `runs/` subdirectory is append-only — no-delete policy applies to captures too.

## When the user says "live capture for OV / Q2"

Push back. Live capture during pipeline run was explicitly ruled out — see feedback memory ("Capture-before-pipeline for anti-bot airlines"). The right answer is one of:

1. **Decoupled capture scheduler** — `scheduler/run_capture_sessions.py` (planned in roadmap; not yet built). Runs ~30 min before main pipeline, captures all stale-bound queries, writes to `runs/`. The pipeline then sees fresh captures.
2. **Manual operator workflow** — operator runs the capture tool 1×/day during morning pre-flight. This is the current practice for Q2 and the manual mode for OV.
3. **Power Automate / n8n agent lane** — Phase 3+ vision; do not propose now.

## When the user says "this airline is going behind a CAPTCHA"

You're moving from direct-API → capture-replay. Expected work:

1. Add HAR import tool: `tools/import_<airline>_har.py`. Use `import_airarabia_har.py` as the template.
2. Add capture summary builder if response shape needs custom normalization.
3. Replace direct connector body with capture-discovery + replay logic. Keep the same `fetch_flights` signature.
4. Set per-source `MAX_CAPTURE_AGE_HOURS` env override (default to 12 for non-real-time-sensitive routes; 6 for active hot routes).
5. Update `aero-extraction-overview` taxonomy table.

## Anti-patterns to refuse

- **Inline Playwright in `fetch_flights`** — except for the SalamAir auto-trigger subprocess pattern, this is forbidden. It blocks the pipeline and trips bot heuristics fast.
- **Caching capture across queries with different routes/dates** — captures are query-specific. A DAC-DXB capture is not a substitute for a DAC-SHJ capture.
- **Falling back to a direct-API code path when capture is stale** — that's how anti-bot airlines start IP-banning workstations.
- **"Just headless Chromium with stealth plugin"** — has been tried and trips DataDome reliably. Operator-in-the-loop is the long-term plan for BS/2A.

## Smoke test for capture-replay sources

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --skip-reports --skip-prediction --skip-bigquery-sync --limit-routes 1 --limit-dates 1
```

Then inspect `output/reports/extraction_health_latest.md` for the source. Look for `final_source` and `capture_age_hours` columns.
