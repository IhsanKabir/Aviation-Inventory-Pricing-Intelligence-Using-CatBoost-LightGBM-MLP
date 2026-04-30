---
name: aero-extraction-hardening
description: Strategic strengthening of the extraction pipeline — concrete improvements ranked by impact/effort, beyond the documented current state. Use when planning Phase 2 reliability work, when a source family is repeatedly failing, or when proposing pipeline-wide changes. Cross-references existing infrastructure to reuse rather than build new.
type: project-skill
scope: project
---

# Extraction Strategy Hardening

This is the action-oriented complement to the four descriptive skills. It tells you **what to change next** to make extraction more reliable, faster, and cheaper to operate. Every recommendation references existing files — most of the building blocks already exist (see `improvement_roadmap` memory).

Read `aero-extraction-overview` and the family playbooks first. The recommendations here assume you understand the current state.

## Ranking framework

For each candidate, score:

- **Impact** (Low / Med / High): rows recovered, runtime saved, or operator hours avoided
- **Effort** (S / M / L): half-day / 1-3 days / week+
- **Risk** (Low / Med / High): blast radius if misimplemented

Default ordering below is High-Impact / Low-Effort / Low-Risk first.

---

## TIER 1 — Cheap wins worth doing now

### 1.1 Expose `check_session()` on every session-dependent module

**Impact**: High — eliminates entire silent-zero-row failure class
**Effort**: S per module (~2 hours)
**Risk**: Low

Currently only [modules/maldivian.py](../../../modules/maldivian.py) implements it. AMYBD, GoZayaan, AirAsia, IndiGo, BS, 2A, ShareTrip, BDFare should all expose it. See `aero-extraction-sessions-preflight` for the contract.

Order to add:
1. AMYBD (highest silent-failure rate)
2. GoZayaan (rate-limit cooldown is detectable cheaply)
3. AirAsia / IndiGo (JWT TTL)
4. BS / 2A wrappers (probe the TTI bootstrap, detect DataDome)
5. BDFare / ShareTrip (cookie/token presence)

Validation: `python tools/pre_flight_session_check.py --dry-run` should report status for each enabled airline.

### 1.2 Mandatory `no_row_reason` on every `ok=False` return

**Impact**: High — unlocks targeted operator dashboards
**Effort**: S (one-pass edit across modules)
**Risk**: Low

Every `fetch_flights` that returns `ok=False` should set a `no_row_reason` in the response (or a new `_extraction_meta` field consumed by the caller). Vocabulary should be small and stable:

```
capture_missing       — no matching capture found
capture_stale         — capture exceeds MAX_CAPTURE_AGE_HOURS
session_expired       — auth artifact expired
session_missing       — auth artifact never captured
rate_limit_cooldown   — 429 / cooldown window active
anti_bot_challenge    — DataDome / captcha detected
upstream_5xx          — server error after retry
empty_valid_response  — upstream says no flights
parser_failed         — response shape unexpected
unknown               — last resort; actionable bug
```

Surface this in `extraction_attempts.no_row_reason` and group by it in [extraction_health_report.py](../../../tools/extraction_health_report.py).

### 1.3 `GOZAYAAN_INTER_QUERY_SLEEP=3.0` as default

**Impact**: Med — prevents 15-min cooldowns burning a whole cycle
**Effort**: S (env default + docs)
**Risk**: Low (slows GoZayaan; doesn't affect parallel families)

Add to `.env.example`. Document the trade-off in PROJECT_DECISIONS.md.

### 1.4 Capture-summary atomic write

**Impact**: Med — prevents half-written captures becoming the "latest"
**Effort**: S
**Risk**: Low

`tools/import_*_har.py` should write to `<final>.tmp` then `os.replace(tmp, final)`. Same for `refresh_*_session.py`. Currently a crash mid-write produces a half-file that subsequent reads silently treat as valid.

### 1.5 Audit log of every fallback transition per query

**Impact**: Med — root-cause for "why did we end up on AMYBD instead of TTI"
**Effort**: S (already structurally present in `extraction_attempts`)
**Risk**: Low

Add a JSON-array column `fallback_path` to `extraction_attempts` (additive, no destructive migration). Wrappers/dispatcher append `(module, ok, reason)` tuples. Surface in the markdown report.

---

## TIER 2 — Medium investments with strong payoff

### 2.1 Decoupled capture scheduler (`scheduler/run_capture_sessions.py`)

**Impact**: High — unblocks reliable G9/OV automation; cuts manual ops
**Effort**: M (~3 days)
**Risk**: Med

Already in the roadmap. Architecture:

```
T-30 min: scheduler triggers run_capture_sessions.py
            ↓ for each route×date in current cycle plan:
            ↓ check existing capture freshness
            ↓ if stale or missing: capture (Playwright or operator notify)
            ↓ write to runs/<code>_<route>_<date>_<ts>/
T+0:      run_pipeline.py → captures already fresh → fast replay
```

Reuse the existing `tools/capture_*` scripts as worker invocations. Don't reimplement Playwright orchestration; subprocess to existing tools.

### 2.2 Family-aware parallelism expansion in `parallel_airline_runner.py`

**Impact**: High — cuts cycle from ~4h31m → ~1h45m
**Effort**: M (~2 days)
**Risk**: Med (concurrency + family rules can interact)

Current `FAMILY_CONFIG` doesn't include G9/OV. Extend with the recommended worker counts from `aero-extraction-ota-fallback`:

| Family | Workers | Inter-query sleep |
|--------|---------|-------------------|
| direct | 3 | 0 |
| capture-replay | 1 | 0 |
| wrapper | 1 | 1.5s |
| sharetrip | 1 | 3.0s |
| gozayaan | 1 | 3.0s |

Then add `--route-workers N` flag to `run_all.py` that defaults from family config. **Never hardcode > 1 for non-direct families** — see feedback memory.

### 2.3 Health-gate-aware retry pass

**Impact**: Med — recovers cycles that hit transient issues
**Effort**: M
**Risk**: Med (retry storms must be bounded)

`run_pipeline.py --retry-missing-airlines` already exists. Strengthen:
- Only retry airlines with `extraction_attempts.no_row_reason ∈ {upstream_5xx, rate_limit_cooldown}`
- Maximum one retry pass
- Wait window proportional to the largest cooldown observed
- Skip retry if pre-flight FAILed to begin with — fix sessions first

### 2.4 Per-source row-floor guards

**Impact**: Med — surfaces partial extraction failures
**Effort**: S-M
**Risk**: Low

Each source has an empirical minimum row count for a working day (e.g. BG DAC-DXB averages 8-12 offers). When a query falls below 30% of the historical floor for the same route/date/cabin, classify as `WARN` even though `ok=True`. Add to `core/extraction_health.py`. Don't over-tune — start with one float per source-route bucket from the last 30 days.

### 2.5 Single-source capture validation tool

**Impact**: Med — speeds up "is the new HAR good?" loop
**Effort**: S
**Risk**: Low

`tools/validate_capture.py --source <s> --capture-file <path>` that:
- Decodes / parses the response body
- Runs through the source parser
- Reports row count, currencies, brand presence, raw_meta coverage
- Flags suspiciously thin captures before they're used in production

---

## TIER 3 — Larger but strategically important

### 3.1 Operator-in-the-loop lane for BS / 2A / future protected sources

**Impact**: High long-term — eliminates dependence on TTI direct path
**Effort**: L
**Risk**: Med (requires UI / workflow tooling)

The vision (Phase 3+) is Power Automate / n8n agent-driven captures, not headless scraping. Concrete first step:

- `tools/manual_assisted_webhook_worker.py` already exists. Define a **stable contract** for operator-tool → capture-summary → ingestion.
- Build a tiny operator UI (single-page, FastAPI-served) showing: queue of routes needing capture, fresh / stale / missing per route, "open browser to capture" button.
- Capture summaries flow into the same `runs/` layout used by HAR import.

Don't propose this until after Tier 1+2 wins land — it's the largest piece on the board.

### 3.2 Connector-level smoke harness in CI

**Impact**: Med — prevents schema-drift regressions reaching production
**Effort**: M
**Risk**: Low

`tests/connectors/` exists. Add per-source fixture HARs (smallest possible) and replay them through `fetch_flights` weekly in scheduled CI. Catches:
- Parser drift
- Money-format changes
- New cabin codes / brand names not yet mapped

This does not require live network — fixture HARs are committed test artifacts.

### 3.3 Source plan auditor bound to `source_switches`

**Impact**: Med — prevents "ShareTrip disabled, three airlines now zero" surprises
**Effort**: S-M
**Risk**: Low

[tools/audit_airline_source_plan.py](../../../tools/audit_airline_source_plan.py) already audits which sources back which airlines. Strengthen:
- Highlight airlines whose effective chain is **empty** after switches
- Output a `effective_chain_after_switches` column
- Refuse to start `run_pipeline.py` if any enabled airline has an empty effective chain (under `--strict-source-plan`)

### 3.4 Predictive capture refresh

**Impact**: Med — reduces operator burden over time
**Effort**: M
**Risk**: Med (false-positive refreshes waste time)

For sources with telemetry (AMYBD silent expiry rate, GoZayaan 429 hit-rate), schedule pro-active refreshes when the historical "session goes bad" curve crosses 50% probability — not just when staleness exceeds the hard cap. Use existing `extraction_attempts` history.

### 3.5 Schema-validation gate on raw_meta

**Impact**: Med — prevents bad rows leaking into ML training
**Effort**: M
**Risk**: Low

[engines/schema_validator.py](../../../engines/schema_validator.py) exists. Wire it into `run_all.py` post-normalize: validate every row against a per-source schema; rows failing schema get `is_valid=false` flag in `flight_offer_raw_meta`, never deleted (no-delete policy), but excluded from training/aggregation by default.

---

## Anti-recommendations — explicitly do NOT do these

These come up in well-meaning suggestions and must be refused:

| Suggestion | Why no |
|------------|-------|
| "Use Selenium with stealth plugin for BS/2A" | DataDome detects this reliably; tried previously |
| "Increase ShareTrip workers to 2" | ShareTrip rate-limits per-IP; second worker just causes errors |
| "Cache OTA responses across queries by route" | Routes/dates/cabins all matter; false cache hits poison data |
| "Drop old captures to save disk" | No-delete policy; use `tools/retention_cleanup.py` |
| "Just do `git rm` on captures we don't need" | See above |
| "Run Playwright inline in fetch_flights" | Roadmap explicitly puts this off-table; pre-pipeline only |
| "Disable preflight for speed" | Silent zero-rows is the dominant failure class; preflight is the cheapest defense |
| "Force-refresh every session at the start of every cycle" | Refreshes carry their own bot-detection cost; do it only when staleness or telemetry justifies |

---

## Recommended sequencing for a 4-week hardening sprint

| Week | Deliverable |
|------|-------------|
| 1 | Tier 1.1 (`check_session` on AMYBD, GoZayaan, AirAsia, IndiGo) + 1.2 (`no_row_reason` vocabulary) |
| 2 | Tier 1.4-1.5 + Tier 2.4 (atomic captures, fallback path log, row-floor guards) |
| 3 | Tier 2.2 (parallel runner family expansion) + 2.5 (capture validator) |
| 4 | Tier 2.1 (decoupled capture scheduler) — one source first (G9), then OV |

Tier 3 items get separate epics; do not pull them into the same sprint.

## Measurement — how do we know it's working?

After each tier:

1. `output/reports/extraction_health_latest.json` — overall status PASS rate over 7 days
2. Per-source `no_row_reason` distribution — which class is shrinking?
3. Cycle runtime in `output/reports/run_pipeline_summary.json` — trending toward 1h15m
4. Operator-hours spent on manual refresh — track in OPERATIONS_RUNBOOK.md
5. `extraction_attempts.fallback_used` rate per primary source — should trend down for direct sources

If a hardening tier doesn't move at least one of these metrics within 2 cycles, revisit the design.
