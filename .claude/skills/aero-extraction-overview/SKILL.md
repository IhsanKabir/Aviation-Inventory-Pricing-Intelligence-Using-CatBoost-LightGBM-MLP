---
name: aero-extraction-overview
description: Master reference for how Aero Pulse extracts airline fare/inventory data — connector taxonomy, dispatcher contract, fallback chains, and the decision tree for choosing the right strategy when adding or fixing a source. Use this first when touching anything in modules/, run_all.py, or config/airlines.json.
type: project-skill
scope: project
---

# Aero Pulse — Data Extraction Overview

This is the entry point for all extraction work. It defines:
1. The **taxonomy** of source families currently in production.
2. The **connector contract** that `run_all.py` enforces.
3. The **fallback chain** semantics and how `source_switches.json` gates them.
4. The **decision tree** to use when adding or repairing a source.

Read this skill first; then jump to the family-specific playbook skills below.

## Companion skills

| Skill | When to use |
|-------|-------------|
| `aero-extraction-direct-api` | BG / VQ / AK / 6E — direct JSON / GraphQL / form-POST connectors |
| `aero-extraction-capture-replay` | G9 / OV / Q2 — HAR import + Playwright capture lifecycle |
| `aero-extraction-ota-fallback` | BS / 2A wrappers + BDFare / AMYBD / GoZayaan / ShareTrip OTAs |
| `aero-extraction-sessions-preflight` | Session refresh, preflight gating, stale capture handling |
| `aero-extraction-hardening` | Strategic strengthening of the whole extraction pipeline |

## Connector taxonomy (current state)

| Family | Codes | Transport | Auth model | Key risks |
|--------|-------|-----------|------------|-----------|
| Direct-API stateless | BG, VQ | GraphQL / form-POST | None or static headers | Schema drift; no anti-bot |
| Direct-API session | AK, 6E | JSON POST | JWT refresh-token | Token expiry; UA pinning |
| Direct-capture | G9, OV, Q2 | HAR replay + Playwright | Captured cookies/state | Anti-bot WAF; capture staleness |
| TTI wrapper | BS, 2A | Bootstrap HTML + JSON | Sessionized cookies | DataDome blocks; falls to OTAs |
| OTA standalone | (fallbacks) GoZayaan, BDFare, AMYBD, ShareTrip | JSON poll APIs | Token / cookie / RSA-signed | Silent session expiry; rate limits |

Source enable/disable lives in [config/source_switches.json](../../../config/source_switches.json). A source set to `enabled: false` is dropped from both primary and fallback chains at load time. ShareTrip is currently disabled — do not assume it as a safety net.

## The connector contract

`run_all.py` dispatches via `importlib.import_module(f"modules.{module}")` and calls `fetch_flights(...)`. See [run_all.py:2349-2367](../../../run_all.py).

### Required signature

```python
def fetch_flights(
    airline_code: str,
    origin: str,
    destination: str,
    date: str,                 # ISO YYYY-MM-DD outbound
    cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    return_date: str | None = None,
    trip_type: str = "OW",     # "OW" or "RT"
    **kwargs,                  # accept-and-ignore unknown fields — never break dispatch
) -> dict:
    ...
```

### Required return shape

```python
{
    "ok": bool,                    # True only if rows were extracted from this source
    "rows": list[dict],            # normalized offers (see below)
    "raw": dict | None,            # cleaned response payload for raw_meta
    "originalResponse": dict | str # untouched upstream payload for raw_offer_payload_store
}
```

`ok=False` triggers the next entry in `fallback_modules`. Empty `rows` with `ok=True` is treated as a clean miss (sold out) — only return that when you genuinely got a valid empty response.

### Row contract (normalized offer)

Every row inserted into `flight_offers` must carry:

- `airline_code`, `origin`, `destination`, `departure_date`
- `flight_no`, `cabin`, `currency`, `total_fare`
- `booking_class` if available (RBD letter)
- `seats_available` if exposed
- `via` / multi-segment metadata if a connection
- `_raw_meta` dict for `flight_offer_raw_meta` columns (penalties, baggage, fare_basis, brand, etc.)

Use the helpers in [modules/parser.py](../../../modules/parser.py) and [modules/penalties.py](../../../modules/penalties.py) — never hand-roll fare-rule parsing.

### Optional health hooks

If the source has a session, expose:

```python
def check_session() -> dict:
    """Returns {'ok': bool, 'reason': str, 'expires_at': iso8601 | None}."""
```

`tools/pre_flight_session_check.py` discovers these dynamically. Maldivian already implements one ([modules/maldivian.py](../../../modules/maldivian.py)). Any new session-dependent module **must** add one — see `aero-extraction-sessions-preflight`.

## Fallback chain semantics

Configured per-airline in [config/airlines.json](../../../config/airlines.json) under `fallback_modules`. Resolution at load time:

```
primary module → for each fallback in fallback_modules:
                   if source_switches.json[fallback].enabled:
                     append (fallback_module, fetch_flights)
                   else: skip
```

Fallbacks are tried sequentially; the **first** to return `ok=True` wins. When every entry returns `ok=False` the query is recorded as a `FAIL` extraction attempt — not as a process error. This is how [extraction_health_report](../../../tools/extraction_health_report.py) keeps going even when a source is dead.

### Current chains (verify against `airlines.json` — these change)

| Airline | Primary | Effective chain (after switches) |
|---------|---------|-----------------------------------|
| BG | biman | biman → (sharetrip disabled) |
| VQ | novoair | novoair |
| AK | airasia | airasia |
| 6E | indigo | indigo |
| Q2 | maldivian | maldivian |
| G9 | airarabia | airarabia |
| OV | salamair | salamair |
| BS | bs | bs → gozayaan → amybd → bdfare (sharetrip disabled) |
| 2A | airastra | airastra → gozayaan → amybd → bdfare (sharetrip disabled) |

> **Why "verify"**: chain order is set by env-driven overrides inside the `bs`/`airastra` modules (e.g. `BS_AUTO_SOURCE_CHAIN`), not just the JSON. Always cross-check the module before claiming a chain.

## Decision tree — adding or repairing a source

Use this tree when the user says "add airline X" or "X is returning zero rows".

```
Is the airline already represented?
├─ No → New source path:
│   ├─ Public direct API? → aero-extraction-direct-api
│   ├─ Anti-bot WAF? → aero-extraction-capture-replay
│   └─ Otherwise → start as OTA fallback in aero-extraction-ota-fallback
│
└─ Yes → Repair path:
    ├─ Returning zero rows on every query?
    │   ├─ Check source_switches.json (was it disabled?)
    │   ├─ Run pre_flight_session_check.py --dry-run
    │   ├─ Look at extraction_health_latest.md → no_row_reason
    │   └─ Check capture age vs MAX_CAPTURE_AGE_HOURS
    ├─ Returning some rows but malformed?
    │   ├─ Check parser.py / module-specific parser
    │   └─ Validate against models/flight_offer_raw_meta.py columns
    └─ Suddenly slow / 429s?
        └─ aero-extraction-hardening (rate-limit awareness)
```

## Phase / project guardrails (override generic advice)

These are **non-negotiable** in this project — see [feedback memory](../../../../C:/Users/TLL-90134/.claude/projects/c--Users-TLL-90134-Documents-airline-scraper-full-clone/memory/feedback_code_approach.md):

1. **Direct connector beats OTA fallback**. If a direct API is reachable, build a direct module (G9 was promoted from sharetrip → airarabia for this reason). OTA fallback exists only as a safety net.
2. **No-delete storage**. Never propose `DELETE` / `DROP` to recover from bad rows; use compaction (`tools/db_compact_raw_meta.py`) and retention archiving (`tools/retention_cleanup.py`).
3. **Capture before pipeline** for anti-bot sources — never run Playwright inline in `run_all.py`. Use HAR import or a pre-ingestion capture scheduler.
4. **Pre-flight session validation** for OTA/captured sources — silent mid-run expiry produces zero rows with no alert.
5. **Family-aware parallelism** — only direct-API sources get `--route-workers > 1`. ShareTrip / wrapper / GoZayaan stay single-threaded.
6. **Manual-assisted, not headless scraping**, for DataDome-protected BS/2A. Long-term these go through Power Automate / n8n, not Selenium.

## Where extraction state lives on disk

| Path | Purpose |
|------|---------|
| `output/manual_sessions/*_session_latest.json` | Most-recent session blob per source |
| `output/manual_sessions/*_headers_latest.json` | Captured request headers |
| `output/manual_sessions/*_cookies.json` | Captured cookie jars |
| `output/manual_sessions/runs/<code>_<route>_<date>_<ts>/*.har` | HAR snapshots per query |
| `output/manual_sessions/<source>_rate_limit_state.json` | Rate-limit cooldown state |
| `output/reports/extraction_health_latest.{json,md,csv}` | Per-cycle health gate |
| `cache/fleet_capacity_cache.json` | Aircraft seat-capacity cache (24h TTL) |
| `cookies/` | Legacy cookie jar location for some modules |

When a module reports zero rows, **start at `extraction_health_latest.md`** — its `no_row_reason` field tells you which of the above to inspect.

## Glossary (one-liners)

- **cycle_id / scrape_id** — opaque ID grouping all airlines for one parallel snapshot
- **HAR replay** — POST the captured request body again, parse the captured response body
- **Stale capture** — file older than `MAX_CAPTURE_AGE_HOURS` (default 8h), silently rejected
- **Sessionized JSON** — endpoint that requires a prior bootstrap HTML to set cookies before posting JSON
- **Like-for-like** — same passenger mix (ADT=1/CHD=0/INF=0) across rows being compared
- **Family** — group of airlines that share rate-limit / parallelism rules (direct, ShareTrip, wrapper, GoZayaan)
