---
name: aero-extraction-ota-fallback
description: Playbook for OTA-based extraction (BS/2A wrappers + GoZayaan, BDFare, AMYBD, ShareTrip standalone). Covers wrapper modules, the fallback chain order, OTA-specific quirks (rate limits, session expiry, RSA signing, JWT polling), and when to fail-clean vs retry. Use when working on BS/2A, DataDome incidents, or any OTA-related issue.
type: project-skill
scope: project
---

# OTA Fallback & Wrapper Module Playbook

OTAs (online travel agencies) act as proxy data sources when direct airline endpoints are unreachable or anti-bot-protected. In Aero Pulse they fill three roles:

1. **Wrapper module primary** — for BS / 2A whose direct TTInteractive endpoint is DataDome-blocked, the wrapper module orchestrates a fallback chain through OTAs as part of its own `fetch_flights`.
2. **Per-airline fallback** — listed in `airlines.json` `fallback_modules`. The dispatcher tries them after the primary returns `ok=False`.
3. **Standalone primary** — ShareTrip-only airlines (3L, 8D, EK, FZ, MH, OD, QR, SQ, SV, TG, UL, WY) — currently disabled by `source_switches.json`.

Read `aero-extraction-overview` first. This skill drills into OTA mechanics.

## OTAs covered

| Module | Covers (airlines) | Transport | Auth | Throttle |
|--------|-------------------|-----------|------|----------|
| [bs.py](../../../modules/bs.py) | BS (wrapper) | TTI bootstrap + JSON, falls to OTA chain | Sessionized cookies | 10/min |
| [airastra.py](../../../modules/airastra.py) | 2A (wrapper) | Same as BS | Sessionized cookies | 10/min |
| [gozayaan.py](../../../modules/gozayaan.py) | BS, 2A fallback | JSON poll API | x-kong-segment-id JWT + RSA signing | 10/min |
| [bdfare.py](../../../modules/bdfare.py) | BS, 2A fallback | 3-step JSON poll | Cookie session | 10/min |
| [amybd.py](../../../modules/amybd.py) | BS, 2A fallback | URL-encoded form POST | Captured `authid`/`chauth` | 10/min |
| [sharetrip.py](../../../modules/sharetrip.py) | Many (currently disabled) | Initialize + poll JSON | Bearer token + cookies | 10/min |

## Wrapper module pattern (BS / 2A)

`bs.py` and `airastra.py` are **structurally identical** — both are TTInteractive-bootstrap wrappers that:

1. Call the carrier's `fo-airastra.ttinteractive.com` / equivalent bootstrap URL to get a session.
2. Attempt a sessionized JSON search.
3. On DataDome detection (captcha challenge response), give up the direct attempt **cleanly** (no retries — DataDome will only get more aggressive).
4. Walk the configured OTA chain, returning the first source with `ok=True`.

Configuration env vars (per airline):

| Var | Purpose |
|-----|---------|
| `BS_SOURCE_MODE` / `AIRASTRA_SOURCE_MODE` | `auto`, `direct_only`, `ota_only` |
| `BS_AUTO_SOURCE_CHAIN` / `AIRASTRA_AUTO_SOURCE_CHAIN` | Comma-separated module names overriding default order |
| `BS_BDFARE_FALLBACK_ON_EMPTY` | Treat empty-but-valid TTI response as failure → fall through |

**Default chain**: `gozayaan → amybd → bdfare → sharetrip` (with ShareTrip currently dropped at load time).

The wrapper modules call the OTA modules' `fetch_flights` **directly** in code, not through the `run_all.py` dispatcher. So OTA fetchers must be standalone-callable. This is why every OTA module has its own `fetch_flights(...)` even when it's never registered as a primary in `airlines.json`.

### `_has_usable_rows()` helper

The wrappers gate fallback decisions on this helper — it filters out rows that are normalized but empty / all-soldout / all-zero-price. New OTA additions need similar filtering.

## OTA-specific quirks

### GoZayaan — rate-limit + RSA signing

- **Endpoint**: search → poll legs → poll leg-fares
- **Token**: `x-kong-segment-id` (JWT-like), captured via [tools/refresh_gozayaan_token.py](../../../tools/refresh_gozayaan_token.py)
- **TTL check**: `GOZAYAAN_TOKEN_MIN_TTL_SEC` (default unset; recommend 300s)
- **Auto-refresh**: `GOZAYAAN_TOKEN_REFRESH_CMD` env points to a subprocess command
- **Browser capture**: `GOZAYAAN_BROWSER_CAPTURE_AUTO=true` triggers Playwright if token is missing
- **RSA signing**: when `GOZAYAAN_SIGNING_KEY_FILE` is set, requests are signed before being submitted
- **Rate limit**: 429 triggers a **15-minute cooldown**. State persists in `output/manual_sessions/gozayaan_rate_limit_state.json`
- **Inter-query sleep**: `GOZAYAAN_LEG_POLL_SLEEP_SEC` between leg-fare polls — never reduce below 1.0s

**Recommended ops practice**: Always set `GOZAYAAN_INTER_QUERY_SLEEP=3.0` when running multi-route queries. A single 429 burns the whole cooldown for all remaining queries.

### BDFare — 3-step poll dance

- POST `/Search/AirSearch` → returns `requestId`
- GET `GetAirSearch?requestId=...` → fetches initial offers
- POST `RefreshAirSearch?requestId=...` → polls until results stabilize

Env vars: `BDFARE_MAX_POLLS`, `BDFARE_POLL_SLEEP_SEC`. Aborting before stabilization gives partial results — leave the defaults alone unless investigating a specific issue.

Money parsing uses regex `[-+]?\d[\d,]*(?:\.\d+)?` — currency strings sometimes embed locale separators that break naïve `float()`.

### AMYBD — captured form-POST + silent session expiry

- **Endpoint**: `https://www.amybd.com/atapi.aspx` with URL-encoded `CMND=...` payload
- **CMND values**: `_FLIGHTSEARCH_`, `_FLIGHTSEARCHOPEN_` — these are tokenized request types
- **Auth fields**: `AMYBD_TOKEN`, `AMYBD_AUTHID`, `AMYBD_CAUTH` — all captured
- **Auto-refresh**: `AMYBD_SESSION_REFRESH_CMD` invokes [tools/refresh_amybd_session.py](../../../tools/refresh_amybd_session.py)
- **Refresh timeout**: `AMYBD_SESSION_REFRESH_TIMEOUT_SEC`
- **Session storage**: `output/manual_sessions/amybd_session_latest.json`, `amybd_headers_latest.json`, `amybd_cookies.json`

**Critical operational note**: AMYBD sessions expire **silently** with valid-looking 200 responses that have no offers. This is the classic zero-rows-no-error class of bug. Always run the preflight before relying on AMYBD as a fallback.

### ShareTrip — currently disabled

`source_switches.json` has `sharetrip.enabled=false`. Verify with:

```powershell
$env:SHARETRIP_ENABLED="false"
.\.venv\Scripts\python.exe tools\audit_airline_source_plan.py
```

When ShareTrip is disabled, the loader strips it from all chains. **All ShareTrip-only airlines (3L, 8D, EK, FZ, MH, OD, QR, SQ, SV, TG, UL, WY) collect zero rows**. This is by design (verified 2026-04-27) — when it's safe to re-enable, flip the switch and re-run preflight.

When re-enabled, ShareTrip uses initialize-then-poll with adaptive early-stop:

| Var | Purpose |
|-----|---------|
| `SHARETRIP_POLL_MAX_ATTEMPTS` | Hard ceiling on polls |
| `SHARETRIP_POLL_SLEEP_SEC` | Between-poll wait |
| `SHARETRIP_ADAPTIVE_POLL_STOP` | Stop early if results stable |
| `SHARETRIP_EARLY_STOP_MIN_PROGRESS` | Threshold for "stable" |
| `SHARETRIP_MULTI_PAGE_STABLE_POLLS` | Polls required to confirm stability |
| `SHARETRIP_BDFARE_AIRLINES` | Comma list — for these, fall to BDFare on ShareTrip miss |

## Family-aware parallelism rules

These are non-negotiable (see feedback memory). Setting concurrency above the limit triggers blocks:

| Family | Max workers | Inter-query sleep |
|--------|-------------|-------------------|
| Direct (BG/VQ/AK/6E) | 3 | 0 |
| Capture-replay (G9/OV/Q2) | 1 | 0 (replay is fast) |
| Wrapper (BS/2A) | 1 | 1.5s |
| ShareTrip (when enabled) | 1 | 3.0s |
| GoZayaan | 1 | 3.0s |

Family configuration lives in [tools/parallel_airline_runner.py](../../../tools/parallel_airline_runner.py) `FAMILY_CONFIG`.

## Adding a new OTA

When the user says "we have access to OTA Z, add it as a fallback":

1. Implement `modules/<ota>.py` with the same `fetch_flights` signature.
2. Add `check_session()` if the OTA has a session/token (and it almost always does).
3. Add to `config/source_switches.json` with `enabled: true` and the airline list it backs.
4. Add session refresh tool: `tools/refresh_<ota>_session.py` if applicable.
5. Add to wrapper chain (BS/2A) by editing `BS_AUTO_SOURCE_CHAIN` env example in `.env.example` — chain order matters; put fastest/most-reliable first.
6. Register in [tools/pre_flight_session_check.py](../../../tools/pre_flight_session_check.py).
7. Smoke test: `python tools/diagnose_data_sources.py --source <ota> --route DAC-XXX --date YYYY-MM-DD`.
8. Add a row to `tools/audit_airline_source_plan.py` so it surfaces in source plan audits.

## When to fail-clean vs retry

OTA failures fall into two classes. Retry behavior must match:

| Class | Examples | Action |
|-------|----------|--------|
| Transient | 5xx, network timeout, brief 429 with no cooldown header | Retry once via `modules/requester.py` |
| Persistent | DataDome challenge, 429 with cooldown, expired session | **Fail clean** — return `ok=False`, never retry inline |

Persistent failures retried inline turn into IP bans / longer cooldowns. The dispatcher will move on to the next fallback; the next pipeline cycle picks up after captures/sessions refresh.

## Common debugging starts

| Symptom | First thing to check |
|---------|---------------------|
| BS/2A every query falls all the way through chain | DataDome on TTI + every OTA session expired → run preflight |
| GoZayaan zero rows after first 5 routes | Hit 429 cooldown — check `gozayaan_rate_limit_state.json` |
| AMYBD 200 OK but zero offers | Silent session expiry — refresh session, re-run |
| BDFare slow / hangs | Stuck in poll loop — check `BDFARE_MAX_POLLS` |
| ShareTrip-only airline zero rows | Confirm `source_switches.json` `sharetrip.enabled` — currently false |
