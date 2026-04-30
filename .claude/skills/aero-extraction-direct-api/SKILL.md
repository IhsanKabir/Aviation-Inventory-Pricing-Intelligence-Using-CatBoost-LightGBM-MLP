---
name: aero-extraction-direct-api
description: Playbook for direct-API airline connectors (BG, VQ, AK, 6E) — GraphQL/JSON/form-POST patterns, JWT refresh handling, header pinning, and parser conventions. Use when adding a new direct connector or debugging schema drift on existing direct sources.
type: project-skill
scope: project
---

# Direct-API Connector Playbook

Direct connectors talk straight to the airline's public booking API. They produce the richest fare data (RBD, fare basis, brand, penalty rules, segment-level inventory) and should always be preferred over OTA wrappers when the API is reachable. Read `aero-extraction-overview` first for the dispatcher contract.

## Connectors covered

| Code | Module | Transport | Auth | Throttle |
|------|--------|-----------|------|----------|
| BG | [biman.py](../../../modules/biman.py) | GraphQL POST | None (Sabre static headers) | 30/min |
| VQ | [novoair.py](../../../modules/novoair.py) | Form POST + HTML parse | Cookie session | 30/min |
| AK | [airasia.py](../../../modules/airasia.py) | JSON POST | JWT refresh | 10/min |
| 6E | [indigo.py](../../../modules/indigo.py) | JSON POST | JWT refresh + captured headers | 10/min |

## Sub-pattern A — stateless GraphQL (BG)

Biman exposes a Sabre-backed GraphQL endpoint. No login, no cookies, no anti-bot.

- Endpoint: `https://booking.biman-airlines.com/api/graphql`
- Operation: `bookingAirSearch`
- Required headers: `x-sabre-storefront: BGDX`, `application-id: SWS1:SBR-GCPDCShpBk:...`
- Variables: standard origin/destination/date/cabin/passenger triple

Response shape lives in [modules/parser.py](../../../modules/parser.py); fare-rule fees in [modules/penalties.py::parse_bg_category16_penalties](../../../modules/penalties.py).

### Why this matters for new airlines

Many smaller airlines use the same Sabre/Amadeus storefront pattern. If you discover a new direct GraphQL endpoint, copy `biman.py` as the template, swap `application-id` and `x-sabre-storefront`, and reuse the parser as-is.

### Failure modes

- Schema drift in `bookingAirSearch` response — surface as a parser test, not a connector change
- Sabre maintenance window — accepts retry; let `modules/requester.py` handle 5xx
- New cabin classes — add to `cabin_classes` in `airlines.json` after verifying availability response

## Sub-pattern B — form-POST + HTML scrape (VQ)

NovoAir uses an ASPX endpoint that returns rendered HTML. No JSON API exists.

- Endpoint: `secure.flynovoair.com/bookings/Vues/flight_selection.aspx?action=flightSearch`
- Form params: `SS, RT, FL, TT, AM, AD, CC, CR` plus split month/day fields
- Cookie session needed: pre-flight to homepage to seed `ASP.NET_SessionId`
- Parser: [modules/novoair_parser.py](../../../modules/novoair_parser.py) — DOM-based; uses BeautifulSoup

### Failure modes

- ASPX form-key rotation — re-scrape homepage to refresh viewstate
- Layout change → parser breaks silently → zero rows. Add a "did we find any flight rows" sanity check before returning `ok=True`.

## Sub-pattern C — JWT refresh-token JSON (AK / 6E)

Both AirAsia and IndiGo expose JSON search APIs but require a refresh-token-backed bearer token. The token is captured manually (Playwright) or refreshed automatically (when `*_TOKEN_REFRESH_ENABLED=true`).

### Files involved

| Source | Search code | Refresh tool | Persisted state |
|--------|-------------|--------------|-----------------|
| AK | [airasia.py](../../../modules/airasia.py) | [refresh_airasia_session.py](../../../tools/refresh_airasia_session.py) | `output/manual_sessions/airasia_session_latest.json` |
| 6E | [indigo.py](../../../modules/indigo.py) | [refresh_indigo_session.py](../../../tools/refresh_indigo_session.py) | `output/manual_sessions/indigo_headers_latest.json`, `indigo_session_latest.json` |

### Token lifecycle

```
capture (Playwright) → refresh_token persisted
   ↓ on each fetch
   if access_token expired or about to expire:
       POST refresh_url with refresh_token
       persist new access_token + cookies
   ↓
   POST search endpoint with Authorization: Bearer <access_token>
```

The refresh path is gated by env var (`AIRASIA_REFRESH_URL`, `INDIGO_TOKEN_REFRESH_ENABLED`). Default is **off** — refresh is opt-in because it occasionally trips the carrier's bot heuristics.

### Source mode env vars

| Var | Values | Effect |
|-----|--------|--------|
| `AIRASIA_SOURCE_MODE` | `auto` / `direct` / `sharetrip` | Force direct or fall to OTA |
| `INDIGO_SOURCE_MODE` | `auto` / `direct` / `sharetrip` | Same — but ShareTrip is disabled, so `auto` effectively means direct-only |

These are set in `.env` not in `airlines.json`. Document any change in PROJECT_DECISIONS.md.

## Parser conventions (apply to all direct sources)

Use [modules/parser.py](../../../modules/parser.py) and [modules/fleet_mapping.py](../../../modules/fleet_mapping.py) helpers:

- `resolve_seat_capacity(equipment_code, airline)` — translates `787-9` / `ATR72` → seat count
- `parse_segments(...)` — handles connections, via-airports, layover times
- `extract_baggage(...)` — normalizes baggage allowance strings to kg
- `parse_bg_category16_penalties(...)` — Biman fare-rule policies
- `_is_bd_domestic(origin, destination)` — Bangladesh-domestic detection for tax differentiation

## Adding a new direct source — checklist

When the user says "let's add airline X with direct API":

1. **Confirm the API is genuinely public** — open dev tools, see if requests work without referrer/cookies. If they fail you're in `aero-extraction-capture-replay` territory.
2. **Choose a template** — Sabre GraphQL (BG), ASPX form (VQ), or JWT refresh (AK/6E). 90%+ of new sources fit one.
3. **Implement `fetch_flights`** — match the contract from `aero-extraction-overview`.
4. **Reuse parsers** — extend `modules/parser.py`; do not create a new parser file unless the wire format is genuinely novel.
5. **Add `check_session()` if session-dependent** — see `aero-extraction-sessions-preflight`.
6. **Register in `config/airlines.json`** with `enabled: true`, `throttle_per_minute` set conservatively (10/min default; raise to 30 only after observation).
7. **Add to `config/source_switches.json`** as `enabled: true`.
8. **Add `fallback_modules`** sparingly — only if a known OTA carries the same airline.
9. **Write a smoke test** — `tools/diagnose_data_sources.py --airline X --route DAC-XXX --date YYYY-MM-DD`.
10. **Update PROJECT_CONTEXT.md** Decision History row.

## Common debugging starts

| Symptom | First thing to check |
|---------|---------------------|
| BG zero rows | Sabre 5xx — check `requester.py` retry logs |
| VQ zero rows | Cookie session — re-hit homepage; viewstate may have rotated |
| AK 401/403 | Token refresh failed — run `refresh_airasia_session.py` |
| 6E 403 | Headers stale — `INDIGO_HEADERS_FILE` needs refresh; UA pinned |
| Any direct: malformed rows | Parser drift — check `tests/test_parsers.py` against latest captured payload |

## Family-aware parallelism (direct only)

Direct-API sources are the **only** family allowed `--route-workers 3`. They are not session-shared with other airlines, and their rate limits (10–30/min) absorb concurrent route fan-out without tripping anti-bot. See [tools/parallel_airline_runner.py](../../../tools/parallel_airline_runner.py) `FAMILY_CONFIG`.

Never raise `--route-workers` for ShareTrip / wrapper / GoZayaan families even temporarily; they share session state and trip blocks at higher concurrency.
