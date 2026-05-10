# Round-Trip Architecture

Last updated: 2026-03-09

## Objective

Add round-trip support without breaking the existing one-way fact model.

## Decision

One-way flight observations remain the canonical fact layer. Round-trip support is added as search-intent and itinerary-link metadata around those rows.

## Current Delivery

This first architecture pass adds:

- shared trip request normalization in [`core/trip_context.py`](../core/trip_context.py)
- `run_all.py` CLI support for `--trip-type` and `--return-date`
- persisted trip metadata in `flight_offer_raw_meta`
- backward-compatible connector kwargs for round-trip adoption
- first live connector path in [`modules/biman.py`](../modules/biman.py)

The current reporting/UI pass adds:

- route-monitor API exposure for trip metadata
- route-page filters for `OW` / `RT`
- one explicit `return_date`
- return-date range filtering via `return_date_start` / `return_date_end`
- grouped outbound/inbound route shells in the web monitor

The current planning split adds:

- `operational` mode:
  - only active comparison-safe route profiles are used
  - intended for core monitoring and cycle-to-cycle comparison
- `training` mode:
  - the fuller candidate market-trip profile set is used
  - intended for forecasting/training enrichment, including holiday overlays
  - may also add training-only inventory anchor profiles so the same departure horizon can be observed repeatedly over time

## Data Model

Round-trip context is stored in raw meta, not in the core `flight_offers` identity table.

Fields:

- `search_trip_type`: `OW` or `RT`
- `trip_request_id`: stable request fingerprint for outbound/inbound pairing
- `requested_outbound_date`
- `requested_return_date`
- `trip_duration_days`
- `trip_origin`
- `trip_destination`
- `leg_direction`: `outbound` or `inbound`
- `leg_sequence`: `1` or `2`
- `itinerary_leg_count`

## Why Raw Meta First

- the current comparison engine is built around one-way snapshot identity
- forcing round-trip identity into `flight_offers` now would destabilize current change detection
- raw meta preserves trip intent immediately while keeping the current route/flight comparison model intact

## Connector Adoption Rule

Connectors may adopt round-trip support in three levels:

1. Accept kwargs and ignore them
2. Accept kwargs and preserve trip metadata on returned rows
3. Perform true round-trip search and emit outbound/inbound rows with `leg_direction`

Current state:

- `Biman`: level 3 payload support through two `itineraryParts`
- other connectors: backward-compatible through `run_all.py` keyword fallback, but still effectively one-way until explicitly upgraded

## Web/API Implications

Implemented now:

- trip-type filter in search/reporting contracts
- route-monitor payload metadata for paired trip display
- grouped outbound/inbound route shells on Routes

Still next:

- itinerary-level export tabs
- itinerary-level ranking and pricing comparison
- warehouse-side round-trip exposure for hosted-only reads

## Search Configuration

Round-trip search dates are configured at collection time in `run_all.py`.

Outbound date selectors:

- `--date`
- `--dates`
- `--date-start` / `--date-end`
- `--date-offsets`
- `day_offset_start` / `day_offset_end`
- `day_offset_range`
- `day_offset_ranges`
- `config/dates.json`

Return-date selectors:

- `--return-date`
- `--return-dates`
- `--return-date-start` / `--return-date-end`
- `--return-date-offsets`
- `--return-date-offset-start` / `--return-date-offset-end`
- `config/dates.json`
- `config/route_trip_windows.json`

Default outbound behavior:

- if no explicit date arguments are passed, `run_all.py` now uses [`config/dates.json`](../config/dates.json)
- the default repo seed is `day_offsets: [0, 3, 5, 7, 15]`
- the old hardcoded `30` day fallback has been removed from the normal non-quick default

Supported patterns:

1. Single outbound + single return:

```powershell
.\.venv\Scripts\python.exe run_all.py --airline BG --origin DAC --destination CXB --date 2026-03-12 --trip-type RT --return-date 2026-03-15
```

2. Multiple outbound dates + one shared return-date range:

```powershell
.\.venv\Scripts\python.exe run_all.py --airline BG --origin DAC --destination CXB --dates 2026-03-12,2026-03-13 --trip-type RT --return-date-start 2026-03-15 --return-date-end 2026-03-18
```

3. Offset-based return logic from each outbound date:

```powershell
.\.venv\Scripts\python.exe run_all.py --airline BG --origin DAC --destination CXB --date-start 2026-03-12 --date-end 2026-03-14 --trip-type RT --return-date-offsets 2,3,5
```

4. Offset range from each outbound date:

```powershell
.\.venv\Scripts\python.exe run_all.py --airline BG --origin DAC --destination CXB --date-start 2026-03-12 --date-end 2026-03-14 --trip-type RT --return-date-offset-start 2 --return-date-offset-end 5
```

5. Config-file driven return selectors in `config/dates.json`:

```json
{
  "dates": ["2026-03-12", "2026-03-13"],
  "return_date_offsets": [2, 3, 5]
}
```

Or absolute return-date ranges:

```json
{
  "date_start": "2026-03-12",
  "date_end": "2026-03-14",
  "return_date_ranges": [
    { "start": "2026-03-15", "end": "2026-03-17" },
    { "start": "2026-03-20", "end": "2026-03-22" }
  ]
}
```

## Route-Wise Trip Configuration

If you want different trip behavior per route, keep that in [`config/route_trip_windows.json`](../config/route_trip_windows.json).

Purpose:

- keep the route universe in [`config/routes.json`](../config/routes.json)
- keep global outbound-date defaults in `config/dates.json`
- keep route-specific `OW` / `RT` and return-date logic in one separate file

Preferred structure:

- `profiles` for reusable trip policies
- `market_trip_profile` when you want a route or airline block to inherit generic date logic from [`config/market_priors.json`](../config/market_priors.json)
- `airlines` for grouped airline blocks
- `default_profile` inside each airline block
- `routes` map inside each airline block for route-specific overrides

Backward compatibility:

- the loader still accepts the older flat `routes` list
- the preferred human-editable shape is now grouped `airlines`

Supported entry fields:

- `airline`
- `origin` and `destination`
- or `route` as `DAC-CXB`
- `profile`
- `market_trip_profile`
- `market_trip_profiles`
- `active_market_trip_profile`
- `active_market_trip_profiles`
- `training_market_trip_profile`
- `training_market_trip_profiles`
- `trip_type`
- `dates`, `date_start` / `date_end`, `date_ranges`, `day_offsets`
- `day_offset_start` / `day_offset_end`
- `day_offset_range`
- `day_offset_ranges`
- `return_date`
- `return_dates`
- `return_date_start` / `return_date_end`
- `return_date_ranges`
- `return_date_offsets`
- `return_date_offset_start` / `return_date_offset_end`

Example:

```json
{
  "profiles": {
    "bg_domestic_rt": {
      "trip_type": "RT",
      "return_date_offsets": [1, 2, 3]
    },
    "ow_default": {
      "trip_type": "OW"
    }
  },
  "airlines": {
    "BG": {
      "default_profile": "ow_default",
      "routes": {
        "DAC-CXB": {
          "profile": "bg_domestic_rt"
        },
        "DAC-CGP": {
          "profile": "bg_domestic_rt",
          "return_date_start": "2026-03-15",
          "return_date_end": "2026-03-18"
        }
      }
    },
    "VQ": {
      "default_profile": "ow_default",
      "routes": {
        "DAC-SPD": {}
      }
    }
  }
}
```

Behavior:

- airline `default_profile` applies to every route in that airline block unless the route overrides it
- `market_trip_profile` lets you pull generic outbound / return-date ideas from [`config/market_priors.json`](../config/market_priors.json) and then override them locally
- [`config/market_priors.json`](../config/market_priors.json) can now hold:
  - reusable `trip_date_profiles`
  - country-level `holiday_windows` with multiple date ranges
- route-level config overrides the global trip mode for that route
- named `profile` values let you reuse one rule set across many routes
- route-level outbound dates override the global outbound dates for that route
- route-level return selectors override the global return selectors for that route
- if a route is forced to `OW`, return selectors are ignored for that route
- if no route override matches, `run_all.py` uses the normal global CLI / `config/dates.json` settings

Configuration precedence:

1. explicit CLI date / trip arguments
2. [`config/route_trip_windows.json`](../config/route_trip_windows.json) route-level fields
3. `profile` from [`config/route_trip_windows.json`](../config/route_trip_windows.json)
4. `market_trip_profile` from [`config/market_priors.json`](../config/market_priors.json)
5. [`config/dates.json`](../config/dates.json) global outbound defaults

Planning modes:

- `operational`
  - respects `active_market_trip_profiles` where defined
  - keeps comparison cycles cleaner and runtime lower
- `training`
  - expands to the fuller candidate `market_trip_profiles`
  - this is where holiday overlays, richer return windows, and inventory-anchor tracking belong

Training-only inventory anchor tracking:

- use `training_market_trip_profile` or `training_market_trip_profiles` in [`config/route_trip_windows.json`](../config/route_trip_windows.json)
- these profiles are ignored in `operational` mode
- they are appended only in `training` mode
- intended use:
  - fix a departure horizon like `+7 days`
  - observe the same departure date repeatedly over time
  - learn inventory fill / reopen speed and how fare moves alongside inventory

Future-proof decision:

- [`config/dates.json`](../config/dates.json) is the default date engine
- [`config/market_priors.json`](../config/market_priors.json) is the reusable business-template and holiday layer
- [`config/route_trip_windows.json`](../config/route_trip_windows.json) is the only explicit route activation layer
- business templates do not auto-activate routes silently; routes are assigned intentionally in [`config/route_trip_windows.json`](../config/route_trip_windows.json)

Matching rule:

- exact `airline + origin + destination` match wins first
- if no airline-specific row matches, a row without `airline` acts as a wildcard for that route

CLI:

```powershell
.\.venv\Scripts\python.exe run_all.py --route-trip-config config/route_trip_windows.json
```

## Next Implementation Steps

1. Upgrade OTA connectors with explicit round-trip payload builders.
2. Upgrade OTA connectors with explicit round-trip payload builders.
3. Add itinerary-level export and ranking views.
4. Add itinerary-level ranking and forecasting on top of linked legs.

---

> **Codex will review your output once you are done.**
