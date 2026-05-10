# Trip Profile Guide

This guide explains where trip-search behavior is defined and where it is activated.

## File Roles

`dates.json`
- Global default outbound date engine.
- Use this for default day offsets, default date ranges, and other broad outbound-date seeds.

`market_priors.json`
- Reusable business templates.
- This file defines profile behavior, but does not activate routes by itself.

`route_trip_windows.json`
- Actual route-by-route execution control.
- This is the file you edit to turn profiles on or off for an airline and route.

## Activation Model

For each route, these keys matter:

`market_trip_profiles`
- All candidate profiles available for that route.
- Think of this as the menu of possible behaviors.

`active_market_trip_profiles`
- Profiles that are ON for `operational` runs.
- This is the main on/off switch for live collection behavior.
- Important: this list only filters the profiles already present in `market_trip_profiles`.
- It does not activate a profile that is missing from `market_trip_profiles`.
- Practical rule: if you want a profile to run in `operational`, put it in both lists.

`training_market_trip_profiles`
- Extra profiles used only in `training` mode.
- These do not affect normal operational collection unless also included in `active_market_trip_profiles`.
- `training` now means the daily core enrichment pass:
  - operationally active profiles still apply
  - training-only profiles are added on top
- Use `training_market_trip_profiles` to make especially important daily enrichment layers explicit.
- In the current setup, major market-movement layers are also declared explicitly in training so the config remains auditable:
  - domestic Eid overlays
  - worker outbound / return movement
  - regional RT movement
  - tourism RT / tourism `+7` OW extension
  - hub-spoke / long-haul RT movement
  - inventory-anchor tracking

`deep_market_trip_profiles`
- Extra profiles used only in `deep` mode.
- `deep` is the broad weekly/opportunistic enrichment pass:
  - it starts from the broader route-level `market_trip_profiles` candidate set
  - then adds `training_market_trip_profiles`
  - then adds `deep_market_trip_profiles`
- Use this key for the heaviest or rarest enrichment patterns that should not run in daily core training.

## Common Profile Meanings

`default_one_way_monitoring`
- Normal one-way monitoring.
- Current default outbound offsets: `0, 3, 5, 15, 30`

`bangladesh_domestic_round_trip_short`
- Normal Bangladesh domestic round-trip behavior.
- Current return offset: `+7 days`

`bangladesh_domestic_eid_round_trip_2026`
- Eid-focused domestic round-trip window.
- Uses exact outbound and return date ranges around Eid.

`bangladesh_domestic_eid_capital_outbound_one_way_2026`
- One-way Dhaka to domestic flows before Eid.

`bangladesh_domestic_eid_capital_return_one_way_2026`
- One-way domestic to Dhaka flows after Eid.

`regional_round_trip_flexible`
- Short and medium regional return windows.

`worker_visa_outbound_to_middle_east_one_way`
- One-way worker/visa travel from South Asia to the Middle East.

`worker_return_from_middle_east_long_window`
- Long-window return behavior from the Middle East back to South Asia.

`hub_spoke_or_longhaul_return_window`
- Wider return windows for hub-spoke and long-haul routes.

`tourism_bkk_can_round_trip`
- Tourism-oriented return behavior for Bangkok and Guangzhou style routes.

`inventory_anchor_departure_tracking_default`
- Training-only inventory anchor profile.
- Used to repeatedly observe the same departure horizon for inventory movement analysis.

`tourism_mle_bkk_kul_one_way_plus7`
- Extra one-way tourism pass used on `MLE`, `BKK`, and `KUL` routes.
- Adds the `+7 day` departure horizon on top of the normal one-way baseline.

## Current Bangladesh Domestic Baseline

For the current Bangladesh domestic baseline, the intended operational setup is:
- airlines: `BG`, `2A`, `BS`, `VQ`
- one-way baseline: `default_one_way_monitoring`
- optional layered domestic round-trip baseline: `bangladesh_domestic_round_trip_short`

For those airlines, the currently configured DAC-linked domestic routes are:
- `DAC-BZL`, `BZL-DAC`
- `DAC-CGP`, `CGP-DAC`
- `DAC-CXB`, `CXB-DAC`
- `DAC-JSR`, `JSR-DAC`
- `DAC-RJH`, `RJH-DAC`
- `DAC-SPD`, `SPD-DAC`
- `DAC-ZYL`, `ZYL-DAC`

If you do not see one-way domestic behavior for one of those routes, check both files together:
- `config/routes.json` must contain the airline-route pair
- `config/route_trip_windows.json` must contain the same airline-route pair with `default_one_way_monitoring` in both `market_trip_profiles` and `active_market_trip_profiles`

Investigation on March 22, 2026 found and corrected a bug in this area:
- the routes existed
- `default_one_way_monitoring` was often listed in `active_market_trip_profiles`
- but it was missing from `market_trip_profiles`
- result: the loader filtered to the remaining route candidate profiles, and the planner resolved round-trip behavior instead of the intended one-way baseline

That specific wiring has now been corrected for the Bangladesh domestic baseline routes, and the validator should flag the same mistake in future edits.

## Practical Examples

### Turn on normal one-way only

In `route_trip_windows.json`:

```json
"market_trip_profiles": [
  "default_one_way_monitoring"
],
"active_market_trip_profiles": [
  "default_one_way_monitoring"
]
```

### Turn on normal one-way and domestic round-trip

```json
"market_trip_profiles": [
  "default_one_way_monitoring",
  "bangladesh_domestic_round_trip_short"
],
"active_market_trip_profiles": [
  "default_one_way_monitoring",
  "bangladesh_domestic_round_trip_short"
]
```

### Keep operational small, but make training richer

```json
"market_trip_profiles": [
  "default_one_way_monitoring",
  "bangladesh_domestic_round_trip_short"
],
"active_market_trip_profiles": [
  "default_one_way_monitoring"
],
"training_market_trip_profiles": [
  "bangladesh_domestic_eid_round_trip_2026",
  "bangladesh_domestic_eid_capital_outbound_one_way_2026",
  "bangladesh_domestic_eid_capital_return_one_way_2026",
  "inventory_anchor_departure_tracking_default"
]
```

### Add a deep-only enrichment layer

```json
"active_market_trip_profiles": [
  "default_one_way_monitoring"
],
"training_market_trip_profiles": [
  "inventory_anchor_departure_tracking_default"
],
"deep_market_trip_profiles": [
  "worker_return_from_middle_east_long_window",
  "hub_spoke_or_longhaul_return_window"
]
```

## Rule of Thumb

If you want to change what actually runs for a route:
- edit `route_trip_windows.json`

If you want to change what a profile means:
- edit `market_priors.json`

If you want to change the default outbound date universe:
- edit `dates.json`

## Validation

Before changing scheduler-facing trip config, run:

```powershell
.\.venv\Scripts\python.exe tools\validate_trip_config.py
```

This catches:
- unknown profile references
- route keys not present in `config/routes.json`
- duplicate profile names in profile arrays
- missing route-window entries for configured airline-route pairs

It now also fails a route where:
- a profile is listed in `active_market_trip_profiles`
- but the same profile is missing from `market_trip_profiles`

## Current Operational Pattern

The current design separates:
- `operational` for comparison-safe live cycles
- `training` for daily core enrichment, holiday overlays, and inventory-anchor behavior
- `deep` for broad weekly/opportunistic enrichment

Use `active_market_trip_profiles` conservatively if runtime is important.

At the moment, the intended common operational baseline is:
- `default_one_way_monitoring` for one-way coverage across routes
- route-specific round-trip profiles layered on top where applicable
- but the one-way profile only becomes effective if it is also present in `market_trip_profiles`

The current future-proof training pattern is:
- operational coverage stays stable and comparison-safe
- training does not blindly expand to all route-level candidate profiles anymore
- training is the explicit core daily enrichment layer
- domestic airlines explicitly add Eid round-trip and directional Eid one-way overlays in training
- worker, tourism, regional, and hub-spoke route behaviors can be declared explicitly in training at route or airline level
- inventory-anchor tracking is kept in training so availability movement can be modeled against fare change behavior
- deep mode is the broadest lane and is intended for weekly or opportunistic enrichment rather than daily execution

---

> **Codex will review your output once you are done.**
