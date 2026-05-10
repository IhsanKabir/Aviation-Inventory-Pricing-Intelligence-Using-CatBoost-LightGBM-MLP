# Web Product Requirements

Last updated: 2026-03-09

## Purpose

This document formalizes the active web product scope for the Aero Pulse Intelligence Monitor.

It converts recent product feedback into:

- bug fixes
- UI/UX improvements
- new features
- future roadmap items

Use this as the active implementation brief for the Next.js frontend, FastAPI reporting API, and BigQuery-backed hosted monitor surfaces.

## Product Principles

1. The active page state must always be obvious.
2. Time is the primary comparison axis for operational flight review.
3. Route-level analysis must be easy to scan before it becomes deep.
4. Filters and date controls must be consistent across pages.
5. Excel remains an export artifact, not the primary interaction surface.
6. New features must preserve future compatibility with forecasting, inventory estimation, and penalty modeling.

## Information Architecture

Current and planned primary pages:

- `Overview`
- `Health`
- `Routes`
- `Airline Operations`
- `Taxes`
- `Changes`
- `Forecasting`
- `Penalties` later remains available, but exact penalty logic stays partially provisional until the RGN penalty model is ready for integration

Recommended navigation structure:

- `Overview`: executive summary and cycle freshness
- `Health`: pipeline and coverage status
- `Routes`: flight-level route monitor and route comparison
- `Operations`: daily and weekly airline activity by route
- `Taxes`: tax comparison and tax movement
- `Changes`: market-wide movement dashboard plus drilldown
- `Forecasting`: forecast, backtest, and confidence review

## Scope Grouping

## Bug Fixes

### B1. Navigation highlight must follow the active page

Problem:

- The current top navigation can leave `Overview` highlighted while another page is active.

Requirement:

- The active navigation state must always match the current route.
- Highlight logic must work for every top-level page.
- Active, hover, and keyboard-focus states must be visually distinct.

Acceptance criteria:

- Visiting `/routes` highlights `Routes`, not `Overview`.
- Visiting `/changes` highlights `Changes`, not `Overview`.
- Deep links and filtered URLs still map to the correct active tab.

Priority:

- Immediate

Current delivery note:

- First dashboard version should ship with:
  - scope-level change counts
  - daily movement series
  - top routes and airlines
  - domain and field mix
  - largest-move drilldown support

Current delivery note:

- First web version should ship with:
  - route-level spread summary
  - airline-level recent tax movement summary
  - recent-cycle tax trend strips
  - retained row-level tax verification table

## UI/UX Improvements

### U1. Comparison views must sort by departure time across airlines

Problem:

- Side-by-side views are currently harder to compare when rows are grouped airline by airline.

Requirement:

- For a selected route and date, all flight options should be ordered by departure time regardless of airline.
- Airline identity remains visible, but sort order should be chronological first.

Recommended UX:

- Use departure time as the default primary sort.
- Keep airline color tags or badges for fast visual grouping.
- Offer a secondary sort toggle if needed later, but default must stay time-first.

Acceptance criteria:

- Mixed-airline flights on the same route/date appear in chronological order.
- The ordering remains stable when prices or inventory values change.

Priority:

- Immediate

### U2. Changes view needs a scanning-first layout

Problem:

- The current changes presentation is functional but not yet optimized for fast scanning and investigation.

Requirement:

- The Changes surface must support quick market scanning, route drilldown, and row-level inspection.

Recommended UX:

- Use a three-part layout:
  - sticky left filter rail
  - main market dashboard area
  - sticky row-detail or summary panel on the right
- Keep high-value metrics always visible at the top:
  - change count
  - affected airlines
  - affected routes
  - net increase vs decrease mix
  - most volatile routes
- In the main table, pin the core identity columns:
  - route
  - airline
  - flight
  - departure
  - field changed
- Add quick chips:
  - `price`
  - `inventory`
  - `tax`
  - `schedule`
  - `penalty`
  - `new`
  - `sold out`

Acceptance criteria:

- Users can filter and inspect changes without losing context.
- Summary counts remain visible while scrolling a long event list.
- The same page supports both broad market review and row-level inspection.

Priority:

- Immediate

### U3. Date selection must become a first-class control

Problem:

- Users need better control for current, past, and time-comparison views.

Requirement:

- Users must be able to select:
  - a single date
  - a date range
  - preset windows such as `today`, `last 3 days`, `last 7 days`, `this week`

Recommended UX:

- Standardize one date control model across routes, taxes, changes, and operations:
  - preset chips
  - explicit start/end date picker
  - selected cycle override when needed
- Distinguish between:
  - `travel date`
  - `capture date`
  - `cycle date`

Acceptance criteria:

- Historical review and current review use the same date-control pattern.
- Users can switch between latest-cycle mode and explicit historical mode.

Priority:

- Immediate

### U4. Conditional inventory columns

Problem:

- `Open/Cap` and `Inv Press` should not appear when the underlying data is absent or not meaningful.

Requirement:

- Hide these columns unless the filtered dataset includes valid values.
- Keep the design ready for a stronger inventory estimation layer.

Recommended UX:

- Show the columns only when one of the following is true:
  - real capacity data exists
  - estimated capacity logic is active and confidence is non-null
- When visible, explain estimated values with a tooltip or confidence badge.

Acceptance criteria:

- Empty inventory-estimation columns do not consume horizontal space.
- Users can tell whether the shown values are observed or estimated.

Priority:

- Immediate

## New Features

### F1. Excel export based on current filters

Requirement:

- Users must be able to export an Excel workbook based on the currently selected filter state.

Recommended workbook structure:

- `Summary`
  - selected filters
  - cycle/date coverage
  - high-level counts
- `Routes`
  - filtered route monitor rows
- `Changes`
  - filtered change events
- `Taxes`
  - filtered tax comparison rows
- `Operations`
  - route-airline schedule summary when applicable
- `Forecasting`
  - latest filtered forecast rows when applicable
- `Metadata`
  - generation timestamp
  - source system
  - cycle IDs
  - parameter notes

Design note:

- Export should be generic and reusable across pages. Prefer a centralized export service rather than page-specific ad hoc workbook generation.

Priority:

- Immediate

### F2. International / domestic categorization

Requirement:

- Add reusable route categorization for `DOM` and `INT`.

Business rule:

- `DOM` when origin and destination are in the same country and both are domestic airports in that country.
- `INT` for all cross-border routes and domestic-to-international-country flows.

Recommended implementation structure:

- Keep airport-to-country mapping in configuration.
- Derive route category from normalized airport metadata.
- Make category available in:
  - current snapshot views
  - route monitor
  - changes
  - taxes
  - operations
  - forecasting features

Priority:

- Immediate

### F3. Round-trip support

Requirement:

- Add round-trip flight support across search, persistence, and display layers.

Scope:

- search and ingestion
- canonical data model additions
- API exposure
- web display patterns

Recommended structure:

- Preserve one-way observations as the base fact layer.
- Add round-trip search intent metadata rather than forcing one-way and round-trip into the same identity semantics.
- Treat outbound and inbound legs as linked legs under one trip query context.

Suggested phases:

- Phase 1:
  - support round-trip search and store paired-leg metadata
- Phase 2:
  - support round-trip comparison and ranking in the UI
- Phase 3:
  - support round-trip forecasting and itinerary-level intelligence

Priority:

- Immediate planning, later implementation after current one-way UX fixes

Current delivery note:

- Architecture groundwork is now in place:
  - shared trip request normalization
  - `OW` / `RT` search intent in collection runtime
  - persisted round-trip metadata in raw meta
  - one connector proof-of-concept path
- The next UI/API pass should expose paired outbound/inbound comparison rather than mixing legs into flat one-way tables.

### F4. New page: Airline Operations

Goal:

- Understand daily and weekly route operations by airline.

Core questions this page should answer:

- Which airlines operate a route?
- How many flights does each airline run?
- At what times do they depart?
- What is the daily pattern?
- What is the weekly pattern?
- How has the operation pattern changed?

Recommended UX:

- Top summary cards:
  - active airlines on route
  - total flights per day
  - first departure
  - last departure
- Main views:
  - route timetable matrix by airline and departure time
  - weekday heatmap
  - frequency trend over time
  - operational changes panel

Priority:

- Immediate feature planning

Current delivery note:

- First web version should ship with:
  - shared route/airline/date/cycle filters
  - route-level airline schedule summary
  - weekday rhythm summary
  - recent-cycle operating change table

### F5. Tax page should become a tax monitor, not a flat list only

Goal:

- Compare and monitor taxes across airlines and over time.

Requirement:

- Tax comparison must work route-wise and airline-wise.
- Tax movement over time must be visible without reading every row.

Recommended UX:

- Summary cards:
  - min tax
  - max tax
  - spread
  - routes with the largest change
- Comparison table sorted by route and departure time
- Compact trend or change strip per route-airline pair
- Highlight tax deltas similarly to fare movement

Priority:

- Immediate

### F6. Changes page should become a market movement dashboard

Goal:

- Reveal overall market movement, not just a raw list of event rows.

Requirement:

- Combine dashboard metrics and drilldown on one page.

Recommended dashboard modules:

- change volume over time
- top changed routes
- top changed airlines
- change-type distribution
- increase vs decrease mix
- schedule change and sell-out signals
- filtered event browser

Priority:

- Immediate

## Future Roadmap

### R1. Penalty module integration

Requirement:

- Keep the penalty area structurally ready for exact RGN penalty logic.

Current decision:

- Do not force a final penalty model yet.
- Preserve current penalty reporting as a placeholder and compatibility layer.

Design requirement:

- Penalty storage and API contracts should allow later replacement or enrichment without breaking page structure.

Priority:

- Future integration

### R2. Forecasting becomes the primary advanced intelligence module

Goal:

- Predict future airline movement
- predict future market movement
- predict likely airline inventory movement

Recommended forecasting roadmap:

- Version 1:
  - route-airline-cabin next-day point forecasts
  - baseline persistence and seasonal baselines shown beside ML outputs
  - reliability summary visible in the UI
- Version 2:
  - event classification:
    - likely increase
    - likely decrease
    - likely sell-out
    - likely schedule shift
- Version 3:
  - probabilistic forecasts
  - uncertainty bands
  - route-level market movement scores
  - inventory pressure estimation

Product requirement:

- Forecast pages must show model quality next to predictions.
- Users should never see predictions without freshness and confidence context.

Priority:

- Main strategic priority

### R3. Proactive product improvement expectation

Expectation:

- Product structure, data model, workflow, and visualization improvements should be recommended whenever there is a clear better path.

Implementation rule:

- Proposals should be documented before major structural changes.
- Relevant docs must be updated as scope shifts.

## Delivery Priority

Recommended order of work:

1. Navigation highlight fix
2. Departure-time ordering
3. Changes page scanning redesign
4. Shared date-selection model
5. Excel export foundation
6. INT/DOM categorization
7. Conditional inventory columns
8. Tax monitor upgrade
9. Changes dashboard upgrade
10. Airline Operations page
11. Round-trip architecture and search scope
12. Forecasting expansion
13. Future penalty model integration

## Data and API Implications

The following cross-cutting changes are expected:

- add stable shared filter contracts for:
  - date range
  - route
  - airline
  - cabin
  - route type (`DOM` / `INT`)
- expose operations-oriented aggregations from the API, not only raw rows
- ensure exports use the same filter semantics as the web pages
- keep current-cycle views and historical comparisons on compatible route identities
- preserve backward compatibility where hosted pages still rely on BigQuery curated views

## Acceptance Baseline for the Next Iteration

The next implementation cycle should deliver at minimum:

- correct active navigation state
- departure-time-first comparison ordering
- formal redesign direction for the Changes page
- reusable date filter pattern
- Excel export design contract
- INT/DOM data contract
- visibility rules for inventory-estimation fields
- scoped plan for Airline Operations, round-trip support, forecasting expansion, and future penalty integration

---

> **Codex will review your output once you are done.**
