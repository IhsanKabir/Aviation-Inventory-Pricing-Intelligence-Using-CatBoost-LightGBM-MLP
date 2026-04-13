# Aero Pulse Web Application Plan

This directory contains the Next.js web application for the hosted operational monitor.

Active scope, UX requirements, and delivery priorities are tracked in [../../docs/WEB_PRODUCT_REQUIREMENTS.md](../../docs/WEB_PRODUCT_REQUIREMENTS.md).

## Stack

- Next.js
- React
- TypeScript
- API-first data access through the FastAPI reporting layer
- Vercel Analytics via `@vercel/analytics/next` in the root layout

## Local Run

From the repository root:

```powershell
cd apps\web
npm install
npm run dev
```

Default API target:

- `http://127.0.0.1:8000`

Override with:

- `API_BASE_URL`
- `NEXT_PUBLIC_API_BASE_URL`

See:

- [.env.example](.env.example)

## Current Pages

- `/`
  Executive overview with cycle health, platform status, airline coverage, and route coverage cards.

- `/market`
  Market Intelligence hub that groups the live routes, operations, penalties, taxes, changes, and GDS views.

- `/routes`
  Live route monitor with API-backed filters for route, airline, cabin, and trip scope, plus filter-scoped Excel export.

- `/operations`
  Airline operations page for route-level airline activity, weekday rhythm, and recent-cycle operational change review.

- `/penalties`
  Penalty comparison screen against the reporting API with filter-scoped Excel export.

- `/taxes`
  Tax monitor screen with route spread, airline movement summaries, and filter-scoped Excel export.

- `/changes`
  Market-movement dashboard plus row-level event browser, with filter-scoped Excel export.

- `/forecasting`
  Warehouse-backed ML/DL forecast and backtest review surface.

## Planned Page Evolution

- `/routes`
  Now supports first-pass round-trip grouping by pairing outbound and inbound route blocks under a shared round-trip shell when trip metadata is present.

- `/operations`
  Planned route-level airline operations page for daily and weekly pattern review.

- `/taxes`
  Will evolve from a flat current-cycle table into a comparative tax-monitor surface with change visibility.

- `/changes`
  Now combines a market-movement dashboard, daily volume strip, largest-move callouts, and the row-level drilldown table.

- `/forecasting`
  Remains the main forward-looking intelligence page and must always pair predictions with quality and freshness context.

## UX Guardrails

- Navigation highlight must always match the active route.
- Top-level navigation should reflect the primary workspaces: overview, market intelligence, forecasting, and downloads.
- Cross-airline comparison views default to departure-time ordering.
- Date-selection behavior should be shared across pages instead of implemented differently per screen.
- Route-bearing views should expose country-aware `DOM` / `INT` context from the API instead of duplicating route classification in the client.
- Operations views should stay route-first: who flies, when they fly, how often they fly, and how that footprint changes across recent cycles.
- Dense analytical pages should prefer sticky filters, pinned identity columns, and scan-first summaries.
- Excel is a downloadable artifact, not the primary interaction model.

## Why Vercel May Help

Vercel is useful for the Next.js frontend only. It is not required to build the web app locally.

Recommended split later:

- Vercel:
  deploy the Next.js frontend

- separate backend host:
  deploy FastAPI

- BigQuery + Looker Studio:
  analytics and dashboards

## Deployment

For production:

- set Vercel Root Directory to `apps/web`
- use [vercel.json](vercel.json)
- set env vars from [.env.production.example](.env.production.example)
- set `NEXTAUTH_URL` to the deployed web domain
- set `AUTH_SECRET` if Google sign-in is enabled
- add `AUTH_GOOGLE_ID` and `AUTH_GOOGLE_SECRET` only when enabling Google sign-in

The frontend should point to the hosted API, not localhost.
