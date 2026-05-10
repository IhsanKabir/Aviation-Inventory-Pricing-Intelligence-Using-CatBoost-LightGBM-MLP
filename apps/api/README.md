# Aero Pulse Reporting API

This API is the operational backend for the planned web application.

## Scope

- serve hosted reporting views from BigQuery-first warehouse queries
- fall back to PostgreSQL only for local transitional endpoints where needed
- serve change-event summaries and details
- serve penalty and tax reporting views
- provide route and airline filter metadata for the frontend

## Current Entry Point

Run locally from the repository root:

```powershell
.\.venv\Scripts\python.exe -m uvicorn apps.api.app.main:app --reload
```

## Initial Endpoints

- `GET /health`
- `GET /api/v1/meta/airlines`
- `GET /api/v1/meta/routes`
- `GET /api/v1/reporting/cycles/latest`
- `GET /api/v1/reporting/cycles/recent`
- `GET /api/v1/reporting/current-snapshot`
- `GET /api/v1/reporting/airline-operations`
- `GET /api/v1/reporting/route-summary`
- `GET /api/v1/reporting/change-events`
- `GET /api/v1/reporting/change-dashboard`
- `GET /api/v1/reporting/penalties`
- `GET /api/v1/reporting/taxes`
- `GET /api/v1/reporting/export.xlsx`

## Hosted Mode

The preferred hosted deployment path is BigQuery-backed. These endpoints are
designed to work without `AIRLINE_DB_URL` when BigQuery is configured:

- `GET /health`
- `GET /api/v1/reporting/cycle-health`
- `GET /api/v1/meta/airlines`
- `GET /api/v1/meta/routes`
- `GET /api/v1/reporting/cycles/latest`
- `GET /api/v1/reporting/cycles/recent`
- `GET /api/v1/reporting/route-monitor-matrix`
- `GET /api/v1/reporting/airline-operations`
- `GET /api/v1/reporting/change-events`
- `GET /api/v1/reporting/change-dashboard`
- `GET /api/v1/reporting/penalties`
- `GET /api/v1/reporting/taxes`
- `GET /api/v1/reporting/export.xlsx`

`current-snapshot` and `route-summary` remain transitional PostgreSQL-oriented endpoints.

## Excel Export

`GET /api/v1/reporting/export.xlsx` returns a filter-scoped workbook that can include:

- `routes`
- `changes`
- `taxes`
- `penalties`

The endpoint accepts the same core query parameters already used by the reporting pages, including `cycle_id`, `airline`, `origin`, `destination`, `cabin`, `start_date`, `end_date`, `domain`, `change_type`, `direction`, `route_limit`, `history_limit`, and `limit`.

Route-bearing payloads now include country-aware route metadata derived from `config/airport_countries.json`, including `route_type` (`DOM`, `INT`, `UNK`), `origin_country_code`, `destination_country_code`, `country_pair`, `domestic_country_code`, and `is_cross_border`.

`GET /api/v1/reporting/airline-operations` is the route-operations surface for the web page. It returns route-level operating summaries, weekday profiles, airline departure-time bands, and recent-cycle trend points using the same filter contract as the rest of the reporting API plus `route_type` and `trend_limit`.

`GET /api/v1/reporting/taxes` now acts as a tax-monitor payload, not only a detail-row feed. It still returns `rows`, but now also includes route-level spread summaries, airline-level movement summaries, and recent-cycle tax trend metadata for the same filtered scope.

`GET /api/v1/reporting/change-dashboard` is the market-movement summary surface for the Changes page. It returns scope-level counts, daily change volume, top routes, top airlines, domain mix, field mix, and largest-move events for the same filter contract as `change-events`.

`GET /api/v1/reporting/route-monitor-matrix` now accepts optional `trip_type` and `return_date` filters for PostgreSQL-backed round-trip review. When round-trip metadata is present, route payloads include trip pairing fields and flight-group leg metadata so the web can group outbound and inbound sections together.

## Naming Note

The API uses `cycle_id` publicly. The existing PostgreSQL model still stores the same identifier in a legacy UUID field for backward compatibility.

## Why This Exists

The current Excel workbook is useful as an export, but it is too heavy to remain the main interactive surface. The API is the stable backend layer for:

- a faster Next.js operational monitor
- downstream analytics handoff
- reusable query contracts for BigQuery export

## Deployment

Production target:

- host on Google Cloud Run
- use [Dockerfile](Dockerfile)
- use [cloudrun.service.yaml](cloudrun.service.yaml) as the deployment template
- set env vars from [.env.example](.env.example)

For BigQuery in Cloud Run, use the attached service account. Do not mount a downloaded JSON key in production.

---

> **Codex will review your output once you are done.**
