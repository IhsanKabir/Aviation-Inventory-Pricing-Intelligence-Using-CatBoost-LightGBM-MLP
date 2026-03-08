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
- `GET /api/v1/reporting/route-summary`
- `GET /api/v1/reporting/change-events`
- `GET /api/v1/reporting/penalties`
- `GET /api/v1/reporting/taxes`

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
- `GET /api/v1/reporting/change-events`
- `GET /api/v1/reporting/penalties`
- `GET /api/v1/reporting/taxes`

`current-snapshot` and `route-summary` remain transitional PostgreSQL-oriented endpoints.

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
