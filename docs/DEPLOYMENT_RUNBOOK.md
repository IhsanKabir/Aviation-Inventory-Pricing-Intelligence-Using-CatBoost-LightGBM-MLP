# Deployment Runbook

This system should be deployed as a split stack:

- `apps/web` on Vercel
- `apps/api` on Google Cloud Run
- PostgreSQL on a managed provider
- BigQuery + Looker Studio remain the analytics layer

GitHub is source control and CI, not the runtime host.

## Recommended target architecture

### Frontend

- Platform: Vercel
- App root: `apps/web`
- Runtime env:
  - `API_BASE_URL`
  - `NEXT_PUBLIC_API_BASE_URL`

### API

- Platform: Google Cloud Run
- Container source: `apps/api/Dockerfile`
- Runtime env:
  - `AIRLINE_DB_URL`
  - `API_CORS_ORIGINS`
  - `API_FORECASTING_SOURCE=bigquery`
  - `BIGQUERY_PROJECT_ID=aeropulseintelligence`
  - `BIGQUERY_DATASET=aviation_intel`

### Database

- Use managed PostgreSQL
- Preferred options:
  - Cloud SQL
  - Neon
  - Supabase
  - Railway Postgres

### Warehouse / BI

- BigQuery dataset: `aeropulseintelligence.aviation_intel`
- Looker Studio connects to curated views

## Why not GitHub Pages

GitHub Pages can only host static content. This project depends on:

- server-side API execution
- PostgreSQL access
- BigQuery access
- authenticated runtime environment variables

So GitHub Pages is not a valid host for the application.

## Why Vercel is valid

Vercel is a good fit for the Next.js frontend only. The frontend already reads:

- `API_BASE_URL`
- `NEXT_PUBLIC_API_BASE_URL`

from environment variables, so it can point cleanly to a hosted API.

## Cloud Run deployment model

Use Cloud Run with a dedicated service account.

Important:
- do not deploy the API with a downloaded Google JSON key
- grant the Cloud Run service account BigQuery read access instead
- keep `AIRLINE_DB_URL` in Secret Manager

## Environment variable map

### Vercel

- `API_BASE_URL=https://YOUR_API_DOMAIN`
- `NEXT_PUBLIC_API_BASE_URL=https://YOUR_API_DOMAIN`

### Cloud Run

- `AIRLINE_DB_URL=postgresql+psycopg2://...`
- `API_CORS_ORIGINS=https://YOUR_VERCEL_DOMAIN.vercel.app`
- `API_DEFAULT_LIMIT=250`
- `API_MAX_LIMIT=5000`
- `API_FORECASTING_SOURCE=bigquery`
- `BIGQUERY_PROJECT_ID=aeropulseintelligence`
- `BIGQUERY_DATASET=aviation_intel`

## Vercel setup checklist

1. Import the GitHub repo into Vercel
2. Set Root Directory to `apps/web`
3. Framework preset should detect `Next.js`
4. Add env vars from [apps/web/.env.production.example](../apps/web/.env.production.example)
5. Deploy

## Cloud Run setup checklist

1. Build container from repo root using [apps/api/Dockerfile](../apps/api/Dockerfile)
2. Push image to Artifact Registry
3. Create service account for the API
4. Grant:
   - BigQuery Data Viewer
   - BigQuery Job User
5. Put `AIRLINE_DB_URL` into Secret Manager
6. Deploy Cloud Run service using [apps/api/cloudrun.service.yaml](../apps/api/cloudrun.service.yaml)
7. Set `API_CORS_ORIGINS` to the real Vercel domain

## Suggested first production rollout

1. Deploy API first
2. Verify `/health` and `/docs`
3. Deploy web to Vercel
4. Point Vercel env vars to the API domain
5. Verify:
   - `/health`
   - `/routes`
   - `/forecasting`

## Database migration

If the app still uses local PostgreSQL, migrate first.

Preferred first path:

- [NEON_MIGRATION_RUNBOOK.md](NEON_MIGRATION_RUNBOOK.md)
