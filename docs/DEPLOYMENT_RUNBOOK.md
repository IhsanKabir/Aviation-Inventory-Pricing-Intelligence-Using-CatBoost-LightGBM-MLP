# Deployment Runbook

This system should be deployed as a split stack:

- `apps/web` on Vercel
- `apps/api` on Google Cloud Run
- local PostgreSQL remains on the collection/training machine
- BigQuery + Looker Studio remain the hosted analytics and forecasting layer

GitHub is source control and CI, not the runtime host.

For repository-driven deployments, see:

- [docs/GITHUB_DEPLOY_SECRETS.md](GITHUB_DEPLOY_SECRETS.md)
- [.github/workflows/deploy-api-cloud-run.yml](../.github/workflows/deploy-api-cloud-run.yml)
- [.github/workflows/deploy-web-vercel.yml](../.github/workflows/deploy-web-vercel.yml)

## Recommended target architecture

### Frontend

- Platform: Vercel
- App root: `apps/web`
- Runtime env:
  - `API_BASE_URL`
  - `NEXT_PUBLIC_API_BASE_URL`
  - `NEXTAUTH_URL`
  - `AUTH_SECRET` when Google OAuth is enabled
  - `AUTH_GOOGLE_ID` and `AUTH_GOOGLE_SECRET` only if you want Google sign-in enabled

### API

- Platform: Google Cloud Run
- Container source: `apps/api/Dockerfile`
- Runtime env:
  - `API_CORS_ORIGINS`
  - `API_FORECASTING_SOURCE=bigquery`
  - `BIGQUERY_PROJECT_ID=aeropulseintelligence`
  - `BIGQUERY_DATASET=aviation_intel`
  - `AIRLINE_DB_URL` only if you intentionally keep PostgreSQL-backed transitional endpoints enabled

### Database

- No-cost recommended path:
  - keep PostgreSQL local for ingestion, comparisons, and ML/DL training
- Optional later:
  - move the operational write path to managed PostgreSQL if full hosted writes are required

### Warehouse / BI

- BigQuery dataset: `aeropulseintelligence.aviation_intel`
- Looker Studio connects to curated views

## Why not GitHub Pages

GitHub Pages can only host static content. This project depends on:

- server-side API execution
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
- keep `AIRLINE_DB_URL` in Secret Manager only if you still need transitional PostgreSQL-backed endpoints

## Environment variable map

### Vercel

- `API_BASE_URL=https://YOUR_API_DOMAIN`
- `NEXT_PUBLIC_API_BASE_URL=https://YOUR_API_DOMAIN`
- `NEXTAUTH_URL=https://YOUR_WEB_DOMAIN`
- `AUTH_SECRET=long-random-secret` when Google OAuth is enabled
- `AUTH_GOOGLE_ID=...` optional
- `AUTH_GOOGLE_SECRET=...` optional

### Cloud Run

- `API_CORS_ORIGINS=https://YOUR_VERCEL_DOMAIN.vercel.app`
- `API_DEFAULT_LIMIT=250`
- `API_MAX_LIMIT=5000`
- `API_FORECASTING_SOURCE=bigquery`
- `BIGQUERY_PROJECT_ID=aeropulseintelligence`
- `BIGQUERY_DATASET=aviation_intel`
- `AIRLINE_DB_URL=postgresql+psycopg2://...` only if you want PostgreSQL transitional endpoints enabled

## Vercel setup checklist

1. Import the GitHub repo into Vercel
2. Set Root Directory to `apps/web`
3. Framework preset should detect `Next.js`
4. Add env vars from [apps/web/.env.production.example](../apps/web/.env.production.example)
5. If you want Google sign-in, set `AUTH_SECRET`, `AUTH_GOOGLE_ID`, and `AUTH_GOOGLE_SECRET`
6. Deploy

## Cloud Run setup checklist

1. Build container from repo root using [apps/api/Dockerfile](../apps/api/Dockerfile)
2. Push image to Artifact Registry
3. Create service account for the API
4. Grant:
   - BigQuery Data Viewer
   - BigQuery Job User
5. Skip `AIRLINE_DB_URL` entirely for the preferred BigQuery-backed hosted deployment. Add it only if you want PostgreSQL transitional endpoints enabled.
6. Deploy Cloud Run service using [apps/api/cloudrun.service.yaml](../apps/api/cloudrun.service.yaml)
7. Set `API_CORS_ORIGINS` to the real Vercel domain

Helper script:

- [tools/deploy_api_cloud_run.ps1](../tools/deploy_api_cloud_run.ps1)
  - default behavior: BigQuery-backed deployment without `AIRLINE_DB_URL`
  - add `-UseDbSecret -DbSecretName airline-db-url` only if you intentionally enable PostgreSQL transitional endpoints

GitHub Actions alternative:

- use `.github/workflows/deploy-api-cloud-run.yml`
- supply the required secrets from [docs/GITHUB_DEPLOY_SECRETS.md](GITHUB_DEPLOY_SECRETS.md)

## Suggested first production rollout

1. Deploy API first
2. Verify `/health` and `/docs`
3. Deploy web to Vercel
4. Point Vercel env vars to the API domain
5. Verify:
   - `/health`
   - `/routes`
   - `/forecasting`

GitHub Actions alternative:

- use `.github/workflows/deploy-web-vercel.yml`
- supply `VERCEL_TOKEN`, `VERCEL_ORG_ID`, and `VERCEL_PROJECT_ID`
- the workflow now uses `vercel pull -> vercel build --prod -> vercel deploy --prebuilt --prod`
- in this monorepo, the workflow runs Vercel CLI from repo root while Vercel itself keeps `apps/web` as the configured Root Directory

## Database migration

If you want a low-cost hosted read path, do not migrate the full local PostgreSQL database. Keep collection, training, and long history local, then export only a bounded recent hot-cache slice into BigQuery for hosted reads.

## Automatic warehouse sync after scheduler runs

`run_pipeline.py` supports cost-safe automatic BigQuery sync after a successful cycle.

Behavior:

- enabled only when `BIGQUERY_SYNC_ENABLED=1` or `--bigquery-sync-enabled` is set
- also requires `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET`
- skipped when `--skip-bigquery-sync` is used
- exports a rolling recent UTC capture-date window, then loads BigQuery with `partition-refresh`
- skipped when extraction health is `FAIL`; process success alone is not enough to publish
- does not fail the entire pipeline by default if warehouse sync fails

Useful controls:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --bigquery-sync-enabled --bigquery-sync-lookback-days 2 --bigquery-load-mode partition-refresh
.\.venv\Scripts\python.exe run_pipeline.py --skip-bigquery-sync
.\.venv\Scripts\python.exe run_pipeline.py --fail-on-bigquery-sync-error
.\.venv\Scripts\python.exe run_pipeline.py --fail-on-extraction-gate
```

Retention controls:

```powershell
.\.venv\Scripts\python.exe tools\bigquery_apply_retention.py --project-id aeropulseintelligence --dataset aviation_intel --hot-days 35 --forecast-days 90 --time-travel-hours 48 --apply
.\.venv\Scripts\python.exe tools\bigquery_storage_audit.py --project-id aeropulseintelligence --dataset aviation_intel
```

This keeps BigQuery-backed hosted pages close to the latest local collection cycle without turning BigQuery into the long-term storage system.

## Extraction quality before publish

Before relying on a hosted data refresh, check:

```powershell
.\.venv\Scripts\python.exe tools\pre_flight_session_check.py --dry-run
Get-Content output\reports\extraction_health_latest.md
```

`output/reports/extraction_health_latest.md` is the operator-facing source-quality gate. A `FAIL` status means BigQuery auto-sync is intentionally skipped until the extraction issue is fixed or a same-cycle retry succeeds.

---

> **Codex will review your output once you are done.**
