# Neon Migration Runbook

This is the fastest path to move the application off the local Windows PostgreSQL instance.

## Current local state

The application currently resolves the database from local `.env` values:

- `DB_HOST=localhost`
- `DB_PORT=5432`
- `DB_NAME=Playwright_API_Calling`
- `DB_USER=postgres`

That works locally only. It is not deployable to Cloud Run.

## Target state

Replace local DB variables with a single:

```env
AIRLINE_DB_URL=postgresql+psycopg2://USER:PASSWORD@HOST/Playwright_API_Calling?sslmode=require
```

## One-command local migration helper

Use:

```powershell
.\tools\migrate_to_neon.ps1 -NeonDbUrl "postgresql+psycopg2://USER:PASSWORD@HOST/Playwright_API_Calling?sslmode=require"
```

The script will:

1. read local DB settings from `.env`
2. locate `pg_dump` / `pg_restore`
3. create a custom dump
4. restore into Neon
5. validate with SQLAlchemy

## After migration

Update local `.env` to:

```env
AIRLINE_DB_URL=postgresql+psycopg2://USER:PASSWORD@HOST/Playwright_API_Calling?sslmode=require
```

Then remove or ignore:

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

## Validation

Run:

```powershell
.\.venv\Scripts\python.exe -m uvicorn apps.api.app.main:app --reload
```

Check:

- `/health`
- `/docs`
- `/api/v1/reporting/cycles/latest`

## Production follow-up

Once validated locally:

1. store the same `AIRLINE_DB_URL` in Secret Manager
2. deploy Cloud Run API
3. point Vercel frontend to the Cloud Run API URL

---

> **Codex will review your output once you are done.**
