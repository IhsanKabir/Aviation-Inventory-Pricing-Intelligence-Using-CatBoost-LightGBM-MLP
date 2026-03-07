# Aero Pulse Web Application Plan

This directory now contains the first Next.js shell for the operational monitor.

## Stack

- Next.js
- React
- TypeScript
- API-first data access through the FastAPI reporting layer

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

- [apps/web/.env.example](C:/Users/TLL-90134/Documents/airline_scraper_full_clone/apps/web/.env.example)

## Current Pages

- `/`
  Executive shell with API health, latest cycle, airline, and route cards.

- `/routes`
  Route-monitor plan surface.

- `/changes`
  Change-event browser plan surface.

- `/forecasting`
  Forecast view placeholder.

## Why Vercel May Help

Vercel is useful for the Next.js frontend only. It is not required to build the shell locally.

Recommended split later:

- Vercel:
  deploy the Next.js frontend

- separate backend host:
  deploy FastAPI

- BigQuery + Looker Studio:
  analytics and dashboards
