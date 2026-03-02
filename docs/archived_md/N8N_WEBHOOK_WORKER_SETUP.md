# n8n + PAD Manual-Assisted Automation Guide

Last updated: 2026-02-27

## Purpose

Use this when n8n does not support `n8n-nodes-base.executeCommand`.

Architecture:

1. n8n triggers HTTP requests
2. local worker receives request and runs `tools/run_all_manual_assisted.py`
3. n8n polls job status until complete
4. optional PAD can do the same (HTTP mode) or run direct PowerShell mode

## Files

- n8n import file: `workflows/n8n_manual_assisted_webhook.json`
- worker: `tools/manual_assisted_webhook_worker.py`
- worker launcher: `tools/manual_assisted_webhook_worker.bat`
- PAD direct runner: `tools/pad_run_manual_assisted.ps1`

## Token: what it is and what to use

`--token` is **your own secret string**. You choose it.

Example token:

`USBA_LOCAL_2026_ManualAssist`

If worker starts with a token, every call must send:

`Authorization: Bearer <that exact token>`

If you do not want auth for local testing, start worker **without** `--token`.

## 1) Start worker (recommended with token)

```powershell
Set-Location C:\Users\TLL-90134\Documents\airline_scraper_full_clone
tools\manual_assisted_webhook_worker.bat --host 127.0.0.1 --port 8787 --token USBA_LOCAL_2026_ManualAssist
```

## 2) Verify worker

With token:

```powershell
$token = "USBA_LOCAL_2026_ManualAssist"
iwr http://127.0.0.1:8787/health `
  -Headers @{ Authorization = "Bearer $token" } `
  -UseBasicParsing | Select-Object -Expand Content
```

Expected:

`{"ok": true, ...}`

If you open `http://127.0.0.1:8787` in browser and see `{"ok":false,"error":"unauthorized"}`, that is expected when token auth is enabled.

## 3) Import and configure n8n workflow

Import:

- `workflows/n8n_manual_assisted_webhook.json`

In node `Config`, set:

- `workerBaseUrl` = `http://127.0.0.1:8787`
- `workerToken` = same token used in worker startup
- `cliArgs` = arguments for `run_all_manual_assisted.py`

Example `cliArgs`:

```json
["--limit-dates","1","--ingest","--non-interactive","--stop-on-error"]
```

Run flow:

1. `Start Run (POST)` sends job to worker
2. worker returns `job_id`
3. n8n loops `Wait` -> `Get Job Status`
4. flow ends at `Final Output` when job status is `succeeded` or `failed`

## 4) Worker endpoints

- `GET /health`
- `POST /run-all-manual-assisted`
- `GET /jobs`
- `GET /jobs/<job_id>`

POST payload:

```json
{
  "cli_args": ["--limit-dates","1","--ingest","--non-interactive","--stop-on-error"]
}
```

## 5) Artifact locations

Worker state:

- `output/manual_sessions/webhook_worker/jobs/<job_id>.json`
- `output/manual_sessions/webhook_worker/logs/<job_id>.log`
- `output/manual_sessions/webhook_worker/run_all_manual_assisted_<job_id>.json`

Capture artifacts:

- `output/manual_sessions/queues/`
- `output/manual_sessions/queue_runs/`
- `output/manual_sessions/runs/`

## 6) PAD mode A: call worker over HTTP

PAD can:

1. call `POST /run-all-manual-assisted`
2. parse returned `job_id`
3. poll `GET /jobs/<job_id>` until `succeeded` or `failed`

Use header:

- `Authorization: Bearer <token>` (if token enabled)

## 7) PAD mode B: run direct local script (no worker)

```powershell
powershell -ExecutionPolicy Bypass -File tools\pad_run_manual_assisted.ps1 `
  -RepoRoot "C:\Users\TLL-90134\Documents\airline_scraper_full_clone" `
  -RunAllArgs @("--limit-dates","1","--ingest","--non-interactive","--stop-on-error")
```

Use this if you want PAD-only orchestration and no persistent worker process.

## Troubleshooting

1. `unauthorized` response:
   - token mismatch between worker startup and request header.
2. `connect ECONNREFUSED`:
   - worker is not running on host/port.
3. Job failed quickly with BS/2A:
   - expected when anti-bot/session state fails; check:
   - `output/manual_sessions/webhook_worker/logs/<job_id>.log`
   - batch retry queue output in `output/manual_sessions/queue_runs/`.
