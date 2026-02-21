# Setup Quickstart

## One command (Windows PowerShell)

```powershell
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1
```

## What it does

1. Creates `.venv` if missing.
2. Upgrades `pip/setuptools/wheel`.
3. Installs dependencies from `requirements-lock.txt` (fallback: `requirements.txt`).

## After setup

```powershell
.\.venv\Scripts\python.exe -m py_compile run_all.py run_pipeline.py predict_next_day.py
.\.venv\Scripts\python.exe scheduler\maintenance_tasks.py --task daily_ops
.\.venv\Scripts\python.exe tools\ci_checks.py --reports-dir output\reports --timestamp-tz local
```

## Notes

- Database connection resolution order:
1. `AIRLINE_DB_URL`
2. `DB_HOST` + `DB_PORT` + `DB_NAME` + `DB_USER` + `DB_PASSWORD`
3. fallback local URL
- Use `.env.example` as reference for env variables.
- Keep `requirements-lock.txt` updated after dependency upgrades:

```powershell
.\.venv\Scripts\python.exe -m pip freeze > requirements-lock.txt
```

- Optional: install local pre-commit guard

```powershell
powershell -ExecutionPolicy Bypass -File tools\install_git_hooks.ps1
```
