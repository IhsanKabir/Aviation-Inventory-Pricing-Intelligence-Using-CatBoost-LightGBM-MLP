@echo off
setlocal EnableDelayedExpansion
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\output\reports" mkdir "%ROOT%\output\reports"

set "PYEXE=%ROOT%\.venv\Scripts\python.exe"
set "LOGFILE=%ROOT%\logs\ingestion_4h.log"
set "RECOVERY_HELPER=%ROOT%\tools\recover_interrupted_accumulation.py"
set "RECOVERY_STATUS=%ROOT%\output\reports\accumulation_recovery_latest.json"
set "CYCLE_STATE=%ROOT%\output\reports\accumulation_cycle_latest.json"
set "ENVFILE=%ROOT%\.env"

if not exist "%PYEXE%" (
  echo [%date% %time%] python exe not found: %PYEXE%>> "%LOGFILE%"
  exit /b 1
)

if exist "%ENVFILE%" (
  for /f "usebackq eol=# tokens=1* delims==" %%A in ("%ENVFILE%") do (
    if /I "%%~A"=="BIGQUERY_PROJECT_ID" set "BIGQUERY_PROJECT_ID=%%~B"
    if /I "%%~A"=="BIGQUERY_DATASET" set "BIGQUERY_DATASET=%%~B"
    if /I "%%~A"=="GOOGLE_APPLICATION_CREDENTIALS" set "GOOGLE_APPLICATION_CREDENTIALS=%%~B"
    if /I "%%~A"=="OPERATIONAL_COMPLETION_BUFFER_MINUTES" set "OPERATIONAL_COMPLETION_BUFFER_MINUTES=%%~B"
    if /I "%%~A"=="ACCUMULATION_COMPLETION_BUFFER_MINUTES" set "ACCUMULATION_COMPLETION_BUFFER_MINUTES=%%~B"
  )
)

if not defined OPERATIONAL_COMPLETION_BUFFER_MINUTES set "OPERATIONAL_COMPLETION_BUFFER_MINUTES=%ACCUMULATION_COMPLETION_BUFFER_MINUTES%"
if not defined OPERATIONAL_COMPLETION_BUFFER_MINUTES set "OPERATIONAL_COMPLETION_BUFFER_MINUTES=90"

if not defined BIGQUERY_PROJECT_ID (
  echo [%date% %time%] warning: BIGQUERY_PROJECT_ID not set; automatic BigQuery sync will be skipped>> "%LOGFILE%"
)
if not defined BIGQUERY_DATASET (
  echo [%date% %time%] warning: BIGQUERY_DATASET not set; automatic BigQuery sync will be skipped>> "%LOGFILE%"
)
if defined BIGQUERY_PROJECT_ID if defined BIGQUERY_DATASET if not defined GOOGLE_APPLICATION_CREDENTIALS (
  echo [%date% %time%] warning: GOOGLE_APPLICATION_CREDENTIALS not set; automatic BigQuery sync requires ADC or an explicit service-account JSON>> "%LOGFILE%"
)

if exist "%RECOVERY_HELPER%" (
  echo [%date% %time%] ingestion cycle launch check>> "%LOGFILE%"
  "%PYEXE%" "%RECOVERY_HELPER%" --mode guarded-run --python-exe "%PYEXE%" --root "%ROOT%" --reports-dir "%ROOT%\output\reports" --min-completed-gap-minutes "%OPERATIONAL_COMPLETION_BUFFER_MINUTES%" -- "%PYEXE%" "%ROOT%\run_pipeline.py" --python-exe "%PYEXE%" --skip-reports --report-output-dir "%ROOT%\output\reports" --report-timestamp-tz local >> "%LOGFILE%" 2>&1
  set "RC=%ERRORLEVEL%"
  if exist "%RECOVERY_STATUS%" if exist "%CYCLE_STATE%" (
    powershell -NoProfile -Command "$p = Get-Content -Raw '%RECOVERY_STATUS%' | ConvertFrom-Json; $c = Get-Content -Raw '%CYCLE_STATE%' | ConvertFrom-Json; $msg = ('[{0} {1}] ingestion wrapper summary: state={2} action={3} reason={4} cycle_id={5} launched={6} db_ok={7} rc={8}' -f (Get-Date -Format 'ddd MM/dd/yyyy'), (Get-Date -Format 'HH:mm:ss.ff'), $c.state, $p.action, $p.reason, $c.cycle_id, $p.launched, $p.db_check.ok, '!RC!'); Add-Content -Path '%LOGFILE%' -Value $msg" >nul 2>&1
  )
  if "!RC!"=="10" (
    echo [%date% %time%] ingestion cycle skipped: wrapper lock or active accumulation already present>> "%LOGFILE%"
    exit /b 0
  )
  if "!RC!"=="11" (
    echo [%date% %time%] ingestion cycle skipped: %OPERATIONAL_COMPLETION_BUFFER_MINUTES% minute post-completion buffer is active>> "%LOGFILE%"
    exit /b 0
  )
  if "!RC!"=="12" (
    echo [%date% %time%] ingestion cycle skipped: PostgreSQL is unavailable>> "%LOGFILE%"
    exit /b 0
  )
  echo [%date% %time%] ingestion wrapper finished rc=!RC!>> "%LOGFILE%"
  exit /b !RC!
)

echo [%date% %time%] starting ingestion cycle>> "%LOGFILE%"
"%PYEXE%" "%ROOT%\run_pipeline.py" --python-exe "%PYEXE%" --skip-reports --report-output-dir "%ROOT%\output\reports" --report-timestamp-tz local >> "%LOGFILE%" 2>&1
set "RC=%ERRORLEVEL%"
echo [%date% %time%] ingestion cycle finished rc=!RC!>> "%LOGFILE%"
exit /b !RC!
