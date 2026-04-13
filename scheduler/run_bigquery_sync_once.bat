@echo off
setlocal EnableDelayedExpansion
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\output\reports" mkdir "%ROOT%\output\reports"

set "LOGFILE=%ROOT%\logs\bigquery_sync.log"
set "LOADER=%ROOT%\tools\load_bigquery_latest.ps1"
set "ENVFILE=%ROOT%\.env"

set "BIGQUERY_PROJECT_ID="
set "BIGQUERY_DATASET="
set "GOOGLE_APPLICATION_CREDENTIALS="
set "BIGQUERY_SYNC_LOOKBACK_DAYS="
set "BIGQUERY_SYNC_OUTPUT_DIR="

if exist "%ENVFILE%" (
  for /f "usebackq eol=# tokens=1* delims==" %%A in ("%ENVFILE%") do (
    if /I "%%~A"=="BIGQUERY_PROJECT_ID" set "BIGQUERY_PROJECT_ID=%%~B"
    if /I "%%~A"=="BIGQUERY_DATASET" set "BIGQUERY_DATASET=%%~B"
    if /I "%%~A"=="GOOGLE_APPLICATION_CREDENTIALS" set "GOOGLE_APPLICATION_CREDENTIALS=%%~B"
    if /I "%%~A"=="BIGQUERY_SYNC_LOOKBACK_DAYS" set "BIGQUERY_SYNC_LOOKBACK_DAYS=%%~B"
    if /I "%%~A"=="BIGQUERY_SYNC_OUTPUT_DIR" set "BIGQUERY_SYNC_OUTPUT_DIR=%%~B"
  )
)

if not defined BIGQUERY_SYNC_LOOKBACK_DAYS set "BIGQUERY_SYNC_LOOKBACK_DAYS=7"
if not defined BIGQUERY_SYNC_OUTPUT_DIR set "BIGQUERY_SYNC_OUTPUT_DIR=output/warehouse/bigquery"

if not defined BIGQUERY_PROJECT_ID (
  echo [%date% %time%] bigquery sync skipped: BIGQUERY_PROJECT_ID not set>> "%LOGFILE%"
  exit /b 0
)
if not defined BIGQUERY_DATASET (
  echo [%date% %time%] bigquery sync skipped: BIGQUERY_DATASET not set>> "%LOGFILE%"
  exit /b 0
)
if not defined GOOGLE_APPLICATION_CREDENTIALS (
  echo [%date% %time%] bigquery sync skipped: GOOGLE_APPLICATION_CREDENTIALS not set>> "%LOGFILE%"
  exit /b 0
)
if not exist "%GOOGLE_APPLICATION_CREDENTIALS%" (
  echo [%date% %time%] bigquery sync skipped: credentials file missing "%GOOGLE_APPLICATION_CREDENTIALS%">> "%LOGFILE%"
  exit /b 0
)
if not exist "%LOADER%" (
  echo [%date% %time%] bigquery sync skipped: loader not found "%LOADER%">> "%LOGFILE%"
  exit /b 1
)

for /f %%I in ('powershell -NoProfile -Command "(Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')"') do set "END_DATE=%%I"
for /f %%I in ('powershell -NoProfile -Command "(Get-Date).ToUniversalTime().AddDays(-([int]%BIGQUERY_SYNC_LOOKBACK_DAYS%-1)).ToString('yyyy-MM-dd')"') do set "START_DATE=%%I"

echo [%date% %time%] starting bigquery sync start_date=!START_DATE! end_date=!END_DATE! lookback_days=!BIGQUERY_SYNC_LOOKBACK_DAYS!>> "%LOGFILE%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%LOADER%" -CredentialsJson "%GOOGLE_APPLICATION_CREDENTIALS%" -StartDate "%START_DATE%" -EndDate "%END_DATE%" -ProjectId "%BIGQUERY_PROJECT_ID%" -Dataset "%BIGQUERY_DATASET%" -OutputDir "%BIGQUERY_SYNC_OUTPUT_DIR%" >> "%LOGFILE%" 2>&1
set "RC=%ERRORLEVEL%"

rem Task cadence is owned by the Task Scheduler repeating trigger registered in
rem install_bigquery_sync_autorun.ps1 — no finish-driven reschedule needed here.
set "RESCHEDULED=1"

if "!RC!"=="0" (
  echo [%date% %time%] bigquery sync finished rescheduled=!RESCHEDULED! rc=0>> "%LOGFILE%"
) else (
  echo [%date% %time%] bigquery sync finished rescheduled=!RESCHEDULED! rc=!RC!>> "%LOGFILE%"
)
exit /b !RC!
