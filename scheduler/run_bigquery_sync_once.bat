@echo off
setlocal EnableDelayedExpansion
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\output\reports" mkdir "%ROOT%\output\reports"

set "LOGFILE=%ROOT%\logs\bigquery_sync.log"
set "LOADER=%ROOT%\tools\load_bigquery_latest.ps1"
set "RESCHEDULER=%ROOT%\scheduler\reschedule_finish_driven_task.ps1"
set "TASK_NAME=AirlineIntel_BigQuerySync"
set "ENVFILE=%ROOT%\.env"

set "BIGQUERY_PROJECT_ID="
set "BIGQUERY_DATASET="
set "GOOGLE_APPLICATION_CREDENTIALS="
set "BIGQUERY_SYNC_LOOKBACK_DAYS="
set "BIGQUERY_SYNC_OUTPUT_DIR="
set "BIGQUERY_SYNC_BUFFER_MINUTES="

if exist "%ENVFILE%" (
  for /f "usebackq eol=# tokens=1* delims==" %%A in ("%ENVFILE%") do (
    if /I "%%~A"=="BIGQUERY_PROJECT_ID" set "BIGQUERY_PROJECT_ID=%%~B"
    if /I "%%~A"=="BIGQUERY_DATASET" set "BIGQUERY_DATASET=%%~B"
    if /I "%%~A"=="GOOGLE_APPLICATION_CREDENTIALS" set "GOOGLE_APPLICATION_CREDENTIALS=%%~B"
    if /I "%%~A"=="BIGQUERY_SYNC_LOOKBACK_DAYS" set "BIGQUERY_SYNC_LOOKBACK_DAYS=%%~B"
    if /I "%%~A"=="BIGQUERY_SYNC_OUTPUT_DIR" set "BIGQUERY_SYNC_OUTPUT_DIR=%%~B"
    if /I "%%~A"=="BIGQUERY_SYNC_BUFFER_MINUTES" set "BIGQUERY_SYNC_BUFFER_MINUTES=%%~B"
  )
)

for /f %%I in ('powershell -NoProfile -Command "$p = 'config/schedule.json'; if (Test-Path $p) { try { $s = Get-Content $p -Raw | ConvertFrom-Json; [int]($s.task_windows.bigquery_sync.repeat_minutes) } catch { '' } }"') do set "SCHEDULE_SYNC_REPEAT_MINUTES=%%I"

if not defined BIGQUERY_SYNC_LOOKBACK_DAYS set "BIGQUERY_SYNC_LOOKBACK_DAYS=7"
if not defined BIGQUERY_SYNC_OUTPUT_DIR set "BIGQUERY_SYNC_OUTPUT_DIR=output/warehouse/bigquery"
if not defined BIGQUERY_SYNC_BUFFER_MINUTES if defined SCHEDULE_SYNC_REPEAT_MINUTES set "BIGQUERY_SYNC_BUFFER_MINUTES=%SCHEDULE_SYNC_REPEAT_MINUTES%"
if not defined BIGQUERY_SYNC_BUFFER_MINUTES set "BIGQUERY_SYNC_BUFFER_MINUTES=180"

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

set "RESCHEDULED=0"
if exist "%RESCHEDULER%" (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%RESCHEDULER%" -TaskName "%TASK_NAME%" -BatchPath "%~f0" -DelayMinutes %BIGQUERY_SYNC_BUFFER_MINUTES% -ExecutionTimeLimitHours 4 >> "%LOGFILE%" 2>&1
  if "!ERRORLEVEL!"=="0" set "RESCHEDULED=1"
)

if "!RC!"=="0" (
  echo [%date% %time%] bigquery sync finished rescheduled=!RESCHEDULED! rc=0>> "%LOGFILE%"
) else (
  echo [%date% %time%] bigquery sync finished rescheduled=!RESCHEDULED! rc=!RC!>> "%LOGFILE%"
)
exit /b !RC!
