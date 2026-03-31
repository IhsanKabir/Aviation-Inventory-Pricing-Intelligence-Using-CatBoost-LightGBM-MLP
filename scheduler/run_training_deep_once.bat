@echo off
setlocal EnableDelayedExpansion
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\output\reports" mkdir "%ROOT%\output\reports"

set "PYEXE=%ROOT%\.venv\Scripts\python.exe"
set "LOGFILE=%ROOT%\logs\training_deep.log"
set "RECOVERY_HELPER=%ROOT%\tools\recover_interrupted_accumulation.py"
set "RECOVERY_STATUS=%ROOT%\output\reports\accumulation_recovery_latest.json"
set "CYCLE_STATE=%ROOT%\output\reports\accumulation_cycle_latest.json"
set "ENVFILE=%ROOT%\.env"
set "RUN_ALL_TRIP_PLAN_MODE=deep"

if not exist "%PYEXE%" (
  echo [%date% %time%] python exe not found: %PYEXE%>> "%LOGFILE%"
  exit /b 1
)

if exist "%ENVFILE%" (
  for /f "usebackq eol=# tokens=1* delims==" %%A in ("%ENVFILE%") do (
    if /I "%%~A"=="BIGQUERY_PROJECT_ID" set "BIGQUERY_PROJECT_ID=%%~B"
    if /I "%%~A"=="BIGQUERY_DATASET" set "BIGQUERY_DATASET=%%~B"
    if /I "%%~A"=="GOOGLE_APPLICATION_CREDENTIALS" set "GOOGLE_APPLICATION_CREDENTIALS=%%~B"
    if /I "%%~A"=="DEEP_COMPLETION_BUFFER_MINUTES" set "DEEP_COMPLETION_BUFFER_MINUTES=%%~B"
    if /I "%%~A"=="TRAINING_COMPLETION_BUFFER_MINUTES" set "TRAINING_COMPLETION_BUFFER_MINUTES=%%~B"
    if /I "%%~A"=="ACCUMULATION_COMPLETION_BUFFER_MINUTES" set "ACCUMULATION_COMPLETION_BUFFER_MINUTES=%%~B"
    if /I "%%~A"=="DEEP_PREDICTION_ML_MODELS" set "DEEP_PREDICTION_ML_MODELS=%%~B"
    if /I "%%~A"=="DEEP_PREDICTION_DL_MODELS" set "DEEP_PREDICTION_DL_MODELS=%%~B"
    if /I "%%~A"=="DEEP_SKIP_BIGQUERY_SYNC" set "DEEP_SKIP_BIGQUERY_SYNC=%%~B"
  )
)

if not defined DEEP_COMPLETION_BUFFER_MINUTES set "DEEP_COMPLETION_BUFFER_MINUTES=%TRAINING_COMPLETION_BUFFER_MINUTES%"
if not defined DEEP_COMPLETION_BUFFER_MINUTES set "DEEP_COMPLETION_BUFFER_MINUTES=%ACCUMULATION_COMPLETION_BUFFER_MINUTES%"
if not defined DEEP_COMPLETION_BUFFER_MINUTES set "DEEP_COMPLETION_BUFFER_MINUTES=120"
if not defined DEEP_PREDICTION_ML_MODELS set "DEEP_PREDICTION_ML_MODELS=catboost,lightgbm"
if not defined DEEP_PREDICTION_DL_MODELS set "DEEP_PREDICTION_DL_MODELS=mlp"
if not defined DEEP_SKIP_BIGQUERY_SYNC set "DEEP_SKIP_BIGQUERY_SYNC=1"

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
  "%PYEXE%" "%RECOVERY_HELPER%" --mode preflight --python-exe "%PYEXE%" --root "%ROOT%" --reports-dir "%ROOT%\output\reports" --min-completed-gap-minutes "%DEEP_COMPLETION_BUFFER_MINUTES%" >> "%LOGFILE%" 2>&1
  set "PRE_RC=%ERRORLEVEL%"
  if exist "%RECOVERY_STATUS%" if exist "%CYCLE_STATE%" (
    powershell -NoProfile -Command "$p = Get-Content -Raw '%RECOVERY_STATUS%' | ConvertFrom-Json; $c = Get-Content -Raw '%CYCLE_STATE%' | ConvertFrom-Json; $msg = ('[{0} {1}] deep wrapper result: event={2} state={3} reason={4} cycle_id={5} launched={6} db_ok={7} rc={8}' -f (Get-Date -Format 'ddd MM/dd/yyyy'), (Get-Date -Format 'HH:mm:ss.ff'), $p.wrapper_event, $c.state, $p.reason, $c.cycle_id, $p.launched, $p.db_check.ok, '!PRE_RC!'); Add-Content -Path '%LOGFILE%' -Value $msg" >nul 2>&1
  )
  if "!PRE_RC!"=="10" (
    echo [%date% %time%] deep wrapper result: event=skipped_active_run rc=0>> "%LOGFILE%"
    exit /b 0
  )
  if "!PRE_RC!"=="11" (
    echo [%date% %time%] deep wrapper result: event=skipped_buffer rc=0>> "%LOGFILE%"
    exit /b 0
  )
  if "!PRE_RC!"=="12" (
    echo [%date% %time%] deep wrapper result: event=skipped_db_unavailable rc=0>> "%LOGFILE%"
    exit /b 0
  )
  if not "!PRE_RC!"=="0" (
    echo [%date% %time%] deep wrapper result: event=preflight_warning rc=!PRE_RC! (continuing)>> "%LOGFILE%"
  )
)

echo [%date% %time%] starting deep training cycle>> "%LOGFILE%"
if /I "%DEEP_SKIP_BIGQUERY_SYNC%"=="1" (
  "%PYEXE%" "%ROOT%\run_pipeline.py" --python-exe "%PYEXE%" --trip-plan-mode deep --report-format xlsx --report-output-dir "%ROOT%\output\reports" --report-timestamp-tz local --prediction-ml-models "%DEEP_PREDICTION_ML_MODELS%" --prediction-dl-models "%DEEP_PREDICTION_DL_MODELS%" --skip-bigquery-sync >> "%LOGFILE%" 2>&1
) else (
  "%PYEXE%" "%ROOT%\run_pipeline.py" --python-exe "%PYEXE%" --trip-plan-mode deep --report-format xlsx --report-output-dir "%ROOT%\output\reports" --report-timestamp-tz local --prediction-ml-models "%DEEP_PREDICTION_ML_MODELS%" --prediction-dl-models "%DEEP_PREDICTION_DL_MODELS%" >> "%LOGFILE%" 2>&1
)
set "RC=%ERRORLEVEL%"
if "!RC!"=="0" (
  echo [%date% %time%] deep wrapper result: event=wrapper_finished_success rc=0>> "%LOGFILE%"
) else (
  echo [%date% %time%] deep wrapper result: event=wrapper_finished_failure rc=!RC!>> "%LOGFILE%"
)
exit /b !RC!
