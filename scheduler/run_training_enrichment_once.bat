@echo off
setlocal EnableDelayedExpansion
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\output\reports" mkdir "%ROOT%\output\reports"

set "PYEXE=%ROOT%\.venv\Scripts\python.exe"
set "LOGFILE=%ROOT%\logs\training_enrichment.log"
set "RECOVERY_HELPER=%ROOT%\tools\recover_interrupted_accumulation.py"
set "ENVFILE=%ROOT%\.env"
set "RUN_ALL_TRIP_PLAN_MODE=training"

if not exist "%PYEXE%" (
  echo [%date% %time%] python exe not found: %PYEXE%>> "%LOGFILE%"
  exit /b 1
)

if exist "%ENVFILE%" (
  for /f "usebackq eol=# tokens=1* delims==" %%A in ("%ENVFILE%") do (
    if /I "%%~A"=="BIGQUERY_PROJECT_ID" set "BIGQUERY_PROJECT_ID=%%~B"
    if /I "%%~A"=="BIGQUERY_DATASET" set "BIGQUERY_DATASET=%%~B"
    if /I "%%~A"=="GOOGLE_APPLICATION_CREDENTIALS" set "GOOGLE_APPLICATION_CREDENTIALS=%%~B"
    if /I "%%~A"=="TRAINING_COMPLETION_BUFFER_MINUTES" set "TRAINING_COMPLETION_BUFFER_MINUTES=%%~B"
    if /I "%%~A"=="ACCUMULATION_COMPLETION_BUFFER_MINUTES" set "ACCUMULATION_COMPLETION_BUFFER_MINUTES=%%~B"
    if /I "%%~A"=="TRAINING_PREDICTION_ML_MODELS" set "TRAINING_PREDICTION_ML_MODELS=%%~B"
    if /I "%%~A"=="TRAINING_PREDICTION_DL_MODELS" set "TRAINING_PREDICTION_DL_MODELS=%%~B"
    if /I "%%~A"=="TRAINING_SKIP_BIGQUERY_SYNC" set "TRAINING_SKIP_BIGQUERY_SYNC=%%~B"
  )
)

if not defined TRAINING_COMPLETION_BUFFER_MINUTES set "TRAINING_COMPLETION_BUFFER_MINUTES=%ACCUMULATION_COMPLETION_BUFFER_MINUTES%"
if not defined TRAINING_COMPLETION_BUFFER_MINUTES set "TRAINING_COMPLETION_BUFFER_MINUTES=120"
if not defined TRAINING_PREDICTION_ML_MODELS set "TRAINING_PREDICTION_ML_MODELS=catboost,lightgbm"
if not defined TRAINING_PREDICTION_DL_MODELS set "TRAINING_PREDICTION_DL_MODELS=mlp"
if not defined TRAINING_SKIP_BIGQUERY_SYNC set "TRAINING_SKIP_BIGQUERY_SYNC=0"

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
  "%PYEXE%" "%RECOVERY_HELPER%" --mode preflight --python-exe "%PYEXE%" --root "%ROOT%" --reports-dir "%ROOT%\output\reports" --min-completed-gap-minutes "%TRAINING_COMPLETION_BUFFER_MINUTES%" >> "%LOGFILE%" 2>&1
  set "PRE_RC=%ERRORLEVEL%"
  if "!PRE_RC!"=="10" (
    echo [%date% %time%] training enrichment skipped: active or fresh accumulation already present>> "%LOGFILE%"
    exit /b 0
  )
  if "!PRE_RC!"=="11" (
    echo [%date% %time%] training enrichment skipped: %TRAINING_COMPLETION_BUFFER_MINUTES% minute post-completion buffer is active>> "%LOGFILE%"
    exit /b 0
  )
  if "!PRE_RC!"=="12" (
    echo [%date% %time%] training enrichment skipped: PostgreSQL is unavailable>> "%LOGFILE%"
    exit /b 0
  )
  if not "!PRE_RC!"=="0" (
    echo [%date% %time%] training preflight warning rc=!PRE_RC! (continuing)>> "%LOGFILE%"
  )
)

echo [%date% %time%] starting training enrichment cycle>> "%LOGFILE%"
if /I "%TRAINING_SKIP_BIGQUERY_SYNC%"=="1" (
  "%PYEXE%" "%ROOT%\run_pipeline.py" --python-exe "%PYEXE%" --trip-plan-mode training --skip-reports --report-output-dir "%ROOT%\output\reports" --report-timestamp-tz local --prediction-ml-models "%TRAINING_PREDICTION_ML_MODELS%" --prediction-dl-models "%TRAINING_PREDICTION_DL_MODELS%" --skip-bigquery-sync >> "%LOGFILE%" 2>&1
) else (
  "%PYEXE%" "%ROOT%\run_pipeline.py" --python-exe "%PYEXE%" --trip-plan-mode training --skip-reports --report-output-dir "%ROOT%\output\reports" --report-timestamp-tz local --prediction-ml-models "%TRAINING_PREDICTION_ML_MODELS%" --prediction-dl-models "%TRAINING_PREDICTION_DL_MODELS%" >> "%LOGFILE%" 2>&1
)
set "RC=%ERRORLEVEL%"
echo [%date% %time%] training enrichment cycle finished rc=!RC!>> "%LOGFILE%"
exit /b !RC!
