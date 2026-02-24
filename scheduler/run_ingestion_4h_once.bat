@echo off
setlocal
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\output\reports" mkdir "%ROOT%\output\reports"

set "PYEXE=%ROOT%\.venv\Scripts\python.exe"
set "LOGFILE=%ROOT%\logs\ingestion_4h.log"

if not exist "%PYEXE%" (
  echo [%date% %time%] python exe not found: %PYEXE%>> "%LOGFILE%"
  exit /b 1
)

echo [%date% %time%] starting ingestion cycle>> "%LOGFILE%"
"%PYEXE%" "%ROOT%\run_pipeline.py" --python-exe "%PYEXE%" --skip-reports --report-output-dir "%ROOT%\output\reports" --report-timestamp-tz local >> "%LOGFILE%" 2>&1
set "RC=%ERRORLEVEL%"
echo [%date% %time%] ingestion cycle finished rc=%RC%>> "%LOGFILE%"
exit /b %RC%
