@echo off
setlocal EnableDelayedExpansion
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\output\reports" mkdir "%ROOT%\output\reports"

set "PYEXE=%ROOT%\.venv\Scripts\python.exe"
set "LOGFILE=%ROOT%\logs\route_monitor_report.log"
set "RECOVERY_HELPER=%ROOT%\tools\recover_interrupted_accumulation.py"

if not exist "%PYEXE%" (
  echo [%date% %time%] python exe not found: %PYEXE%>> "%LOGFILE%"
  exit /b 1
)

if exist "%RECOVERY_HELPER%" (
  "%PYEXE%" "%RECOVERY_HELPER%" --mode preflight --python-exe "%PYEXE%" --root "%ROOT%" --reports-dir "%ROOT%\output\reports" >> "%LOGFILE%" 2>&1
  set "PRE_RC=!ERRORLEVEL!"
  if "!PRE_RC!"=="10" (
    echo [%date% %time%] route-monitor report skipped: accumulation pipeline already running>> "%LOGFILE%"
    exit /b 0
  )
  if not "!PRE_RC!"=="0" (
    echo [%date% %time%] route-monitor preflight warning rc=!PRE_RC! (continuing)>> "%LOGFILE%"
  )
)

echo [%date% %time%] starting route-monitor report cycle>> "%LOGFILE%"
"%PYEXE%" "%ROOT%\generate_route_flight_fare_monitor.py" --output-dir "%ROOT%\output\reports" --timestamp-tz local --style compact >> "%LOGFILE%" 2>&1
set "RC=!ERRORLEVEL!"
echo [%date% %time%] route-monitor report cycle finished rc=!RC!>> "%LOGFILE%"
exit /b !RC!
