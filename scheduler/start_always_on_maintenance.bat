@echo off
setlocal
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
if not exist "%ROOT%\output\reports" mkdir "%ROOT%\output\reports"

set "PYEXE=%ROOT%\.venv\Scripts\python.exe"
set "SCRIPT=%ROOT%\scheduler\always_on_maintenance.py"
set "LOGFILE=%ROOT%\logs\always_on_maintenance.log"

if not exist "%PYEXE%" (
  echo [%date% %time%] python exe not found: %PYEXE%>> "%LOGFILE%"
  exit /b 1
)
if not exist "%SCRIPT%" (
  echo [%date% %time%] script not found: %SCRIPT%>> "%LOGFILE%"
  exit /b 1
)

echo [%date% %time%] starting always_on_maintenance>> "%LOGFILE%"
"%PYEXE%" "%SCRIPT%" --python-exe "%PYEXE%" --reports-dir "%ROOT%\output\reports" --poll-minutes 10 --weekly-day SUN --run-on-start >> "%LOGFILE%" 2>&1
endlocal
