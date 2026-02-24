@echo off
setlocal
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
"%ROOT%\.venv\Scripts\python.exe" "%ROOT%\scheduler\always_on_maintenance.py" --python-exe "%ROOT%\.venv\Scripts\python.exe" --reports-dir "%ROOT%\output\reports" --run-on-start --once >> "%ROOT%\logs\maintenance_pulse.log" 2>&1
endlocal
