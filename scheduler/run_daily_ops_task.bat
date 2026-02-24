@echo off
setlocal
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
"%ROOT%\.venv\Scripts\python.exe" "%ROOT%\scheduler\maintenance_tasks.py" --task daily_ops --reports-dir "%ROOT%\output\reports" --timestamp-tz local >> "%ROOT%\logs\maintenance_daily_ops.log" 2>&1
endlocal
