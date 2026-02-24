@echo off
setlocal
set "ROOT=%~dp0.."
if not exist "%ROOT%\logs" mkdir "%ROOT%\logs"
"%ROOT%\.venv\Scripts\python.exe" "%ROOT%\scheduler\maintenance_tasks.py" --task weekly_pack --reports-dir "%ROOT%\output\reports" --timestamp-tz local >> "%ROOT%\logs\maintenance_weekly_pack.log" 2>&1
endlocal
