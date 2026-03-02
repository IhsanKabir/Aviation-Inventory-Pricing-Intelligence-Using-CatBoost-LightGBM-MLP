param(
    [string]$PythonExe = "python",
    [string]$VenvDir = ".venv"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $VenvDir)) {
    Write-Host "Creating virtual environment at $VenvDir ..."
    & $PythonExe -m venv $VenvDir
}

$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Virtual environment python not found: $VenvPython"
}

Write-Host "Upgrading pip/setuptools/wheel ..."
& $VenvPython -m pip install --upgrade pip setuptools wheel

$ReqLock = "requirements-lock.txt"
$ReqBase = "requirements.txt"
if (Test-Path $ReqLock) {
    Write-Host "Installing locked dependencies from $ReqLock ..."
    & $VenvPython -m pip install -r $ReqLock
}
elseif (Test-Path $ReqBase) {
    Write-Host "Installing dependencies from $ReqBase ..."
    & $VenvPython -m pip install -r $ReqBase
}
else {
    throw "No requirements file found."
}

Write-Host "Setup complete."
Write-Host "Next checks:"
Write-Host "  $VenvPython -m py_compile run_all.py run_pipeline.py predict_next_day.py"
Write-Host "  $VenvPython scheduler/maintenance_tasks.py --task daily_ops"
Write-Host "If OTA token auto-refresh is used:"
Write-Host "  $VenvPython -m playwright install chromium"
