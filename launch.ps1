<#
.SYNOPSIS
    Start the OTA Discount Comparison desktop app.

.DESCRIPTION
    Default: run from source with the project's .venv (always the current code).
    -Exe:    run the newest built OTADiscountReport.exe instead (dist\ or E:\tmp\ota_dist_*).
    Any other arguments are passed to the app, e.g.  .\launch.ps1 --selftest

.EXAMPLE
    .\launch.ps1
.EXAMPLE
    .\launch.ps1 -Exe
#>
[CmdletBinding()]
param(
    [switch]$Exe,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$AppArgs
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
Set-Location $root

if ($Exe) {
    $candidates = @(Get-ChildItem -Path (Join-Path $root "dist"), "E:\tmp" -Recurse -Filter "OTADiscountReport.exe" `
                        -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending)
    if (-not $candidates) {
        Write-Error "No built OTADiscountReport.exe found. Build it first: .\.venv\Scripts\pyinstaller.exe --noconfirm desktop\build.spec"
    }
    $path = $candidates[0].FullName
    Write-Host "Starting $path"
    Start-Process -FilePath $path -ArgumentList $AppArgs -WorkingDirectory (Split-Path $path)
    return
}

$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    Write-Error "No virtual environment at .venv. Create it first: py -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}
Write-Host "Starting the app from source ($python launcher.py)"
& $python (Join-Path $root "launcher.py") @AppArgs
exit $LASTEXITCODE
