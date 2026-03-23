param(
    [string]$TaskName = "AirlineIntel_Ingestion4H",
    [string]$OnLogonTaskName = "AirlineIntel_IngestionOnLogon",
    [string]$ShortcutName = "AirlineIntel Ingestion Kickoff.lnk",
    [switch]$SkipPrimaryTask,
    [switch]$SkipOnLogonTask,
    [switch]$SkipStartupShortcut,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$SchtasksExe = Join-Path $env:SystemRoot "System32\schtasks.exe"
if (-not (Test-Path $SchtasksExe)) {
    $SchtasksExe = "schtasks.exe"
}

function Remove-SchtaskIfPresent {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ($WhatIf) {
        Write-Host "[WhatIf] schtasks /Delete /TN $Name /F"
        return
    }

    $queryArgs = @("/Query", "/TN", $Name)
    & $SchtasksExe @queryArgs *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Task not present: $Name"
        return
    }

    & $SchtasksExe /Delete /TN $Name /F | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to delete scheduled task '$Name' (rc=$LASTEXITCODE)"
    }
    Write-Host "Removed scheduled task: $Name"
}

function Remove-StartupShortcutIfPresent {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $startupDir = [Environment]::GetFolderPath("Startup")
    $shortcutPath = Join-Path $startupDir $Name
    if (-not (Test-Path $shortcutPath)) {
        Write-Host "Startup shortcut not present: $shortcutPath"
        return
    }

    if ($WhatIf) {
        Write-Host "[WhatIf] Remove-Item $shortcutPath -Force"
        return
    }

    Remove-Item $shortcutPath -Force
    Write-Host "Removed startup shortcut: $shortcutPath"
}

if (-not $SkipPrimaryTask) {
    Remove-SchtaskIfPresent -Name $TaskName
}

if (-not $SkipOnLogonTask) {
    Remove-SchtaskIfPresent -Name $OnLogonTaskName
}

if (-not $SkipStartupShortcut) {
    Remove-StartupShortcutIfPresent -Name $ShortcutName
}

if (-not $WhatIf) {
    Write-Host ""
    Write-Host "Done. Ingestion autorun disable completed for current user context."
}
