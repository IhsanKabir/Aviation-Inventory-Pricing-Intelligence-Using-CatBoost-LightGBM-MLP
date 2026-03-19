param(
    [string]$TaskName = "AirlineIntel_Ingestion4H",
    [string]$OnLogonTaskName = "AirlineIntel_IngestionOnLogon",
    [switch]$KeepStartupShortcut,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batchPath = Join-Path $repoRoot "scheduler\run_ingestion_4h_once.bat"
$startupDir = [Environment]::GetFolderPath("Startup")
$shortcutPath = Join-Path $startupDir "AirlineIntel Ingestion Kickoff.lnk"

function Invoke-Schtasks {
    param(
        [string[]]$CmdArgs,
        [switch]$AllowFailure
    )

    if ($WhatIf) {
        Write-Host "[WhatIf] schtasks $($CmdArgs -join ' ')"
        return $true
    }

    if ($AllowFailure) {
        $prev = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & schtasks.exe @CmdArgs | Out-Host
        }
        finally {
            $ErrorActionPreference = $prev
        }
    }
    else {
        & schtasks.exe @CmdArgs | Out-Host
    }

    $rc = $LASTEXITCODE
    if ($rc -ne 0) {
        if ($AllowFailure) {
            return $false
        }
        throw "schtasks failed (rc=$rc): $($CmdArgs -join ' ')"
    }
    return $true
}

function Disable-TaskIfPresent {
    param([string]$Name)

    $queryOk = Invoke-Schtasks -CmdArgs @("/Query", "/TN", $Name) -AllowFailure
    if (-not $queryOk) {
        Write-Host "Task not present, skipping: $Name"
        return
    }

    $disableOk = Invoke-Schtasks -CmdArgs @("/Change", "/TN", $Name, "/Disable") -AllowFailure
    if ($disableOk) {
        if ($WhatIf) {
            Write-Host "Would disable task: $Name"
        }
        else {
            Write-Host "Disabled task: $Name"
        }
    }
    else {
        Write-Warning "Could not disable task: $Name"
    }
}

Disable-TaskIfPresent -Name $TaskName
Disable-TaskIfPresent -Name $OnLogonTaskName

if (-not $KeepStartupShortcut) {
    if ($WhatIf) {
        Write-Host "[WhatIf] Remove startup shortcut $shortcutPath"
    }
    elseif (Test-Path $shortcutPath) {
        Remove-Item $shortcutPath -Force
        Write-Host "Removed startup shortcut: $shortcutPath"
    }
}

Write-Host ""
Write-Host "Operational ingestion is now set to manual mode on this machine."
Write-Host "Manual launch command:"
Write-Host "  cmd /c `"$batchPath`""
