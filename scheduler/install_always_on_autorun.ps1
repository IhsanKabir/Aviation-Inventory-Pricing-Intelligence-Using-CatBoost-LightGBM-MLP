param(
    [string]$TaskName = "AirlineIntel_AlwaysOn",
    [string]$PulseTaskName = "AirlineIntel_MaintenancePulse",
    [int]$PulseMinutes = 30,
    [switch]$SkipStartupShortcut,
    [switch]$SkipOnLogonTask,
    [switch]$SkipPulseTask,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batchPath = Join-Path $repoRoot "scheduler\start_always_on_maintenance.bat"
$pulseBatchPath = Join-Path $repoRoot "scheduler\run_maintenance_pulse_once.bat"
if (-not (Test-Path $batchPath)) {
    throw "Batch launcher not found: $batchPath"
}
if (-not (Test-Path $pulseBatchPath)) {
    throw "Pulse batch launcher not found: $pulseBatchPath"
}
if ($PulseMinutes -lt 5) {
    throw "PulseMinutes must be >= 5"
}

function Invoke-Schtasks {
    param(
        [string[]]$CmdArgs,
        [switch]$AllowFailure
    )
    & schtasks.exe @CmdArgs | Out-Host
    $rc = $LASTEXITCODE
    if ($rc -ne 0) {
        if ($AllowFailure) {
            return $false
        }
        throw "schtasks failed (rc=$rc): $($CmdArgs -join ' ')"
    }
    return $true
}

function Ensure-StartupShortcut {
    param([string]$TargetBatch)
    $startupDir = [Environment]::GetFolderPath("Startup")
    $shortcutPath = Join-Path $startupDir "AirlineIntel AlwaysOn.lnk"
    if ($WhatIf) {
        Write-Host "[WhatIf] Create startup shortcut -> $shortcutPath"
        return
    }
    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = "cmd.exe"
    $shortcut.Arguments = "/c `"$TargetBatch`""
    $shortcut.WorkingDirectory = Split-Path $TargetBatch -Parent
    $shortcut.WindowStyle = 7
    $shortcut.IconLocation = "$env:SystemRoot\System32\shell32.dll,1"
    $shortcut.Description = "AirlineIntel always-on maintenance launcher"
    $shortcut.Save()
    Write-Host "Startup shortcut created: $shortcutPath"
}

function Ensure-OnLogonTask {
    param([string]$Name, [string]$TargetBatch)
    $cmd = "`"$TargetBatch`""
    $args = @("/Create", "/TN", $Name, "/SC", "ONLOGON", "/TR", $cmd, "/F")
    if ($WhatIf) {
        Write-Host "[WhatIf] schtasks $($args -join ' ')"
        return $true
    }
    $ok = Invoke-Schtasks -CmdArgs $args -AllowFailure
    if ($ok) {
        Write-Host "ONLOGON task ensured: $Name"
        return $true
    }
    Write-Warning "ONLOGON task could not be created (likely permission policy). Startup shortcut and pulse task will still auto-run while logged in."
    return $false
}

function Ensure-PulseTask {
    param([string]$Name, [string]$TargetBatch, [int]$EveryMinutes)
    $cmd = "`"$TargetBatch`""
    $args = @(
        "/Create",
        "/TN", $Name,
        "/SC", "DAILY",
        "/ST", "00:00",
        "/RI", "$EveryMinutes",
        "/DU", "23:59",
        "/TR", $cmd,
        "/F"
    )
    if ($WhatIf) {
        Write-Host "[WhatIf] schtasks $($args -join ' ')"
        return $true
    }
    $ok = Invoke-Schtasks -CmdArgs $args -AllowFailure
    if ($ok) {
        Write-Host "Pulse task ensured ($EveryMinutes min): $Name"
        return $true
    }
    Write-Warning "Pulse task could not be created."
    return $false
}

$onLogonOk = $false
$pulseOk = $false

if (-not $SkipStartupShortcut) {
    Ensure-StartupShortcut -TargetBatch $batchPath
}
if (-not $SkipOnLogonTask) {
    $onLogonOk = Ensure-OnLogonTask -Name $TaskName -TargetBatch $batchPath
}
if (-not $SkipPulseTask) {
    $pulseOk = Ensure-PulseTask -Name $PulseTaskName -TargetBatch $pulseBatchPath -EveryMinutes $PulseMinutes
}

if (-not $WhatIf) {
    Write-Host ""
    if (-not $SkipOnLogonTask -and $onLogonOk) {
        Invoke-Schtasks -CmdArgs @("/Query", "/TN", $TaskName, "/FO", "LIST", "/V") -AllowFailure | Out-Null
    }
    if (-not $SkipPulseTask -and $pulseOk) {
        Invoke-Schtasks -CmdArgs @("/Query", "/TN", $PulseTaskName, "/FO", "LIST", "/V") -AllowFailure | Out-Null
    }
    Write-Host "Done. Always-on autorun installed for current user context."
    Write-Host "Coverage:"
    Write-Host "- Startup shortcut: $(if ($SkipStartupShortcut) {'skipped'} else {'enabled'})"
    Write-Host "- ONLOGON task: $(if ($SkipOnLogonTask) {'skipped'} elseif ($onLogonOk) {'enabled'} else {'not available'})"
    Write-Host "- Pulse task: $(if ($SkipPulseTask) {'skipped'} elseif ($pulseOk) {'enabled'} else {'not available'})"
}
