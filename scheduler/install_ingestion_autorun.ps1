param(
    [string]$TaskName = "AirlineIntel_Ingestion4H",
    [string]$OnLogonTaskName = "AirlineIntel_IngestionOnLogon",
    [string]$StartTime = "",
    [int]$RepeatMinutes = 0,
    [switch]$SkipStartupShortcut,
    [switch]$SkipOnLogonTask,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batchPath = Join-Path $repoRoot "scheduler\run_ingestion_4h_once.bat"
$schedulePath = Join-Path $repoRoot "config\schedule.json"
if (-not (Test-Path $batchPath)) {
    throw "Ingestion wrapper not found: $batchPath"
}

function Parse-Time {
    param([string]$Value)
    try {
        return [datetime]::ParseExact($Value, "HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)
    }
    catch {
        throw "Invalid time format '$Value'. Expected HH:mm."
    }
}

function Load-ScheduleDefaults {
    if (-not (Test-Path $schedulePath)) {
        return @{}
    }
    try {
        $schedule = Get-Content $schedulePath -Raw | ConvertFrom-Json
    }
    catch {
        return @{}
    }
    $repeatMinutes = 360
    if ($schedule.task_windows.ingestion.repeat_minutes) {
        $repeatMinutes = [int]$schedule.task_windows.ingestion.repeat_minutes
    }
    elseif ($schedule.auto_run_interval_hours) {
        $repeatMinutes = [int]$schedule.auto_run_interval_hours * 60
    }
    return @{
        StartTime = [string]($schedule.task_windows.ingestion.start_time)
        RepeatMinutes = $repeatMinutes
    }
}

function Invoke-Schtasks {
    param(
        [string[]]$CmdArgs,
        [switch]$AllowFailure
    )
    if ($AllowFailure) {
        $prev = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & schtasks.exe @CmdArgs | Out-Host
        }
        finally {
            $ErrorActionPreference = $prev
        }
    } else {
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

function Register-IngestionTask {
    param(
        [string]$Name,
        [string]$TargetBatch,
        [datetime]$At
    )
    $now = Get-Date
    $anchor = Get-Date -Hour $At.Hour -Minute $At.Minute -Second 0
    if ($anchor -lt $now) {
        $anchor = $anchor.AddDays(1)
    }

    if ($WhatIf) {
        Write-Host "[WhatIf] Register-ScheduledTask -TaskName $Name (initial one-shot at $($anchor.ToString('yyyy-MM-dd HH:mm')))"
        return
    }

    $arg = "/c `"$TargetBatch`""
    $action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg
    $trigger = New-ScheduledTaskTrigger -Once -At $anchor

    $settings = New-ScheduledTaskSettingsSet `
        -WakeToRun `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Hours 8)

    $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
    Register-ScheduledTask -TaskName $Name -InputObject $task -Force | Out-Null
    Write-Host "Ingestion repeat task ensured: $Name"
}

function Register-OnLogonKickoff {
    param(
        [string]$Name,
        [string]$TargetBatch
    )

    $cmd = "`"$TargetBatch`""
    $args = @(
        "/Create",
        "/TN", $Name,
        "/SC", "ONLOGON",
        "/TR", $cmd,
        "/F"
    )
    if ($WhatIf) {
        Write-Host "[WhatIf] schtasks $($args -join ' ')"
        return
    }

    $ok = Invoke-Schtasks -CmdArgs $args -AllowFailure
    if ($ok) {
        Write-Host "On-logon ingestion kickoff ensured: $Name"
        return $true
    }
    Write-Warning "On-logon ingestion kickoff could not be created for '$Name' (permission policy). The 4-hour recurring task is still active."
    return $false
}

function Ensure-StartupShortcut {
    param([string]$TargetBatch)
    $startupDir = [Environment]::GetFolderPath("Startup")
    $shortcutPath = Join-Path $startupDir "AirlineIntel Ingestion Kickoff.lnk"
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
    $shortcut.Description = "AirlineIntel ingestion kickoff launcher"
    $shortcut.Save()
    Write-Host "Startup shortcut ensured: $shortcutPath"
}

function Show-TaskSummary {
    param([string]$Name)
    if ($WhatIf) {
        return
    }
    Write-Host ""
    & schtasks.exe /Query /TN $Name /FO LIST /V | Select-String -Pattern "TaskName:|Status:|Next Run Time:|Repeat: Every:|Task To Run:|Run As User:|Logon Mode:" | ForEach-Object {
        Write-Host "  $($_.Line.Trim())"
    }
}

$scheduleDefaults = Load-ScheduleDefaults
if (-not $StartTime) {
    $StartTime = if ($scheduleDefaults.StartTime) { $scheduleDefaults.StartTime } else { "00:05" }
}
if ($RepeatMinutes -le 0) {
    $RepeatMinutes = if ($scheduleDefaults.RepeatMinutes -ge 60) { $scheduleDefaults.RepeatMinutes } else { 360 }
}
if ($RepeatMinutes -lt 60) {
    throw "RepeatMinutes must be >= 60"
}
$startAt = Parse-Time $StartTime

Register-IngestionTask -Name $TaskName -TargetBatch $batchPath -At $startAt
$onLogonOk = $false
if (-not $SkipOnLogonTask) {
    $onLogonOk = Register-OnLogonKickoff -Name $OnLogonTaskName -TargetBatch $batchPath
}
if (-not $SkipStartupShortcut -and (-not $onLogonOk)) {
    Ensure-StartupShortcut -TargetBatch $batchPath
}

Show-TaskSummary -Name $TaskName
if (-not $SkipOnLogonTask -and $onLogonOk) {
    Show-TaskSummary -Name $OnLogonTaskName
}

if (-not $WhatIf) {
    Write-Host ""
    Write-Host "Done. Ingestion autorun is installed for current user context."
    Write-Host "This task is finish-driven: the wrapper reschedules the next run after completion + buffer."
    Write-Host "Main command:"
    Write-Host "  $batchPath"
}
