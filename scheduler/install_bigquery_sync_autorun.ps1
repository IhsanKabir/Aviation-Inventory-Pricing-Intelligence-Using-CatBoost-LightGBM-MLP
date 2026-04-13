param(
    [string]$TaskName = "AirlineIntel_BigQuerySync",
    [string]$StartTime = "",
    [int]$RepeatMinutes = 0,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batchPath = Join-Path $repoRoot "scheduler\run_bigquery_sync_once.bat"
$schedulePath = Join-Path $repoRoot "config\schedule.json"
if (-not (Test-Path $batchPath)) {
    throw "BigQuery sync wrapper not found: $batchPath"
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
    return @{
        StartTime = [string]($schedule.task_windows.bigquery_sync.start_time)
        RepeatMinutes = [int]($schedule.task_windows.bigquery_sync.repeat_minutes)
    }
}

function Register-SyncTask {
    param(
        [string]$Name,
        [string]$TargetBatch,
        [datetime]$At,
        [int]$RepeatMinutes
    )
    $now = Get-Date
    $anchor = Get-Date -Hour $At.Hour -Minute $At.Minute -Second 0
    # If today's anchor has already passed, anchor to today anyway — StartWhenAvailable
    # will fire the first run immediately, then the repetition interval takes over.
    if ($anchor -lt $now) {
        # Keep today's anchor so the repetition series is rooted to the configured time.
        # Do not push to tomorrow — that would delay the first catch-up run unnecessarily.
    }

    if ($WhatIf) {
        Write-Host "[WhatIf] Register-ScheduledTask -TaskName $Name (repeating every $RepeatMinutes min, anchored $($anchor.ToString('yyyy-MM-dd HH:mm')))"
        return
    }

    $arg = "/c `"$TargetBatch`""
    $action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg
    # Fixed repeating trigger — fires at anchor then every RepeatMinutes forever.
    # No finish-driven reschedule needed; Task Scheduler owns the cadence.
    $trigger = New-ScheduledTaskTrigger -Once -At $anchor `
        -RepetitionInterval (New-TimeSpan -Minutes $RepeatMinutes) `
        -RepetitionDuration (New-TimeSpan -Days 3650)
    $settings = New-ScheduledTaskSettingsSet `
        -WakeToRun `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Hours 4)

    $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
    Register-ScheduledTask -TaskName $Name -InputObject $task -Force | Out-Null
    Write-Host "BigQuery sync task registered: $Name (repeating every $RepeatMinutes min)"
}

function Show-TaskSummary {
    param([string]$Name)
    if ($WhatIf) {
        return
    }
    Write-Host ""
    & schtasks.exe /Query /TN $Name /FO LIST /V | Select-String -Pattern "TaskName:|Status:|Next Run Time:|Task To Run:|Run As User:|Logon Mode:" | ForEach-Object {
        Write-Host "  $($_.Line.Trim())"
    }
}

$scheduleDefaults = Load-ScheduleDefaults
if (-not $StartTime) {
    $StartTime = if ($scheduleDefaults.StartTime) { $scheduleDefaults.StartTime } else { "03:30" }
}
if ($RepeatMinutes -le 0) {
    $RepeatMinutes = if ($scheduleDefaults.RepeatMinutes -ge 60) { $scheduleDefaults.RepeatMinutes } else { 180 }
}
if ($RepeatMinutes -lt 60) {
    throw "RepeatMinutes must be >= 60"
}

$startAt = Parse-Time $StartTime
Register-SyncTask -Name $TaskName -TargetBatch $batchPath -At $startAt -RepeatMinutes $RepeatMinutes
Show-TaskSummary -Name $TaskName

if (-not $WhatIf) {
    Write-Host ""
    Write-Host "Done. BigQuery sync autorun is installed for current user context."
    Write-Host "The task uses a fixed repeating trigger - Task Scheduler fires it every $RepeatMinutes minutes"
    Write-Host "regardless of whether previous runs succeeded or failed. No finish-driven reschedule needed."
    Write-Host "Main command:"
    Write-Host "  $batchPath"
}
