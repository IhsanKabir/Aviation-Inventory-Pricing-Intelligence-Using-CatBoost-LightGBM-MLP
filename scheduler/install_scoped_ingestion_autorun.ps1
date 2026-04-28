param(
    [string]$TaskPrefix = "AirlineIntel_Ingestion",
    [string]$ScheduleFile = "",
    [string]$PythonExe = "",
    [switch]$SkipBigQuerySync,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $ScheduleFile) {
    $ScheduleFile = Join-Path $repoRoot "config\schedule.json"
}
if (-not $PythonExe) {
    $PythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
}
$scopeRunner = Join-Path $repoRoot "scheduler\run_scheduled_scope_once.py"

if (-not (Test-Path $ScheduleFile)) {
    throw "Schedule file not found: $ScheduleFile"
}
if (-not (Test-Path $PythonExe)) {
    throw "Python exe not found: $PythonExe"
}
if (-not (Test-Path $scopeRunner)) {
    throw "Scope runner not found: $scopeRunner"
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

function Anchor-Time {
    param([datetime]$At)
    $now = Get-Date
    $anchor = Get-Date -Hour $At.Hour -Minute $At.Minute -Second 0
    if ($anchor -lt $now) {
        $anchor = $anchor.AddDays(1)
    }
    return $anchor
}

function Clean-TaskPart {
    param([string]$Value)
    return (($Value -replace '[^A-Za-z0-9_-]+', '_').Trim('_'))
}

function Add-Entry {
    param(
        [System.Collections.ArrayList]$Entries,
        [string]$ScopeType,
        [string]$ScopeId,
        [object]$Config,
        [string]$FallbackStartTime,
        [int]$FallbackRepeatMinutes
    )
    if (-not $Config) {
        return
    }
    $enabled = $false
    if ($null -ne $Config.enabled) {
        $enabled = [bool]$Config.enabled
    }
    if (-not $enabled) {
        return
    }
    $startTime = if ($Config.start_time) { [string]$Config.start_time } else { $FallbackStartTime }
    $repeatMinutes = if ($Config.repeat_minutes) { [int]$Config.repeat_minutes } else { $FallbackRepeatMinutes }
    if ($repeatMinutes -lt 1) {
        throw "repeat_minutes must be >= 1 for $ScopeType/$ScopeId"
    }
    [void]$Entries.Add([pscustomobject]@{
        ScopeType = $ScopeType
        ScopeId = $ScopeId
        StartTime = $startTime
        RepeatMinutes = $repeatMinutes
        CompletionBufferMinutes = if ($Config.completion_buffer_minutes) { [int]$Config.completion_buffer_minutes } else { 1 }
    })
}

$schedule = Get-Content $ScheduleFile -Raw | ConvertFrom-Json
$timing = $schedule.scheduler_timing
if (-not $timing -or $timing.enabled -eq $false) {
    Write-Host "No enabled scheduler_timing block found in $ScheduleFile"
    exit 0
}

$globalStart = if ($timing.global.start_time) { [string]$timing.global.start_time } elseif ($schedule.task_windows.ingestion.start_time) { [string]$schedule.task_windows.ingestion.start_time } else { "00:05" }
$globalRepeat = if ($timing.global.repeat_minutes) { [int]$timing.global.repeat_minutes } elseif ($schedule.task_windows.ingestion.repeat_minutes) { [int]$schedule.task_windows.ingestion.repeat_minutes } else { 360 }

$entries = New-Object System.Collections.ArrayList

if ($timing.sources) {
    foreach ($prop in $timing.sources.PSObject.Properties) {
        $cfg = $prop.Value
        $sourceId = if ($cfg.source) { [string]$cfg.source } else { [string]$prop.Name }
        Add-Entry -Entries $entries -ScopeType "source" -ScopeId $sourceId -Config $cfg -FallbackStartTime $globalStart -FallbackRepeatMinutes $globalRepeat
    }
}

if ($timing.airlines) {
    foreach ($prop in $timing.airlines.PSObject.Properties) {
        $cfg = $prop.Value
        $airlineId = if ($cfg.airline) { [string]$cfg.airline } else { [string]$prop.Name }
        Add-Entry -Entries $entries -ScopeType "airline" -ScopeId $airlineId -Config $cfg -FallbackStartTime $globalStart -FallbackRepeatMinutes $globalRepeat
    }
}

if ($timing.routes) {
    foreach ($cfg in $timing.routes) {
        if (-not $cfg.enabled) {
            continue
        }
        $routeId = if ($cfg.id) { [string]$cfg.id } else { "{0}_{1}_{2}" -f $cfg.airline, $cfg.origin, $cfg.destination }
        Add-Entry -Entries $entries -ScopeType "route" -ScopeId $routeId -Config $cfg -FallbackStartTime $globalStart -FallbackRepeatMinutes $globalRepeat
    }
}

foreach ($entry in $entries) {
    $taskName = "{0}_{1}_{2}" -f $TaskPrefix, (Clean-TaskPart $entry.ScopeType), (Clean-TaskPart $entry.ScopeId)
    $at = Anchor-Time (Parse-Time $entry.StartTime)
    $argParts = @(
        "`"$scopeRunner`"",
        "--scope-type", $entry.ScopeType,
        "--scope-id", $entry.ScopeId,
        "--python-exe", "`"$PythonExe`"",
        "--schedule-file", "`"$ScheduleFile`"",
        "--min-completed-gap-minutes", $entry.CompletionBufferMinutes
    )
    if ($SkipBigQuerySync) {
        $argParts += "--skip-bigquery-sync"
    }
    $actionArgs = ($argParts -join " ")

    if ($WhatIf) {
        Write-Host "[WhatIf] Register-ScheduledTask -TaskName $taskName ($($entry.ScopeType):$($entry.ScopeId) at $($entry.StartTime), every $($entry.RepeatMinutes)m)"
        continue
    }

    $action = New-ScheduledTaskAction -Execute $PythonExe -Argument $actionArgs
    $trigger = New-ScheduledTaskTrigger -Once -At $at -RepetitionInterval (New-TimeSpan -Minutes $entry.RepeatMinutes)
    $settings = New-ScheduledTaskSettingsSet `
        -WakeToRun `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Hours 8)

    $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
    Register-ScheduledTask -TaskName $taskName -InputObject $task -Force | Out-Null
    Write-Host "Scoped ingestion task ensured: $taskName"
}

if ($entries.Count -eq 0) {
    Write-Host "No enabled source/airline/route scoped scheduler entries found."
}
