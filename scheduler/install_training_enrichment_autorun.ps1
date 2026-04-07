param(
    [string]$TaskName = "AirlineIntel_TrainingEnrichment",
    [string]$StartTime = "",
    [int]$RepeatMinutes = 0,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batchPath = Join-Path $repoRoot "scheduler\run_training_enrichment_once.bat"
$schedulePath = Join-Path $repoRoot "config\schedule.json"
if (-not (Test-Path $batchPath)) {
    throw "Training enrichment wrapper not found: $batchPath"
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
        StartTime = [string]($schedule.task_windows.training_enrichment.start_time)
        RepeatMinutes = 1440
    }
}

function Register-TrainingTask {
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
        -ExecutionTimeLimit (New-TimeSpan -Hours 16)

    $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
    Register-ScheduledTask -TaskName $Name -InputObject $task -Force | Out-Null
    Write-Host "Training enrichment task ensured: $Name"
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
    $StartTime = if ($scheduleDefaults.StartTime) { $scheduleDefaults.StartTime } else { "01:30" }
}
if ($RepeatMinutes -le 0) {
    $RepeatMinutes = if ($scheduleDefaults.RepeatMinutes -ge 60) { $scheduleDefaults.RepeatMinutes } else { 1440 }
}
if ($RepeatMinutes -lt 60) {
    throw "RepeatMinutes must be >= 60"
}
$startAt = Parse-Time $StartTime
Register-TrainingTask -Name $TaskName -TargetBatch $batchPath -At $startAt
Show-TaskSummary -Name $TaskName

if (-not $WhatIf) {
    Write-Host ""
    Write-Host "Done. Training enrichment autorun is installed for current user context."
    Write-Host "This task is finish-driven: the wrapper reschedules the next run after completion + buffer."
    Write-Host "Main command:"
    Write-Host "  $batchPath"
}
