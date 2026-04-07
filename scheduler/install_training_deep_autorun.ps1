param(
    [string]$TaskName = "AirlineIntel_TrainingDeep",
    [string]$StartTime = "",
    [ValidateSet("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")]
    [string]$DayOfWeek = "Sunday",
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batchPath = Join-Path $repoRoot "scheduler\run_training_deep_once.bat"
$schedulePath = Join-Path $repoRoot "config\schedule.json"
if (-not (Test-Path $batchPath)) {
    throw "Deep training wrapper not found: $batchPath"
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
        StartTime = [string]($schedule.task_windows.training_deep.start_time)
        DayOfWeek = [string]($schedule.task_windows.training_deep.day_of_week)
    }
}

function Register-DeepTask {
    param(
        [string]$Name,
        [string]$TargetBatch,
        [datetime]$At,
        [string]$Weekday
    )

    if ($WhatIf) {
        Write-Host "[WhatIf] Register-ScheduledTask -TaskName $Name (weekly $Weekday at $($At.ToString('HH:mm')))"
        return
    }

    $arg = "/c `"$TargetBatch`""
    $action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg
    $trigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $Weekday -At $At
    $settings = New-ScheduledTaskSettingsSet `
        -WakeToRun `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Hours 96)

    $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
    Register-ScheduledTask -TaskName $Name -InputObject $task -Force | Out-Null
    Write-Host "Deep training task ensured: $Name"
}

function Show-TaskSummary {
    param([string]$Name)
    if ($WhatIf) {
        return
    }
    Write-Host ""
    & schtasks.exe /Query /TN $Name /FO LIST /V | Select-String -Pattern "TaskName:|Status:|Next Run Time:|Schedule Type:|Start Time:|Days:|Task To Run:|Run As User:|Logon Mode:" | ForEach-Object {
        Write-Host "  $($_.Line.Trim())"
    }
}

$scheduleDefaults = Load-ScheduleDefaults
if (-not $StartTime) {
    $StartTime = if ($scheduleDefaults.StartTime) { $scheduleDefaults.StartTime } else { "02:00" }
}
if ($DayOfWeek -eq "Sunday" -and $scheduleDefaults.DayOfWeek) {
    $DayOfWeek = $scheduleDefaults.DayOfWeek
}
$startAt = Parse-Time $StartTime
Register-DeepTask -Name $TaskName -TargetBatch $batchPath -At $startAt -Weekday $DayOfWeek
Show-TaskSummary -Name $TaskName

if (-not $WhatIf) {
    Write-Host ""
    Write-Host "Done. Deep training autorun is installed for current user context."
    Write-Host "Main command:"
    Write-Host "  $batchPath"
}
