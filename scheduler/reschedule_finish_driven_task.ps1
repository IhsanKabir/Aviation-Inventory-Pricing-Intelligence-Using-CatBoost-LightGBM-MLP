param(
    [Parameter(Mandatory = $true)]
    [string]$TaskName,
    [Parameter(Mandatory = $true)]
    [string]$BatchPath,
    [int]$DelayMinutes = 90,
    [string]$AnchorTime = "",
    [int]$RepeatMinutes = 0,
    [int]$ExecutionTimeLimitHours = 8,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

if ($DelayMinutes -lt 1) {
    throw "DelayMinutes must be >= 1"
}
if ($RepeatMinutes -lt 0) {
    throw "RepeatMinutes must be >= 0"
}

function Parse-Time {
    param([string]$Value)
    try {
        return [datetime]::ParseExact($Value, "HH:mm", [System.Globalization.CultureInfo]::InvariantCulture)
    }
    catch {
        throw "Invalid AnchorTime '$Value'. Expected HH:mm."
    }
}

$batchFullPath = (Resolve-Path $BatchPath).Path
$now = Get-Date

if ($AnchorTime) {
    if ($RepeatMinutes -lt 1) {
        throw "RepeatMinutes must be >= 1 when AnchorTime is provided"
    }
    $anchorTimeParsed = Parse-Time $AnchorTime
    $nextRun = Get-Date -Hour $anchorTimeParsed.Hour -Minute $anchorTimeParsed.Minute -Second 0
    while ($nextRun -le $now) {
        $nextRun = $nextRun.AddMinutes($RepeatMinutes)
    }
} else {
    $nextRun = $now.AddMinutes($DelayMinutes)
}

if ($WhatIf) {
    Write-Host "[WhatIf] Reschedule task '$TaskName' for $($nextRun.ToString('yyyy-MM-dd HH:mm:ss'))"
    exit 0
}

$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$batchFullPath`""
$trigger = New-ScheduledTaskTrigger -Once -At $nextRun
$settings = New-ScheduledTaskSettingsSet `
    -WakeToRun `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours $ExecutionTimeLimitHours)

$task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null

Write-Host "Rescheduled task '$TaskName' for $($nextRun.ToString('yyyy-MM-dd HH:mm:ss'))"
