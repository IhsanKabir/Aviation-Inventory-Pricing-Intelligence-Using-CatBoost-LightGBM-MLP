param(
    [string]$TaskName = "AirlineIntel_RouteMonitorReport",
    [string]$StartTime = "06:15",
    [int]$RepeatMinutes = 720,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batchPath = Join-Path $repoRoot "scheduler\run_route_monitor_report_once.bat"
if (-not (Test-Path $batchPath)) {
    throw "Route monitor wrapper not found: $batchPath"
}
if ($RepeatMinutes -lt 60) {
    throw "RepeatMinutes must be >= 60"
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

function Register-RouteMonitorTask {
    param(
        [string]$Name,
        [string]$TargetBatch,
        [datetime]$At,
        [int]$EveryMinutes
    )

    $now = Get-Date
    $anchor = Get-Date -Hour $At.Hour -Minute $At.Minute -Second 0
    if ($anchor -lt $now) {
        $anchor = $anchor.AddDays(1)
    }

    $arg = "/c `"$TargetBatch`""
    $action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg
    $trigger = New-ScheduledTaskTrigger `
        -Once `
        -At $anchor `
        -RepetitionInterval (New-TimeSpan -Minutes $EveryMinutes) `
        -RepetitionDuration (New-TimeSpan -Days 3650)

    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Seconds 0)

    if ($WhatIf) {
        Write-Host "[WhatIf] Register-ScheduledTask -TaskName $Name (every $EveryMinutes minutes, anchor $($anchor.ToString('yyyy-MM-dd HH:mm')))"
        return
    }

    $task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings
    Register-ScheduledTask -TaskName $Name -InputObject $task -Force | Out-Null
    Write-Host "Route monitor report task ensured: $Name"
}

function Show-TaskSummary {
    param([string]$Name)
    if ($WhatIf) {
        return
    }
    Write-Host ""
    & schtasks.exe /Query /TN $Name /FO LIST /V | Select-String -Pattern "TaskName:|Status:|Next Run Time:|Repeat: Every:|Task To Run:|Run As User:|Logon Mode:|Stop Task If Runs" | ForEach-Object {
        Write-Host "  $($_.Line.Trim())"
    }
}

$startAt = Parse-Time $StartTime
Register-RouteMonitorTask -Name $TaskName -TargetBatch $batchPath -At $startAt -EveryMinutes $RepeatMinutes
Show-TaskSummary -Name $TaskName

if (-not $WhatIf) {
    Write-Host ""
    Write-Host "Done. Route monitor autorun is installed for current user context."
    Write-Host "Main command:"
    Write-Host "  $batchPath"
}
