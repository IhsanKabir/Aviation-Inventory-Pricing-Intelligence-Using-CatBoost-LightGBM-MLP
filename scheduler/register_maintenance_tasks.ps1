param(
    [string]$TaskPrefix = "AirlineIntel",
    [string]$DailyTime = "00:10",
    [string]$WeeklyTime = "00:20",
    [ValidateSet("SUN","MON","TUE","WED","THU","FRI","SAT")]
    [string]$WeeklyDay = "SUN",
    [switch]$SkipWakeTimerConfig,
    [switch]$PreferCurrentUser,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

function Resolve-DayOfWeek {
    param([string]$Code)
    switch ($Code.ToUpperInvariant()) {
        "SUN" { return "Sunday" }
        "MON" { return "Monday" }
        "TUE" { return "Tuesday" }
        "WED" { return "Wednesday" }
        "THU" { return "Thursday" }
        "FRI" { return "Friday" }
        "SAT" { return "Saturday" }
        default { throw "Unsupported day code: $Code" }
    }
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

function Register-OrUpdateTask {
    param(
        [string]$TaskName,
        [Microsoft.Management.Infrastructure.CimInstance]$Trigger,
        [string]$BatchPath
    )

    if (-not (Test-Path $BatchPath)) {
        throw "Task wrapper not found: $BatchPath"
    }

    $arg = "/c `"$BatchPath`""
    $action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg
    $settings = New-ScheduledTaskSettingsSet `
        -WakeToRun `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit (New-TimeSpan -Hours 6)
    if ($WhatIf) {
        $mode = if ($PreferCurrentUser) { "CurrentUser" } else { "SYSTEM (fallback to CurrentUser on access denied)" }
        Write-Host "[WhatIf] Register-ScheduledTask -TaskName $TaskName ($mode)"
        return
    }

    if (-not $PreferCurrentUser) {
        try {
            $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
            $task = New-ScheduledTask -Action $action -Trigger $Trigger -Settings $settings -Principal $principal
            Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force | Out-Null
            Write-Host "Registered task (SYSTEM): $TaskName"
            return
        }
        catch {
            Write-Warning "SYSTEM registration denied for $TaskName. Falling back to current-user registration."
        }
    }

    $taskFallback = New-ScheduledTask -Action $action -Trigger $Trigger -Settings $settings
    Register-ScheduledTask -TaskName $TaskName -InputObject $taskFallback -Force | Out-Null
    Write-Host "Registered task (CurrentUser): $TaskName"
}

function Show-TaskSummary {
    param([string]$TaskName)
    Write-Host ""
    Write-Host "Task: $TaskName"
    & schtasks.exe /Query /TN $TaskName /FO LIST /V | Select-String -Pattern "Next Run Time|Logon Mode|Run As User|Task To Run|Status" | ForEach-Object {
        Write-Host "  $($_.Line.Trim())"
    }
    try {
        [xml]$xml = & schtasks.exe /Query /TN $TaskName /XML
        $ns = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
        $ns.AddNamespace("t", "http://schemas.microsoft.com/windows/2004/02/mit/task")
        $wake = $xml.SelectSingleNode("//t:WakeToRun", $ns).InnerText
        $startAvail = $xml.SelectSingleNode("//t:StartWhenAvailable", $ns).InnerText
        $logon = $xml.SelectSingleNode("//t:LogonType", $ns).InnerText
        Write-Host "  XML WakeToRun: $wake"
        Write-Host "  XML StartWhenAvailable: $startAvail"
        Write-Host "  XML LogonType: $logon"
    }
    catch {
        Write-Warning "Could not parse task XML for ${TaskName}: $($_.Exception.Message)"
    }
}

function Enable-WakeTimers {
    if ($SkipWakeTimerConfig) {
        Write-Host "Skipping wake timer config by request."
        return
    }
    if ($WhatIf) {
        Write-Host "[WhatIf] powercfg /SETACVALUEINDEX SCHEME_CURRENT SUB_SLEEP RTCWAKE 1"
        Write-Host "[WhatIf] powercfg /SETDCVALUEINDEX SCHEME_CURRENT SUB_SLEEP RTCWAKE 1"
        Write-Host "[WhatIf] powercfg /SETACTIVE SCHEME_CURRENT"
        return
    }

    try {
        & powercfg.exe /SETACVALUEINDEX SCHEME_CURRENT SUB_SLEEP RTCWAKE 1 | Out-Null
        & powercfg.exe /SETDCVALUEINDEX SCHEME_CURRENT SUB_SLEEP RTCWAKE 1 | Out-Null
        & powercfg.exe /SETACTIVE SCHEME_CURRENT | Out-Null
        Write-Host "Wake timers enabled for current power scheme (AC/DC)."
    }
    catch {
        Write-Warning "Could not set wake timers automatically. Run PowerShell as Administrator and retry. Error: $($_.Exception.Message)"
    }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dailyBat = Join-Path $repoRoot "scheduler\run_daily_ops_task.bat"
$weeklyBat = Join-Path $repoRoot "scheduler\run_weekly_pack_task.bat"

$dailyTaskName = "${TaskPrefix}_DailyOps"
$weeklyTaskName = "${TaskPrefix}_WeeklyPack"

$dailyAt = Parse-Time $DailyTime
$weeklyAt = Parse-Time $WeeklyTime
$dow = Resolve-DayOfWeek $WeeklyDay

$dailyTrigger = New-ScheduledTaskTrigger -Daily -At $dailyAt
$weeklyTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $dow -At $weeklyAt

Register-OrUpdateTask -TaskName $dailyTaskName -Trigger $dailyTrigger -BatchPath $dailyBat
Register-OrUpdateTask -TaskName $weeklyTaskName -Trigger $weeklyTrigger -BatchPath $weeklyBat
Enable-WakeTimers

if (-not $WhatIf) {
    Show-TaskSummary -TaskName $dailyTaskName
    Show-TaskSummary -TaskName $weeklyTaskName
    Write-Host ""
    Write-Host "Note: Tasks can wake from sleep, but cannot run when the device is fully powered off."
}
