param(
    [Parameter(Mandatory = $true)]
    [string]$NeonDbUrl,
    [string]$DumpFile = "output\db_migration\playwright_api_calling.dump",
    [switch]$SkipDump,
    [switch]$SkipRestore,
    [switch]$SkipValidate
)

$ErrorActionPreference = "Stop"

function Read-DotEnv {
    param([string]$Path = ".env")
    $map = @{}
    if (-not (Test-Path $Path)) {
        return $map
    }
    foreach ($line in Get-Content $Path) {
        if (-not $line -or $line.Trim().StartsWith("#")) {
            continue
        }
        $parts = $line -split "=", 2
        if ($parts.Length -ne 2) {
            continue
        }
        $map[$parts[0].Trim()] = $parts[1].Trim()
    }
    return $map
}

function Normalize-LibpqUrl {
    param([string]$Url)
    if (-not $Url) { return $Url }
    return $Url -replace "^postgresql\+psycopg2://", "postgresql://"
}

function Normalize-SqlAlchemyUrl {
    param([string]$Url)
    if (-not $Url) { return $Url }
    if ($Url -match "^postgresql\+psycopg2://") {
        return $Url
    }
    if ($Url -match "^postgresql://") {
        return ($Url -replace "^postgresql://", "postgresql+psycopg2://")
    }
    throw "Unsupported PostgreSQL URL: $Url"
}

function Resolve-PgTool {
    param([string]$ToolName, [hashtable]$EnvMap)

    $cmd = Get-Command $ToolName -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Path
    }

    $candidates = @()
    if ($EnvMap.ContainsKey("PG_BIN_DIR") -and $EnvMap["PG_BIN_DIR"]) {
        $candidates += (Join-Path $EnvMap["PG_BIN_DIR"] "$ToolName.exe")
    }
    $candidates += "C:\Program Files\PostgreSQL\18\bin\$ToolName.exe"
    $candidates += "C:\Program Files\PostgreSQL\17\bin\$ToolName.exe"
    $candidates += "C:\Program Files\PostgreSQL\16\bin\$ToolName.exe"

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    throw "$ToolName not found. Add PostgreSQL bin to PATH or set PG_BIN_DIR in .env."
}

$envMap = Read-DotEnv
$localHost = $envMap["DB_HOST"]
$localPort = $envMap["DB_PORT"]
$localDb = $envMap["DB_NAME"]
$localUser = $envMap["DB_USER"]
$localPassword = $envMap["DB_PASSWORD"]

if (-not $localHost -or -not $localPort -or -not $localDb -or -not $localUser) {
    throw "Local DB settings are incomplete in .env."
}

$pgDump = Resolve-PgTool -ToolName "pg_dump" -EnvMap $envMap
$pgRestore = Resolve-PgTool -ToolName "pg_restore" -EnvMap $envMap

$dumpPath = Join-Path (Get-Location) $DumpFile
$dumpDir = Split-Path $dumpPath -Parent
if (-not (Test-Path $dumpDir)) {
    New-Item -ItemType Directory -Path $dumpDir -Force | Out-Null
}

$neonLibpqUrl = Normalize-LibpqUrl $NeonDbUrl
$neonSqlAlchemyUrl = Normalize-SqlAlchemyUrl $NeonDbUrl

if (-not $SkipDump) {
    Write-Host "Dumping local database to $dumpPath"
    $env:PGPASSWORD = $localPassword
    & $pgDump -h $localHost -p $localPort -U $localUser -d $localDb -Fc -f $dumpPath
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}

if (-not $SkipRestore) {
    Write-Host "Restoring dump into Neon"
    & $pgRestore -d $neonLibpqUrl --no-owner --no-privileges $dumpPath
}

if (-not $SkipValidate) {
    Write-Host "Validating Neon connection"
    .\.venv\Scripts\python.exe -c "from sqlalchemy import create_engine, text; e=create_engine(r'$neonSqlAlchemyUrl', future=True); print(e.connect().execute(text('select count(*) from flight_offers')).scalar())"
}

Write-Host ""
Write-Host "Next step: set AIRLINE_DB_URL in .env to the Neon SQLAlchemy URL and remove local DB_* dependency."
