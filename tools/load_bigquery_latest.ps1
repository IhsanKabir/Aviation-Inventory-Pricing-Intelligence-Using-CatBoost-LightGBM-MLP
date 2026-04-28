param(
    [Parameter(Mandatory = $true)]
    [string]$CredentialsJson,
    [Parameter(Mandatory = $true)]
    [string]$StartDate,
    [Parameter(Mandatory = $true)]
    [string]$EndDate,
    [string]$ProjectId = "aeropulseintelligence",
    [string]$Dataset = "aviation_intel",
    [string]$OutputDir = "output/warehouse/bigquery",
    [ValidateSet("partition-refresh", "append", "replace")]
    [string]$LoadMode = "partition-refresh",
    [switch]$Replace
)

$ErrorActionPreference = "Stop"

# Ensure we run from the project root regardless of Task Scheduler's working directory
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

if (-not (Test-Path -LiteralPath $CredentialsJson)) {
    throw "Credentials file not found: $CredentialsJson"
}

$python = ".\.venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Virtual environment Python not found at $python"
}

$env:GOOGLE_APPLICATION_CREDENTIALS = (Resolve-Path -LiteralPath $CredentialsJson).Path
$env:BIGQUERY_PROJECT_ID = $ProjectId
$env:BIGQUERY_DATASET = $Dataset

if ($Replace) {
    $LoadMode = "replace"
}

$args = @(
    "tools/export_bigquery_stage.py",
    "--output-dir", $OutputDir,
    "--start-date", $StartDate,
    "--end-date", $EndDate,
    "--load-bigquery",
    "--project-id", $ProjectId,
    "--dataset", $Dataset,
    "--load-mode", $LoadMode
)

if ($Replace) {
    $args += "--replace"
}

Write-Host "Loading curated warehouse tables into BigQuery dataset $ProjectId.$Dataset"
Write-Host "Load mode: $LoadMode"
Write-Host "Credentials: $($env:GOOGLE_APPLICATION_CREDENTIALS)"
& $python @args
