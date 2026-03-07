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
    [switch]$Replace
)

$ErrorActionPreference = "Stop"

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

$args = @(
    "tools/export_bigquery_stage.py",
    "--output-dir", $OutputDir,
    "--start-date", $StartDate,
    "--end-date", $EndDate,
    "--load-bigquery",
    "--project-id", $ProjectId,
    "--dataset", $Dataset
)

if ($Replace) {
    $args += "--replace"
}

Write-Host "Loading curated warehouse tables into BigQuery dataset $ProjectId.$Dataset"
Write-Host "Credentials: $($env:GOOGLE_APPLICATION_CREDENTIALS)"
& $python @args
