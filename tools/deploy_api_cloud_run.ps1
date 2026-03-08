param(
    [string]$ProjectId = "aeropulseintelligence",
    [string]$Region = "asia-south1",
    [string]$ServiceName = "aero-pulse-api",
    [string]$Repository = "aero-pulse",
    [string]$ImageName = "api",
    [string]$ApiCorsOrigins = "https://YOUR_VERCEL_DOMAIN.vercel.app",
    [string]$BigQueryDataset = "aviation_intel",
    [string]$ServiceAccount = "aero-pulse-api@aeropulseintelligence.iam.gserviceaccount.com",
    [string]$DbSecretName = "",
    [switch]$UseDbSecret
)

$ErrorActionPreference = "Stop"

$image = "$Region-docker.pkg.dev/$ProjectId/$Repository/$ImageName`:latest"

Write-Host "Building API image: $image"
gcloud builds submit `
  --project $ProjectId `
  --tag $image `
  --file apps/api/Dockerfile `
  .

Write-Host "Deploying Cloud Run service: $ServiceName"
$deployArgs = @(
  "run", "deploy", $ServiceName,
  "--project", $ProjectId,
  "--region", $Region,
  "--image", $image,
  "--service-account", $ServiceAccount,
  "--allow-unauthenticated",
  "--set-env-vars", "API_CORS_ORIGINS=$ApiCorsOrigins,API_FORECASTING_SOURCE=bigquery,BIGQUERY_PROJECT_ID=$ProjectId,BIGQUERY_DATASET=$BigQueryDataset"
)

if ($UseDbSecret) {
    if (-not $DbSecretName) {
        throw "When -UseDbSecret is set, provide -DbSecretName."
    }
    $deployArgs += @("--set-secrets", "AIRLINE_DB_URL=$DbSecretName:latest")
}

& gcloud @deployArgs

Write-Host "Done."
