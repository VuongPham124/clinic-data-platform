param(
    [Parameter(Mandatory = $true)][string]$ProjectId,
    [Parameter(Mandatory = $true)][string]$Region,
    [Parameter(Mandatory = $true)][string]$ComposerLocation,
    [Parameter(Mandatory = $true)][string]$ComposerEnv,
    [string]$Repository = "autoscaling",
    [string]$Image = "safe-autoscaling",
    [string]$Service = "composer-safe-autoscaling",
    [string]$SchedulerLocation = "us-central1",
    [string]$Timezone = "Asia/Ho_Chi_Minh",
    [string]$DownscaleCron = "0 19 * * *",
    [string]$RestoreCron = "0 1 * * *",
    [switch]$PrintWorkerConfig = $true,
    [string]$DeployerServiceAccount = ""
)

$ErrorActionPreference = "Stop"
# In PowerShell 7, native commands writing to stderr can be treated as errors.
# gcloud often logs progress/info to stderr; disable that behavior for this script.
if ($null -ne (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue)) {
  $PSNativeCommandUseErrorActionPreference = $false
}

$RuntimeSaName = "composer-autoscaler-runner"
$SchedulerCallerSaName = "scheduler-invoker"

if ($null -eq $Repository) { $Repository = "" }
if ($null -eq $Image) { $Image = "" }
$Repository = $Repository.Trim().Trim("/")
$Image = $Image.Trim().Trim("/")
if ([string]::IsNullOrWhiteSpace($Repository)) { $Repository = "autoscaling" }
if ([string]::IsNullOrWhiteSpace($Image)) { $Image = "safe-autoscaling" }

$ActiveAccount = (gcloud config get-value account 2>$null)
if (-not [string]::IsNullOrWhiteSpace($DeployerServiceAccount)) {
  $env:CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT = $DeployerServiceAccount
  Write-Host "Using deployer impersonation: $DeployerServiceAccount"
} elseif ($ActiveAccount -match "@gmail.com$") {
  Write-Warning "Active account is Gmail user ($ActiveAccount). Prefer -DeployerServiceAccount for team-safe deployment."
}

$RuntimeSa = "$RuntimeSaName@$ProjectId.iam.gserviceaccount.com"
$SchedulerCallerSa = "$SchedulerCallerSaName@$ProjectId.iam.gserviceaccount.com"
$ProjectNumber = (gcloud projects describe $ProjectId --format "value(projectNumber)")
if (-not $ProjectNumber) { throw "Cannot resolve project number for $ProjectId" }
$SchedulerServiceAgent = "service-$ProjectNumber@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
$ImageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$Image" + ":latest"

function Show-ComposerWorkerConfig {
  Write-Host "Composer worker config (current):"
  gcloud composer environments describe $ComposerEnv `
    --project $ProjectId `
    --location $ComposerLocation `
    --format "json(config.workloadsConfig.worker,state,updateTime)" | Out-Host
}

Write-Host "[1/8] Enable required APIs"
gcloud services enable run.googleapis.com cloudscheduler.googleapis.com composer.googleapis.com artifactregistry.googleapis.com --project $ProjectId

Write-Host "[2/8] Create service accounts"
$RuntimeSaExists = (gcloud iam service-accounts list --project $ProjectId --filter "email=$RuntimeSa" --format "value(email)")
if (-not $RuntimeSaExists) {
  gcloud iam service-accounts create $RuntimeSaName --project $ProjectId --display-name "Cloud Run Composer Autoscaler"
}
$SchedulerCallerSaExists = (gcloud iam service-accounts list --project $ProjectId --filter "email=$SchedulerCallerSa" --format "value(email)")
if (-not $SchedulerCallerSaExists) {
  gcloud iam service-accounts create $SchedulerCallerSaName --project $ProjectId --display-name "Scheduler OIDC Invoker"
}

Write-Host "[3/8] Grant Composer update role to Cloud Run runtime SA"
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$RuntimeSa" --role "roles/composer.admin"

Write-Host "[4/8] Ensure Artifact Registry repo exists"
$RepoExists = (gcloud artifacts repositories list --project $ProjectId --location $Region --filter "name~/$Repository$" --format "value(name)")
if (-not $RepoExists) {
  gcloud artifacts repositories create $Repository --project $ProjectId --location $Region --repository-format docker --quiet | Out-Null
}

Write-Host "[5/8] Build and deploy Cloud Run service"
Write-Host "Using image URI: $ImageUri"
gcloud builds submit "poc/safe_scheduled_autoscaling/app" --project $ProjectId --tag $ImageUri
if ($LASTEXITCODE -ne 0) {
  $LastBuildId = (gcloud builds list --project $ProjectId --limit 1 --sort-by=~createTime --format "value(id)")
  if ($LastBuildId) {
    Write-Warning "Cloud Build failed. Latest build id: $LastBuildId"
    Write-Host "Streaming latest build logs..."
    gcloud builds log --project $ProjectId --stream $LastBuildId
  }
  throw "Cloud Build failed for image $ImageUri"
}

gcloud run deploy $Service `
  --project $ProjectId `
  --region $Region `
  --image $ImageUri `
  --service-account $RuntimeSa `
  --no-allow-unauthenticated `
  --set-env-vars "PROJECT_ID=$ProjectId,LOCATION=$ComposerLocation,COMPOSER_ENV=$ComposerEnv,DOWNSCALE_MIN_COUNT=1,DOWNSCALE_MAX_COUNT=1,DOWNSCALE_CPU=0.5,DOWNSCALE_MEMORY_GB=2,DOWNSCALE_STORAGE_GB=1" `
  --min-instances 0 `
  --max-instances 1
if ($LASTEXITCODE -ne 0) { throw "Cloud Run deploy failed for service $Service" }

$ServiceUrl = (gcloud run services describe $Service --project $ProjectId --region $Region --format "value(status.url)")
if (-not $ServiceUrl) { throw "Cannot resolve Cloud Run URL" }
Write-Host "Cloud Run URL: $ServiceUrl"

Write-Host "[6/8] Grant invoker role for Scheduler caller SA"
gcloud run services add-iam-policy-binding $Service --project $ProjectId --region $Region --member "serviceAccount:$SchedulerCallerSa" --role "roles/run.invoker"
gcloud iam service-accounts add-iam-policy-binding $SchedulerCallerSa --project $ProjectId --member "serviceAccount:$SchedulerServiceAgent" --role "roles/iam.serviceAccountTokenCreator"

Write-Host "[7/8] Create/replace Scheduler jobs with OIDC auth"
$JobIds = @(gcloud scheduler jobs list --project $ProjectId --location $SchedulerLocation --format "value(ID)")
$DownscaleExists = $JobIds -contains "composer-downscale"
$RestoreExists = $JobIds -contains "composer-restore"

if ($DownscaleExists) {
  gcloud scheduler jobs update http composer-downscale `
    --project $ProjectId `
    --location $SchedulerLocation `
    --schedule "$DownscaleCron" `
    --time-zone "$Timezone" `
    --uri "$ServiceUrl/downscale" `
    --http-method POST `
    --update-headers "Content-Type=application/json" `
    --message-body '{"reason":"scheduled-downscale"}' `
    --oidc-service-account-email $SchedulerCallerSa `
    --oidc-token-audience $ServiceUrl | Out-Null
} else {
  gcloud scheduler jobs create http composer-downscale `
    --project $ProjectId `
    --location $SchedulerLocation `
    --schedule "$DownscaleCron" `
    --time-zone "$Timezone" `
    --uri "$ServiceUrl/downscale" `
    --http-method POST `
    --headers "Content-Type=application/json" `
    --message-body '{"reason":"scheduled-downscale"}' `
    --oidc-service-account-email $SchedulerCallerSa `
    --oidc-token-audience $ServiceUrl | Out-Null
}

if ($RestoreExists) {
  gcloud scheduler jobs update http composer-restore `
    --project $ProjectId `
    --location $SchedulerLocation `
    --schedule "$RestoreCron" `
    --time-zone "$Timezone" `
    --uri "$ServiceUrl/restore" `
    --http-method POST `
    --update-headers "Content-Type=application/json" `
    --message-body '{"reason":"scheduled-restore"}' `
    --oidc-service-account-email $SchedulerCallerSa `
    --oidc-token-audience $ServiceUrl | Out-Null
} else {
  gcloud scheduler jobs create http composer-restore `
    --project $ProjectId `
    --location $SchedulerLocation `
    --schedule "$RestoreCron" `
    --time-zone "$Timezone" `
    --uri "$ServiceUrl/restore" `
    --http-method POST `
    --headers "Content-Type=application/json" `
    --message-body '{"reason":"scheduled-restore"}' `
    --oidc-service-account-email $SchedulerCallerSa `
    --oidc-token-audience $ServiceUrl | Out-Null
}

Write-Host "[8/8] Dry-run checks"
$IdToken = ""
try {
  $IdToken = (gcloud.cmd auth print-identity-token --impersonate-service-account=$SchedulerCallerSa --audiences=$ServiceUrl --verbosity=error 2>$null)
} catch {
  $IdToken = ""
}
if ($IdToken) {
  Invoke-RestMethod -Method Post -Uri "$ServiceUrl/downscale" -Headers @{ Authorization = "Bearer $IdToken"; "Content-Type" = "application/json" } -Body '{"dryRun":true}' | ConvertTo-Json -Depth 8
  Invoke-RestMethod -Method Post -Uri "$ServiceUrl/restore" -Headers @{ Authorization = "Bearer $IdToken"; "Content-Type" = "application/json" } -Body '{"dryRun":true}' | ConvertTo-Json -Depth 8
} else {
  Write-Warning "Cannot mint OIDC token via impersonation for $SchedulerCallerSa. Falling back to run Scheduler jobs directly."
  gcloud scheduler jobs run composer-downscale --project $ProjectId --location $SchedulerLocation | Out-Null
  gcloud scheduler jobs run composer-restore --project $ProjectId --location $SchedulerLocation | Out-Null
  Write-Host "Triggered scheduler jobs. Check Cloud Run logs for request results."
}

Write-Host "Completed. Scheduler jobs are configured with OIDC and no API key."
if ($PrintWorkerConfig) {
  Show-ComposerWorkerConfig
}
