param(
    [Parameter(Mandatory = $true)][string]$ProjectId,
    [Parameter(Mandatory = $true)][string]$Region,
    [string]$OpsDataset = "ops_monitor",
    [string]$Repository = "ops-monitor",
    [string]$Image = "ops-monitor-exporter",
    [string]$Job = "ops-monitor-exporter-job",
    [string]$SchedulerJob = "ops-monitor-exporter-every-15m",
    [string]$SchedulerLocation = "us-central1",
    [string]$SchedulerCron = "*/15 * * * *",
    [string]$Timezone = "Asia/Ho_Chi_Minh",
    [string]$RuntimeSaName = "ops-monitor-runtime",
    [string]$SchedulerCallerSaName = "ops-monitor-scheduler",
    [string]$DeployerServiceAccount = "",
    [switch]$ExecuteJobAfterDeploy = $true
)

$ErrorActionPreference = "Stop"
if ($null -ne (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue)) {
    $PSNativeCommandUseErrorActionPreference = $false
}

if (-not [string]::IsNullOrWhiteSpace($DeployerServiceAccount)) {
    $env:CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT = $DeployerServiceAccount
    Write-Host "Using deployer impersonation: $DeployerServiceAccount"
}

$RuntimeSa = "$RuntimeSaName@$ProjectId.iam.gserviceaccount.com"
$SchedulerCallerSa = "$SchedulerCallerSaName@$ProjectId.iam.gserviceaccount.com"
$ImageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$Image" + ":latest"
$JobUri = "https://run.googleapis.com/v2/projects/$ProjectId/locations/$Region/jobs/$($Job):run"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ExporterDir = (Resolve-Path (Join-Path $ScriptRoot "..")).Path

function Ensure-ServiceAccount([string]$Email, [string]$Name, [string]$DisplayName) {
    $exists = (gcloud iam service-accounts list --project $ProjectId --filter "email=$Email" --format "value(email)")
    if (-not $exists) {
        gcloud iam service-accounts create $Name --project $ProjectId --display-name $DisplayName | Out-Null
    }
}

function Ensure-ArtifactRepo() {
    $exists = (gcloud artifacts repositories list --project $ProjectId --location $Region --filter "name~/$Repository$" --format "value(name)")
    if (-not $exists) {
        gcloud artifacts repositories create $Repository --project $ProjectId --location $Region --repository-format docker --quiet | Out-Null
    }
}

function Ensure-SchedulerJob() {
    $jobIds = @(gcloud scheduler jobs list --project $ProjectId --location $SchedulerLocation --format "value(ID)")
    $exists = $jobIds -contains $SchedulerJob
    if ($exists) {
        gcloud scheduler jobs update http $SchedulerJob `
            --project $ProjectId `
            --location $SchedulerLocation `
            --schedule "$SchedulerCron" `
            --time-zone "$Timezone" `
            --uri "$JobUri" `
            --http-method POST `
            --oauth-service-account-email $SchedulerCallerSa `
            --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" `
            --update-headers "Content-Type=application/json" `
            --message-body "{}" | Out-Null
    } else {
        gcloud scheduler jobs create http $SchedulerJob `
            --project $ProjectId `
            --location $SchedulerLocation `
            --schedule "$SchedulerCron" `
            --time-zone "$Timezone" `
            --uri "$JobUri" `
            --http-method POST `
            --oauth-service-account-email $SchedulerCallerSa `
            --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" `
            --headers "Content-Type=application/json" `
            --message-body "{}" | Out-Null
    }
}

Write-Host "[1/8] Enable required APIs"
gcloud services enable `
    run.googleapis.com `
    cloudscheduler.googleapis.com `
    cloudbuild.googleapis.com `
    artifactregistry.googleapis.com `
    monitoring.googleapis.com `
    bigquery.googleapis.com `
    --project $ProjectId | Out-Null

Write-Host "[2/8] Ensure service accounts"
Ensure-ServiceAccount -Email $RuntimeSa -Name $RuntimeSaName -DisplayName "Ops Monitor Runtime SA"
Ensure-ServiceAccount -Email $SchedulerCallerSa -Name $SchedulerCallerSaName -DisplayName "Ops Monitor Scheduler Caller SA"

Write-Host "[3/8] Grant IAM roles"
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$RuntimeSa" --role "roles/monitoring.metricWriter" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$RuntimeSa" --role "roles/bigquery.jobUser" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$RuntimeSa" --role "roles/bigquery.dataViewer" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$SchedulerCallerSa" --role "roles/run.developer" | Out-Null

Write-Host "[4/8] Ensure Artifact Registry repository"
Ensure-ArtifactRepo

Write-Host "[5/8] Build exporter image"
gcloud builds submit $ExporterDir --project $ProjectId --tag $ImageUri

Write-Host "[6/8] Deploy Cloud Run Job"
gcloud run jobs deploy $Job `
    --project $ProjectId `
    --region $Region `
    --image $ImageUri `
    --service-account $RuntimeSa `
    --set-env-vars "PROJECT_ID=$ProjectId,OPS_DATASET=$OpsDataset,LOCATION=$Region" `
    --max-retries 1 `
    --tasks 1 `
    --task-timeout 1200s | Out-Null

Write-Host "[7/8] Create or update Cloud Scheduler job"
Ensure-SchedulerJob

Write-Host "[8/8] Verification"
if ($ExecuteJobAfterDeploy) {
    gcloud run jobs execute $Job --project $ProjectId --region $Region | Out-Null
    Write-Host "Triggered Cloud Run Job once for verification."
}
Write-Host "Done."
Write-Host "Image: $ImageUri"
Write-Host "Job URI: $JobUri"
Write-Host "Scheduler: $SchedulerJob ($SchedulerCron, $Timezone)"
