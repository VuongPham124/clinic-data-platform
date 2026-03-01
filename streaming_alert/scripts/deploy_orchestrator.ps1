param(
    [Parameter(Mandatory = $true)][string]$ProjectId,
    [Parameter(Mandatory = $true)][string]$Region,
    [string]$ServiceName = "stream-alert-orchestrator",
    [string]$Repository = "streaming-alert",
    [string]$Image = "orchestrator",
    [string]$FirestoreCollection = "alert_state",
    [string]$BqTableFqn = "",
    [int]$CooldownSeconds = 900,
    [string]$NotifyWebhookUrl = "",
    [string]$EmailEnabled = "false",
    [string]$EmailProvider = "smtp",
    [string]$EmailFrom = "",
    [string]$EmailTo = "",
    [string]$SmtpHost = "smtp.gmail.com",
    [int]$SmtpPort = 587,
    [string]$SmtpUser = "",
    [string]$EmailPasswordSecretName = "",
    [string]$SendgridSecretName = ""
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($BqTableFqn)) {
    $BqTableFqn = "$ProjectId.ops_monitor.alert_history"
}

$EmailEnabledBool = $false
if ($EmailEnabled -is [bool]) {
    $EmailEnabledBool = [bool]$EmailEnabled
} else {
    $v = $EmailEnabled.ToString().Trim().ToLower()
    if ($v -in @("1", "true", "$true", "yes", "y")) {
        $EmailEnabledBool = $true
    } elseif ($v -in @("0", "false", "$false", "no", "n", "")) {
        $EmailEnabledBool = $false
    } else {
        throw "Invalid -EmailEnabled value: '$EmailEnabled'. Use true/false or 1/0."
    }
}

$ImageUri = "$Region-docker.pkg.dev/$ProjectId/$Repository/$Image" + ":latest"
$RuntimeSa = "stream-alert-orchestrator@$ProjectId.iam.gserviceaccount.com"

gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com firestore.googleapis.com bigquery.googleapis.com pubsub.googleapis.com --project $ProjectId | Out-Null

$repoExists = gcloud artifacts repositories list --project $ProjectId --location $Region --filter "name~/$Repository$" --format "value(name)"
if (-not $repoExists) {
    gcloud artifacts repositories create $Repository --project $ProjectId --location $Region --repository-format docker --quiet | Out-Null
}

$saExists = gcloud iam service-accounts list --project $ProjectId --filter "email=$RuntimeSa" --format "value(email)"
if (-not $saExists) {
    gcloud iam service-accounts create "stream-alert-orchestrator" --project $ProjectId --display-name "Stream Alert Orchestrator" | Out-Null
}

gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$RuntimeSa" --role "roles/datastore.user" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$RuntimeSa" --role "roles/bigquery.dataEditor" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$RuntimeSa" --role "roles/bigquery.jobUser" | Out-Null
if (-not [string]::IsNullOrWhiteSpace($SendgridSecretName) -or -not [string]::IsNullOrWhiteSpace($EmailPasswordSecretName)) {
    gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$RuntimeSa" --role "roles/secretmanager.secretAccessor" | Out-Null
}

gcloud builds submit streaming_alert/orchestrator --project $ProjectId --tag $ImageUri

$envVars = "PROJECT_ID=$ProjectId,FIRESTORE_COLLECTION=$FirestoreCollection,BQ_TABLE_FQN=$BqTableFqn,COOLDOWN_SECONDS=$CooldownSeconds,NOTIFY_WEBHOOK_URL=$NotifyWebhookUrl,EMAIL_ENABLED=$EmailEnabledBool,EMAIL_PROVIDER=$EmailProvider,EMAIL_FROM=$EmailFrom,EMAIL_TO=$EmailTo,SMTP_HOST=$SmtpHost,SMTP_PORT=$SmtpPort,SMTP_USER=$SmtpUser"

if (-not [string]::IsNullOrWhiteSpace($SendgridSecretName) -or -not [string]::IsNullOrWhiteSpace($EmailPasswordSecretName)) {
    $secretFlags = @()
    if (-not [string]::IsNullOrWhiteSpace($SendgridSecretName)) {
        $secretFlags += "SENDGRID_API_KEY=$SendgridSecretName:latest"
    }
    if (-not [string]::IsNullOrWhiteSpace($EmailPasswordSecretName)) {
        $secretFlags += "SMTP_PASSWORD=$EmailPasswordSecretName:latest"
    }
    gcloud run deploy $ServiceName `
      --project $ProjectId `
      --region $Region `
      --image $ImageUri `
      --service-account $RuntimeSa `
      --set-env-vars "$envVars" `
      --set-secrets ($secretFlags -join ",") `
      --allow-unauthenticated | Out-Null
} else {
    gcloud run deploy $ServiceName `
      --project $ProjectId `
      --region $Region `
      --image $ImageUri `
      --service-account $RuntimeSa `
      --set-env-vars "$envVars" `
      --allow-unauthenticated | Out-Null
}

Write-Host "Deployed Cloud Run service: $ServiceName"
