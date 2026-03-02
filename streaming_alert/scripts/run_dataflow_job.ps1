param(
    [Parameter(Mandatory = $true)][string]$ProjectId,
    [Parameter(Mandatory = $true)][string]$Region,
    [Parameter(Mandatory = $true)][string]$TempBucket,
    [string]$JobName = "stream-alert-detector",
    [string]$RawSubscription = "",
    [string]$AlertTopic = "",
    [int]$BookingSpikeThreshold = 120,
    [double]$CancelRatioThreshold = 0.40,
    [int]$MinSampleForRatio = 20
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($RawSubscription)) {
    $RawSubscription = "projects/$ProjectId/subscriptions/raw-cdc-events-sub"
}
if ([string]::IsNullOrWhiteSpace($AlertTopic)) {
    $AlertTopic = "projects/$ProjectId/topics/business-alerts"
}

gcloud services enable dataflow.googleapis.com pubsub.googleapis.com --project $ProjectId | Out-Null

$WorkerSa = "stream-alert-dataflow@$ProjectId.iam.gserviceaccount.com"
$saExists = gcloud iam service-accounts list --project $ProjectId --filter "email=$WorkerSa" --format "value(email)"
if (-not $saExists) {
    gcloud iam service-accounts create "stream-alert-dataflow" --project $ProjectId --display-name "Stream Alert Dataflow Worker" | Out-Null
}

gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$WorkerSa" --role "roles/dataflow.worker" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$WorkerSa" --role "roles/pubsub.subscriber" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$WorkerSa" --role "roles/pubsub.publisher" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId --member "serviceAccount:$WorkerSa" --role "roles/storage.objectAdmin" | Out-Null

$jobSuffix = Get-Date -Format "yyyyMMddHHmmss"
$fullJobName = "$JobName-$jobSuffix"

$env:RAW_CDC_SUBSCRIPTION = $RawSubscription
$env:ALERT_TOPIC = $AlertTopic
$env:BOOKING_SPIKE_THRESHOLD = "$BookingSpikeThreshold"
$env:CANCEL_RATIO_THRESHOLD = "$CancelRatioThreshold"
$env:MIN_SAMPLE_FOR_RATIO = "$MinSampleForRatio"

python streaming_alert/dataflow/alert_detector_pipeline.py `
  --runner DataflowRunner `
  --project $ProjectId `
  --region $Region `
  --temp_location "gs://$TempBucket/dataflow/temp" `
  --staging_location "gs://$TempBucket/dataflow/staging" `
  --job_name $fullJobName `
  --service_account_email $WorkerSa `
  --experiments=use_runner_v2 `
  --streaming `
  --requirements_file streaming_alert/dataflow/requirements.txt `
  --save_main_session

Write-Host "Started Dataflow job: $fullJobName"
Write-Host "Set env vars before running Python locally for direct-run testing:"
Write-Host "RAW_CDC_SUBSCRIPTION=$RawSubscription"
Write-Host "ALERT_TOPIC=$AlertTopic"
