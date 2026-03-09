param(
    [Parameter(Mandatory = $true)][string]$ProjectId,
    [Parameter(Mandatory = $true)][string]$TempBucket,
    [string]$Region = "us-central1",
    [string]$JobName = "stream-alert-detector",
    [string]$RawSubscription = "",
    [string]$AlertTopic = "",
    [string]$WorkerServiceAccount = "",
    [string]$WorkerZone = "us-central1-f",
    [string]$WorkerMachineType = "e2-standard-2",
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
if ([string]::IsNullOrWhiteSpace($WorkerServiceAccount)) {
    $WorkerServiceAccount = "stream-alert-dataflow@$ProjectId.iam.gserviceaccount.com"
}

gcloud services enable cloudbuild.googleapis.com dataflow.googleapis.com pubsub.googleapis.com --project $ProjectId | Out-Null

gcloud builds submit . `
  --project $ProjectId `
  --config streaming_alert/dataflow/cloudbuild.submit_dataflow.yaml `
  --substitutions "_REGION=$Region,_TEMP_BUCKET=$TempBucket,_JOB_NAME=$JobName,_RAW_CDC_SUBSCRIPTION=$RawSubscription,_ALERT_TOPIC=$AlertTopic,_WORKER_SERVICE_ACCOUNT=$WorkerServiceAccount,_WORKER_ZONE=$WorkerZone,_WORKER_MACHINE_TYPE=$WorkerMachineType,_BOOKING_SPIKE_THRESHOLD=$BookingSpikeThreshold,_CANCEL_RATIO_THRESHOLD=$CancelRatioThreshold,_MIN_SAMPLE_FOR_RATIO=$MinSampleForRatio"

Write-Host "Submitted Dataflow job via Cloud Build."
