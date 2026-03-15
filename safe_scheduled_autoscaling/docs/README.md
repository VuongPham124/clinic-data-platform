# Safe Scheduled Autoscaling PoC (Cloud Scheduler -> Cloud Run -> Composer API)

## 1) Objective
This PoC implements the following flow:
- A Cloud Run HTTP service exposing `POST /downscale` and `POST /restore` endpoints.
- Cloud Scheduler calls both endpoints on a cron schedule using OIDC (no API key).
- Cloud Run runs under a dedicated service account and calls the Composer API `environments.patch`.
- `updateMask` is restricted to worker knobs only: `minCount,maxCount,cpu,memoryGb,storageGb`.

## 2) Files Created
- `poc/safe_scheduled_autoscaling/app/main.py`
- `poc/safe_scheduled_autoscaling/app/requirements.txt`
- `poc/safe_scheduled_autoscaling/app/Dockerfile`
- `poc/safe_scheduled_autoscaling/scripts/deploy_poc.ps1`

## 3) Safety Notes
The service includes basic guards:
- Does not patch if the Composer environment is in `UPDATING` state (returns 409).
- Supports `dryRun: true`.
- On `/downscale`, saves the baseline worker config for `/restore`.
- `/restore` defaults to `DEFAULT_RESTORE`; uses the saved baseline only when `useBaseline=true` is sent.

PoC limitations:
- Baseline is currently saved to `/tmp/worker-baseline.json` (ephemeral per Cloud Run instance).
- For production use, baseline should be stored in Firestore/GCS/Secret Manager.

## 4) Quick Start
Requirements:
- `gcloud` installed and authenticated (`gcloud auth login` + `gcloud auth application-default login` if needed).
- Deploying user has IAM/Run/Scheduler/Composer permissions.

Run the script:
```powershell
./poc/safe_scheduled_autoscaling/scripts/deploy_poc.ps1 `
  -ProjectId "<PROJECT_ID>" `
  -Region "us-central1" `
  -ComposerLocation "us-central1" `
  -ComposerEnv "<COMPOSER_ENV_NAME>"
```

Recommended for teams: deploy via service account (avoid Gmail user):
```powershell
./poc/safe_scheduled_autoscaling/scripts/deploy_poc.ps1 `
  -ProjectId "<PROJECT_ID>" `
  -Region "us-central1" `
  -ComposerLocation "us-central1" `
  -ComposerEnv "<COMPOSER_ENV_NAME>" `
  -DeployerServiceAccount "vlh-..."
```

IAM requirements to impersonate the deployer SA:
- Deploying user needs `roles/iam.serviceAccountTokenCreator` on `deployer-sa`.
- `deployer-sa` needs roles to execute all deploy steps (Run, Scheduler, Artifact Registry, IAM policy update, Composer).

The script will:
- Enable APIs: Run, Scheduler, Composer, Artifact Registry.
- Create 2 service accounts:
  - Runtime SA for Cloud Run (calls Composer API).
  - Caller SA for Cloud Scheduler OIDC.
- Grant `roles/composer.admin` to the Runtime SA (PoC role to patch the environment).
- Deploy Cloud Run as private (`--no-allow-unauthenticated`).
- Grant `roles/run.invoker` to the Scheduler Caller SA.
- Grant `roles/iam.serviceAccountTokenCreator` to the Cloud Scheduler service agent on the Caller SA.
- Create 2 scheduler jobs:
  - `composer-downscale` -> `POST /downscale`
  - `composer-restore` -> `POST /restore`

## 5) Testing
1. Check health:
```powershell
$URL = gcloud run services describe composer-safe-autoscaling --region us-central1 --format "value(status.url)"
$TOKEN = gcloud auth print-identity-token --audiences=$URL
Invoke-RestMethod -Method Get -Uri "$URL/healthz" -Headers @{ Authorization = "Bearer $TOKEN" }
```

2. Dry-run downscale/restore:
```powershell
Invoke-RestMethod -Method Post -Uri "$URL/downscale" -Headers @{ Authorization = "Bearer $TOKEN"; "Content-Type"="application/json" } -Body '{"dryRun":true}'
Invoke-RestMethod -Method Post -Uri "$URL/restore" -Headers @{ Authorization = "Bearer $TOKEN"; "Content-Type"="application/json" } -Body '{"dryRun":true}'
```

3. Manually trigger scheduler jobs:
```powershell
gcloud scheduler jobs run composer-downscale --location us-central1
gcloud scheduler jobs run composer-restore --location us-central1
```

4. Verify worker config after triggering:
```powershell
gcloud composer environments describe <COMPOSER_ENV_NAME> `
  --location us-central1 `
  --format="json(config.workloadsConfig.worker,state,updateTime)"
```

Note: `environments.patch` is a long-running operation — wait 1-3 minutes and re-check if changes are not immediately visible.

## 6) Tunable Parameters
On Cloud Run, you can modify the following env vars:
- `DOWNSCALE_MIN_COUNT`, `DOWNSCALE_MAX_COUNT`, `DOWNSCALE_CPU`, `DOWNSCALE_MEMORY_GB`, `DOWNSCALE_STORAGE_GB`
- `RESTORE_MIN_COUNT`, `RESTORE_MAX_COUNT`, `RESTORE_CPU`, `RESTORE_MEMORY_GB`, `RESTORE_STORAGE_GB`

Endpoints also support direct override via JSON body:
```json
{
  "minCount": 1,
  "maxCount": 1,
  "cpu": 0.5,
  "memoryGb": 2,
  "storageGb": 1,
  "dryRun": false
}
```

To have `/restore` use the saved baseline:
```json
{
  "useBaseline": true
}
```

## 7) Hardening for Production
- Store baseline in persistent storage (Firestore/GCS) with versioning + TTL.
- Add idempotency key and distributed lock to prevent overlap during scaling.
- Poll the operation until `done=true` and write an audit log.
- Scope the Composer role following principle of least privilege (replace `roles/composer.admin`).
- Add Cloud Monitoring alerts for patch failures, rising 4xx/5xx rates, or environments stuck in `UPDATING`.
