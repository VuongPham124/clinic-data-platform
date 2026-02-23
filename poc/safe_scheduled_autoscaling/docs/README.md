# Safe Scheduled Autoscaling PoC (Cloud Scheduler -> Cloud Run -> Composer API)

## 1) Muc tieu
PoC nay trien khai flow:
- Cloud Run service HTTP co `POST /downscale` va `POST /restore`
- Cloud Scheduler goi 2 endpoint theo cron bang OIDC (khong API key)
- Cloud Run chay bang service account rieng, goi Composer API `environments.patch`
- `updateMask` chi gioi han worker knobs: `minCount,maxCount,cpu,memoryGb,storageGb`

## 2) Cac file da tao
- `poc/safe_scheduled_autoscaling/app/main.py`
- `poc/safe_scheduled_autoscaling/app/requirements.txt`
- `poc/safe_scheduled_autoscaling/app/Dockerfile`
- `poc/safe_scheduled_autoscaling/scripts/deploy_poc.ps1`

## 3) Luu y ve "safe"
Service da co cac guard co ban:
- Khong patch neu Composer env dang `UPDATING` (tra 409)
- Ho tro `dryRun: true`
- Khi `/downscale`, luu baseline worker config de `/restore`
- `/restore` mac dinh dung `DEFAULT_RESTORE`; chi dung baseline khi gui `useBaseline=true`

Gioi han PoC:
- Baseline hien luu o `/tmp/worker-baseline.json` (ephemeral theo instance Cloud Run)
- Neu can production-safe, nen luu baseline o Firestore/GCS/Secret Manager

## 4) Chay nhanh PoC
Yeu cau:
- Da cai `gcloud` va da auth (`gcloud auth login` + `gcloud auth application-default login` neu can)
- User deploy co quyen IAM/Run/Scheduler/Composer

Chay script:
```powershell
./poc/safe_scheduled_autoscaling/scripts/deploy_poc.ps1 `
  -ProjectId "<PROJECT_ID>" `
  -Region "us-central1" `
  -ComposerLocation "us-central1" `
  -ComposerEnv "<COMPOSER_ENV_NAME>"
```

Khuyen nghi cho team: deploy qua service account (khong dung Gmail user):
```powershell
./poc/safe_scheduled_autoscaling/scripts/deploy_poc.ps1 `
  -ProjectId "<PROJECT_ID>" `
  -Region "us-central1" `
  -ComposerLocation "us-central1" `
  -ComposerEnv "<COMPOSER_ENV_NAME>" `
  -DeployerServiceAccount "vlh-..."
```

Yeu cau IAM de impersonate deployer SA:
- User deploy can co `roles/iam.serviceAccountTokenCreator` tren `deployer-sa`
- `deployer-sa` can cac role de thuc thi cac buoc deploy (Run, Scheduler, Artifact Registry, IAM policy update, Composer)

Script se:
- Enable APIs: Run, Scheduler, Composer, Artifact Registry
- Tao 2 service account:
  - Runtime SA cho Cloud Run (goi Composer API)
  - Caller SA cho Cloud Scheduler OIDC
- Gan `roles/composer.admin` cho Runtime SA (PoC role de patch env)
- Deploy Cloud Run private (`--no-allow-unauthenticated`)
- Gan `roles/run.invoker` cho Scheduler Caller SA
- Gan `roles/iam.serviceAccountTokenCreator` cho Cloud Scheduler service agent tren Scheduler Caller SA
- Tao 2 scheduler jobs:
  - `composer-downscale` -> `POST /downscale`
  - `composer-restore` -> `POST /restore`

## 5) Test
1. Kiem tra health:
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

3. Trigger scheduler job thu cong:
```powershell
gcloud scheduler jobs run composer-downscale --location us-central1
gcloud scheduler jobs run composer-restore --location us-central1
```

4. Verify worker config sau khi trigger:
```powershell
gcloud composer environments describe <COMPOSER_ENV_NAME> `
  --location us-central1 `
  --format="json(config.workloadsConfig.worker,state,updateTime)"
```

Luu y: `environments.patch` la long-running operation, doi 1-3 phut roi kiem tra lai neu thay doi chua xuat hien ngay.

## 6) Tham so tinh chinh
Tren Cloud Run, ban co the sua env vars:
- `DOWNSCALE_MIN_COUNT`, `DOWNSCALE_MAX_COUNT`, `DOWNSCALE_CPU`, `DOWNSCALE_MEMORY_GB`, `DOWNSCALE_STORAGE_GB`
- `RESTORE_MIN_COUNT`, `RESTORE_MAX_COUNT`, `RESTORE_CPU`, `RESTORE_MEMORY_GB`, `RESTORE_STORAGE_GB`

Endpoint cung cho override truc tiep qua JSON body:
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

Neu muon `/restore` dung baseline da luu:
```json
{
  "useBaseline": true
}
```

## 7) Hardening de len production
- Luu baseline vao kho ben vung (Firestore/GCS), co version + TTL
- Them idempotency key va distributed lock (tranh overlap khi scale)
- Poll operation den `done=true` va ghi audit log
- Tach role Composer theo principle-of-least-privilege (thay vi `roles/composer.admin`)
- Them Cloud Monitoring alert neu patch fail, 4xx/5xx tang, hoac env stuck `UPDATING`
