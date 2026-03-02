# streaming_alert

Realtime alert flow (muc tieu <= 1 phut) cho CDC bang `public.clinic_bookings`.

## Kien truc

- Postgres CDC (Debezium) -> Pub/Sub `raw-cdc-events`
- Dataflow Streaming detector -> Pub/Sub `business-alerts`
- Cloud Run Orchestrator:
  - dedup + cooldown
  - ghi state vao Firestore (`alert_state`)
  - ghi history vao BigQuery (`ops_monitor.alert_history`)
  - gui notification (webhook + email SMTP)

## Cau truc thu muc

- `debezium/`
  - `application.properties` (config thuc te)
  - `application.properties.example` (mau)
- `dataflow/`
  - `alert_detector_pipeline.py`
  - `requirements.txt`
  - `cloudbuild.submit_dataflow.yaml`
- `orchestrator/`
  - `main.py`
  - `Dockerfile`
  - `requirements.txt`
- `scripts/`
  - `deploy_orchestrator.ps1`
  - `submit_dataflow_via_cloudbuild.ps1`
- `bq/create_alert_history.sql`

## 1) Prerequisites

- Project: `wata-clinicdataplatform-gcp`
- Region: `us-central1`
- APIs:
  - `pubsub.googleapis.com`
  - `run.googleapis.com`
  - `dataflow.googleapis.com`
  - `cloudbuild.googleapis.com`
  - `artifactregistry.googleapis.com`
  - `firestore.googleapis.com`
  - `bigquery.googleapis.com`
- Firestore database `(default)` da duoc tao.

## 2) Tao resource nen tang

### 2.1 Pub/Sub

```bash
gcloud pubsub topics create raw-cdc-events
gcloud pubsub topics create business-alerts

gcloud pubsub subscriptions create raw-cdc-events-sub \
  --topic=raw-cdc-events
```

### 2.2 BigQuery history table

```bash
bq --location=us-central1 query --use_legacy_sql=false < streaming_alert/bq/create_alert_history.sql
```

## 3) Deploy Cloud Run Orchestrator

### 3.1 Tao secret SMTP password (Gmail App Password)

```bash
gcloud secrets create smtp-password --replication-policy=automatic
echo YOUR_GMAIL_APP_PASSWORD | gcloud secrets versions add smtp-password --data-file=-
```

### 3.2 Deploy script (email den anhtuan962002@gmail.com)

```powershell
powershell -ExecutionPolicy Bypass -File streaming_alert/scripts/deploy_orchestrator.ps1 `
  -ProjectId wata-clinicdataplatform-gcp `
  -Region us-central1 `
  -FirestoreCollection alert_state `
  -BqTableFqn wata-clinicdataplatform-gcp.ops_monitor.alert_history `
  -CooldownSeconds 900 `
  -EmailEnabled true `
  -EmailProvider smtp `
  -EmailFrom "tuan.tnakhmt2002@gmail.com" `
  -EmailTo "anhtuan962002@gmail.com" `
  -SmtpHost "smtp.gmail.com" `
  -SmtpPort 587 `
  -SmtpUser "tuan.tnakhmt2002@gmail.com" `
  -EmailPasswordSecretName "smtp-password"
```

### 3.3 Tao push subscription vao Cloud Run

Lay URL service:

```bash
gcloud run services describe stream-alert-orchestrator \
  --region us-central1 \
  --project wata-clinicdataplatform-gcp \
  --format="value(status.url)"
```

Tao push sub:

```bash
gcloud pubsub subscriptions delete business-alerts-push-sub --project=wata-clinicdataplatform-gcp

gcloud pubsub subscriptions create business-alerts-push-sub \
  --project=wata-clinicdataplatform-gcp \
  --topic=business-alerts \
  --push-endpoint=https://<cloud-run-url>/pubsub/push \
  --push-auth-service-account=streamalert-pubsub-inv@wata-clinicdataplatform-gcp.iam.gserviceaccount.com \
  --push-auth-token-audience=https://<cloud-run-url>/pubsub/push
```

Luu y: service Cloud Run duoc deploy voi `--no-allow-unauthenticated` va endpoint `/pubsub/push` bat buoc bearer token hop le.

## 4) Start Dataflow (khong can cai apache_beam tren local)

```powershell
powershell -ExecutionPolicy Bypass -File streaming_alert/scripts/submit_dataflow_via_cloudbuild.ps1 `
  -ProjectId wata-clinicdataplatform-gcp `
  -TempBucket <your-dataflow-temp-bucket> `
  -Region us-central1 `
  -RawSubscription projects/wata-clinicdataplatform-gcp/subscriptions/raw-cdc-events-sub `
  -AlertTopic projects/wata-clinicdataplatform-gcp/topics/business-alerts `
  -BookingSpikeThreshold 1 `
  -CancelRatioThreshold 1 `
  -MinSampleForRatio 1
```

## 5) Config va run Debezium local Docker

File: `streaming_alert/debezium/application.properties`

Yeu cau quan trong:
- `debezium.source.slot.name=datastream_slot_streamalert`
- `debezium.source.table.include.list=public.clinic_bookings`
- `debezium.source.snapshot.mode=no_data` (chi lay thay doi moi)
- offset/schema history phai la file path:
  - `/debezium/data/offsets.dat`
  - `/debezium/data/schemahistory.dat`
- publication mode:
  - neu DB da co `dbz_publication` all-tables, dung `debezium.source.publication.autocreate.mode=all_tables`

Chay container:

```cmd
cd /d D:\watasoft_intern\data-demo-final\streaming_alert\debezium

docker rm -f debezium-stream-alert

docker run -d --name debezium-stream-alert --restart unless-stopped ^
  -v "%cd%\application.properties:/debezium/conf/application.properties:ro" ^
  -v "%cd%\data:/debezium/data" ^
  -v "%cd%\gcp-sa.json\gcp-sa.json:/secrets/gcp-sa.json:ro" ^
  -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp-sa.json ^
  -e GOOGLE_CLOUD_PROJECT=wata-clinicdataplatform-gcp ^
  quay.io/debezium/server:2.7
```

Theo doi log:

```cmd
docker logs -f debezium-stream-alert
```

## 6) Verify end-to-end

1. Insert/update/delete tren `public.clinic_bookings`.
2. Check raw CDC:

```bash
gcloud pubsub subscriptions pull raw-cdc-events-sub --auto-ack --limit 5
```

3. Check Cloud Run log:

```bash
gcloud run services logs read stream-alert-orchestrator \
  --region us-central1 \
  --project wata-clinicdataplatform-gcp \
  --limit 100
```

4. Check BQ history:

```sql
SELECT *
FROM `wata-clinicdataplatform-gcp.ops_monitor.alert_history`
ORDER BY created_at DESC
LIMIT 50;
```

## 7) Tam ngung pipeline

- Debezium:

```cmd
docker stop debezium-stream-alert
```

- Dataflow streaming:

```bash
gcloud dataflow jobs list --region=us-central1 --project=wata-clinicdataplatform-gcp
gcloud dataflow jobs drain <JOB_ID> --region=us-central1 --project=wata-clinicdataplatform-gcp
```

## 8) Troubleshooting nhanh

- `topic.prefix value is invalid`: thieu `debezium.source.topic.prefix`.
- `Is a directory`: offset/history path dang tro vao thu muc, khong phai file.
- `default credentials not found`: chua mount/cau hinh `GOOGLE_APPLICATION_CREDENTIALS`.
- `File does not exist /secrets/gcp-sa.json`: mount sai path key.
- `resource=streamalert.public.clinic_bookings not found`: chua route topic hoac topic chua ton tai.
- `publication ... already active`: doi `publication.autocreate.mode=all_tables` hoac quan ly publication tren DB.

## Bao mat

- Khong commit plaintext credentials vao git.
- Dung Secret Manager cho SMTP password va cac secret khac.
