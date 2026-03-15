# streaming_alert

Real-time alert flow (target latency <= 1 minute) for CDC on `public.clinic_bookings`.

## Architecture and Processing Flow

```text
Postgres (self-hosted)
  -> Debezium (CDC slot: datastream_slot_streamalert, table: public.clinic_bookings)
  -> Pub/Sub topic: raw-cdc-events
  -> Dataflow Streaming detector (1-minute window + spike/cancel ratio rules)
  -> Pub/Sub topic: business-alerts
  -> Pub/Sub push subscription (OIDC) -> Cloud Run /pubsub/push
  -> Orchestrator
       - dedup + cooldown by alert_key
       - writes current state to Firestore: alert_state
       - writes immutable history to BigQuery: ops_monitor.alert_history
       - sends notifications: webhook and/or SMTP email
```

### Component Roles

- **Debezium**
  Reads WAL logical decoding from Postgres, streams only new changes (no historical snapshot), and pushes CDC events to `raw-cdc-events`.

- **Dataflow Streaming Detector**
  Consumes `raw-cdc-events`, computes over 1-minute windows, applies detection rules (booking spike, cancellation ratio spike), and emits `alert_event` to `business-alerts`.

- **Pub/Sub business-alerts (push)**
  Pushes `alert_event` to the Cloud Run `/pubsub/push` endpoint using an OIDC token (invoker service account).

- **Cloud Run Orchestrator**
  Processes alert events transactionally:
  1. Reads/updates state atomically in Firestore to prevent race conditions.
  2. Decides whether to emit or suppress (cooldown).
  3. Writes to BigQuery history for audit/traceability.
  4. If `emitted=true`, sends webhook/email notification.

### Data: Firestore vs BigQuery

- **Firestore `alert_state`**: current state per `alert_key` (last_seen_at, last_sent_at, cooldown_seconds, latest payload).
- **BigQuery `ops_monitor.alert_history`**: immutable event log (alert_id, alert_key, severity, payload_json, emitted, created_at) for audit and reporting.

## Directory Structure

- `debezium/`
  - `application.properties` (active config)
  - `application.properties.example` (template)
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
- APIs required:
  - `pubsub.googleapis.com`
  - `run.googleapis.com`
  - `dataflow.googleapis.com`
  - `cloudbuild.googleapis.com`
  - `artifactregistry.googleapis.com`
  - `firestore.googleapis.com`
  - `bigquery.googleapis.com`
- Firestore `(default)` database must already be created.

## 2) Create Foundation Resources

### 2.1 Pub/Sub

```bash
gcloud pubsub topics create raw-cdc-events
gcloud pubsub topics create business-alerts

gcloud pubsub subscriptions create raw-cdc-events-sub \
  --topic=raw-cdc-events
```

### 2.2 BigQuery History Table

```bash
bq --location=us-central1 query --use_legacy_sql=false < streaming_alert/bq/create_alert_history.sql
```

## 3) Deploy Cloud Run Orchestrator

### 3.1 Create SMTP Password Secret (Gmail App Password)

```bash
gcloud secrets create smtp-password --replication-policy=automatic
echo YOUR_GMAIL_APP_PASSWORD | gcloud secrets versions add smtp-password --data-file=-
```

### 3.2 Deploy Script

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

### 3.3 Create Push Subscription to Cloud Run

Get the service URL:

```bash
gcloud run services describe stream-alert-orchestrator \
  --region us-central1 \
  --project wata-clinicdataplatform-gcp \
  --format="value(status.url)"
```

Create the push subscription:

```bash
gcloud pubsub subscriptions delete business-alerts-push-sub --project=wata-clinicdataplatform-gcp

gcloud pubsub subscriptions create business-alerts-push-sub \
  --project=wata-clinicdataplatform-gcp \
  --topic=business-alerts \
  --push-endpoint=https://<cloud-run-url>/pubsub/push \
  --push-auth-service-account=streamalert-pubsub-inv@wata-clinicdataplatform-gcp.iam.gserviceaccount.com \
  --push-auth-token-audience=https://<cloud-run-url>/pubsub/push
```

Note: the Cloud Run service is deployed with `--no-allow-unauthenticated` — the `/pubsub/push` endpoint requires a valid bearer token.

## 4) Start Dataflow (No Local apache_beam Installation Required)

```powershell
powershell -ExecutionPolicy Bypass -File streaming_alert/scripts/submit_dataflow_via_cloudbuild.ps1 `
  -ProjectId wata-clinicdataplatform-gcp `
  -TempBucket <your-dataflow-temp-bucket> `
  -Region us-central1 `
  -WorkerZone us-central1-f `
  -WorkerMachineType e2-standard-2 `
  -RawSubscription projects/wata-clinicdataplatform-gcp/subscriptions/raw-cdc-events-sub `
  -AlertTopic projects/wata-clinicdataplatform-gcp/topics/business-alerts `
  -BookingSpikeThreshold 1 `
  -CancelRatioThreshold 1 `
  -MinSampleForRatio 1
```

If you encounter `ZONE_RESOURCE_POOL_EXHAUSTED` in a zone (e.g. `us-central1-c`), change `-WorkerZone` to another zone in the same region (`us-central1-a`, `us-central1-b`, `us-central1-f`) and resubmit.

## 5) Configure and Run Debezium (Local Docker)

File: `streaming_alert/debezium/application.properties`

Key requirements:
- `debezium.source.slot.name=datastream_slot_streamalert`
- `debezium.source.table.include.list=public.clinic_bookings`
- `debezium.source.snapshot.mode=no_data` (new changes only)
- Offset/schema history must be file paths (not directories):
  - `/debezium/data/offsets.dat`
  - `/debezium/data/schemahistory.dat`
- Publication mode:
  - If the DB already has a `dbz_publication` for all tables, use `debezium.source.publication.autocreate.mode=all_tables`

Run the container:

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

Monitor logs:

```cmd
docker logs -f debezium-stream-alert
```

## 6) End-to-End Verification

1. Insert/update/delete on `public.clinic_bookings`.
2. Check raw CDC events:

```bash
gcloud pubsub subscriptions pull raw-cdc-events-sub --auto-ack --limit 5
```

3. Check Cloud Run logs:

```bash
gcloud run services logs read stream-alert-orchestrator \
  --region us-central1 \
  --project wata-clinicdataplatform-gcp \
  --limit 100
```

4. Check BigQuery history:

```sql
SELECT *
FROM `wata-clinicdataplatform-gcp.ops_monitor.alert_history`
ORDER BY created_at DESC
LIMIT 50;
```

## 7) Pausing the Pipeline

- Stop Debezium:

```cmd
docker stop debezium-stream-alert
```

- Drain the Dataflow streaming job:

```bash
gcloud dataflow jobs list --region=us-central1 --project=wata-clinicdataplatform-gcp
gcloud dataflow jobs drain <JOB_ID> --region=us-central1 --project=wata-clinicdataplatform-gcp
```

## 8) Quick Troubleshooting

| Error | Cause / Fix |
|---|---|
| `topic.prefix value is invalid` | Missing `debezium.source.topic.prefix` |
| `Is a directory` | Offset/history path points to a directory, not a file |
| `default credentials not found` | `GOOGLE_APPLICATION_CREDENTIALS` not mounted or configured |
| `File does not exist /secrets/gcp-sa.json` | Incorrect key mount path |
| `resource=streamalert.public.clinic_bookings not found` | Topic not routed or does not exist yet |
| `publication ... already active` | Change `publication.autocreate.mode=all_tables` or manage the publication directly on the DB |

## Security

- Do not commit plaintext credentials to git.
- Use Secret Manager for SMTP passwords and all other secrets.
