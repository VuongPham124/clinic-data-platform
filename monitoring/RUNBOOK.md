# Monitoring Runbook (Current Setup)

## 1) Preconditions

- Project has APIs enabled: Monitoring, Logging, BigQuery, Dataproc, Composer, Datastream, Run, Scheduler.
- You have IAM to create:
  - BigQuery scheduled queries
  - Log Router sinks
  - Monitoring dashboards and alert policies

## 2) Create BigQuery dataset for monitor outputs

Example:

```bash
bq --location=us-central1 mk --dataset wata-clinicdataplatform-gcp:ops_monitor
```

## 3) Create Cloud Logging -> BigQuery sink (for GCS/Datastream SQL)

Create sink target dataset (example `ops_logs`) and include:
- `cloudaudit.googleapis.com/activity`
- `cloudaudit.googleapis.com/data_access`

These are required by:
- `monitoring/sql/06_gcs_ingestion_from_logs.sql`
- `monitoring/sql/07_datastream_health_from_logs.sql`

## 4) Create Scheduled Queries (daily)

Run/clone these SQL files into Scheduled Queries:
- `monitoring/sql/01_watermark_health.sql`
- `monitoring/sql/02_stage_run_quality.sql`
- `monitoring/sql/03_silver_freshness.sql`
- `monitoring/sql/05_bq_job_health.sql`
- `monitoring/sql/06_gcs_ingestion_from_logs.sql`
- `monitoring/sql/07_datastream_health_from_logs.sql`

For duplicate PK:
- use `monitoring/sql/04_silver_duplicate_pk_template.sql`
- create one scheduled query per critical table

Recommended destination table pattern:
- `ops_monitor.daily_watermark_health`
- `ops_monitor.daily_stage_run_quality`
- `ops_monitor.daily_silver_freshness`
- `ops_monitor.hourly_bq_job_health`
- `ops_monitor.hourly_gcs_ingestion`
- `ops_monitor.hourly_datastream_health`

## 5) Import Cloud Monitoring dashboard JSON

Use:

```bash
gcloud monitoring dashboards create \
  --project=wata-clinicdataplatform-gcp \
  --config-from-file=monitoring/dashboard/cloud_monitoring_dashboard.importable.json
```

Then edit filters in dashboard widgets:
- Replace `PROJECT_ID`
- Narrow resources (composer env, dataproc cluster, cloud run service, buckets)

## 6) Configure alert policies

Base thresholds:
- DAG/Dataproc/BQ merge failure: immediate alert
- Silver freshness lag > 120 minutes
- Watermark stale > 180 minutes
- Quarantine ratio > 2%
- Cloud Run 5xx > 1% (15m)
- Datastream stream not RUNNING > 5m

## 7) Verification checklist

- SQL scheduled queries run successfully and write to `ops_monitor.*`.
- Dashboard widgets show data (not "No data") after filter updates.
- Trigger one known run and confirm:
  - watermark updated
  - stage quality row exists
  - silver freshness decreases after successful run
