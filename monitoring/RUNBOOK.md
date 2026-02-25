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

## 3) Create Cloud Logging -> BigQuery sink (for log-based SQL)

Create sink target dataset (example `ops_logs`) and include:
- `cloudaudit_googleapis_com_data_access`
- `datastream_googleapis_com_stream_activity`
- `airflow_scheduler`
- `airflow_triggerer`
- `airflow_webserver`
- `airflow_worker`
- `dag_processor_manager`
- `cloudscheduler_googleapis_com_executions`

These are required by:
- `monitoring/sql/06_gcs_ingestion_from_logs.sql`
- `monitoring/sql/07_datastream_health_from_logs.sql`
- `monitoring/sql/08_composer_airflow_health_from_logs.sql`
- `monitoring/sql/10_cloud_scheduler_health_from_logs.sql`

## 4) Create Scheduled Queries (daily)

Run/clone these SQL files into Scheduled Queries:
- `monitoring/sql/01_watermark_health.sql`
- `monitoring/sql/05_bq_job_health.sql`
- `monitoring/sql/06_gcs_ingestion_from_logs.sql`
- `monitoring/sql/07_datastream_health_from_logs.sql`
- `monitoring/sql/08_composer_airflow_health_from_logs.sql`
- `monitoring/sql/10_cloud_scheduler_health_from_logs.sql`
- `monitoring/sql/11_daily_critical_dag_slo.sql`

For duplicate PK:
- use `monitoring/sql/04_silver_duplicate_pk_template.sql`
- create one scheduled query per critical table

Notes on script vs non-script:
 - Non-script (safe with destination table + write disposition): `01`, `04`, `05`, `06`, `07`, `08`, `10`
- Dynamic script (uses `DECLARE/EXECUTE IMMEDIATE`): `02`, `03`
  - For `02`, `03` either:
    1) run manually/ad-hoc, or
    2) keep as scheduled query without destination write settings and write output in SQL.

Recommended destination table pattern:
- `ops_monitor.daily_watermark_health`
- `ops_monitor.daily_stage_run_quality`
- `ops_monitor.daily_silver_freshness`
- `ops_monitor.hourly_bq_job_health`
- `ops_monitor.hourly_gcs_ingestion`
- `ops_monitor.hourly_datastream_health`
- `ops_monitor.hourly_composer_airflow_health`
- `ops_monitor.hourly_cloud_scheduler_health`
- `ops_monitor.daily_critical_dag_slo`

Required input table for DAG SLO:
- `ops_monitor.dag_run_metrics` with columns:
  - `dag_id`, `run_id`, `status`, `start_ts`, `end_ts`, `duration_sec` (optional if start/end present)

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

## 5.1) Export BigQuery monitor tables to Cloud Monitoring custom metrics

Use exporter in `monitoring/exporter/`:
- script: `monitoring/exporter/ops_monitor_to_custom_metrics.py`
- metrics prefix: `custom.googleapis.com/ops/*`
- includes DAG metrics:
  - `custom.googleapis.com/ops/dag/critical_p95_minutes`
  - `custom.googleapis.com/ops/dag/recovery_time_minutes`
  - `custom.googleapis.com/ops/dag/restore_buffer_minutes`
  - `custom.googleapis.com/ops/dag/restore_window_minutes`

One-command deploy script (PowerShell, supports SA impersonation):

```powershell
powershell -ExecutionPolicy Bypass -File monitoring/exporter/scripts/deploy_ops_monitor_exporter.ps1 `
  -ProjectId wata-clinicdataplatform-gcp `
  -Region us-central1 `
  -OpsDataset ops_monitor `
  -DeployerServiceAccount deployer-sa@wata-clinicdataplatform-gcp.iam.gserviceaccount.com
```

Recommended deploy path:
1. Build/push container from `monitoring/exporter/Dockerfile`
2. Deploy Cloud Run Job with env:
   - `PROJECT_ID=wata-clinicdataplatform-gcp`
   - `OPS_DATASET=ops_monitor`
   - `LOCATION=us-central1`
3. Schedule Cloud Run Job invocation every 5-15 minutes with Cloud Scheduler.

IAM for runtime service account:
- BigQuery Data Viewer on `ops_monitor` dataset
- Monitoring Metric Writer (`roles/monitoring.metricWriter`)

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
