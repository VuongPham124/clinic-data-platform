# Monitoring Pack

This folder provides daily SQL monitors and dashboard templates for:
- CDC normalize/merge pipeline
- Dataproc/Composer jobs
- Datastream and GCS ingestion observability
- Silver/platinum data quality

## Files

- `METRICS_GUIDE.md`
- `sql/01_watermark_health.sql`
- `sql/02_stage_run_quality.sql`
- `sql/03_silver_freshness.sql`
- `sql/04_silver_duplicate_pk_template.sql`
- `sql/05_bq_job_health.sql`
- `sql/06_gcs_ingestion_from_logs.sql`
- `sql/07_datastream_health_from_logs.sql`
- `sql/08_composer_airflow_health_from_logs.sql`
- `sql/10_cloud_scheduler_health_from_logs.sql`
- `sql/11_daily_critical_dag_slo.sql`
- `dashboard/ops_dashboard_template.md`
- `exporter/ops_monitor_to_custom_metrics.py`
- `exporter/requirements.txt`
- `exporter/Dockerfile`

## How to run daily

1. Create Scheduled Queries in BigQuery for files `01`, `02`, `03`, `05`.
2. Create one Scheduled Query per critical table using `04_silver_duplicate_pk_template.sql`.
3. For `06` and `07`, first export Cloud Logging to BigQuery dataset (for example `ops_logs`).
4. For `08` and `10`, ensure airflow and scheduler log tables are present in `ops_logs`.
5. For `11`, maintain source table `ops_monitor.dag_run_metrics` (dag_id, run_id, status, start_ts, end_ts, duration_sec).
6. Wire query outputs into Looker Studio or Cloud Monitoring custom dashboard tiles.
7. Configure alert policies with thresholds from `dashboard/ops_dashboard_template.md`.
8. If Looker Studio is unavailable, run exporter to push `ops_monitor.*` into `custom.googleapis.com/ops/*` and visualize in Cloud Monitoring.

## Notes

- Update project/dataset defaults in each SQL file.
- `02_stage_run_quality.sql` expects stage table naming pattern `*_cdc`.
- `03_silver_freshness.sql` expects silver tables to contain `__commit_ts`.
- Log-based queries (`06`, `07`) depend on your Log Router sink table names.
- Log-based query (`08`) depends on airflow log tables being exported.
- Log-based query (`10`) depends on cloudscheduler execution logs being exported.
- DAG SLO query (`11`) depends on `ops_monitor.dag_run_metrics` being populated.
