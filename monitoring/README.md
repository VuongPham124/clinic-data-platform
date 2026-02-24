# Monitoring Pack

This folder provides daily SQL monitors and dashboard templates for:
- CDC normalize/merge pipeline
- Dataproc/Composer jobs
- Datastream and GCS ingestion observability
- Silver/platinum data quality

## Files

- `sql/01_watermark_health.sql`
- `sql/02_stage_run_quality.sql`
- `sql/03_silver_freshness.sql`
- `sql/04_silver_duplicate_pk_template.sql`
- `sql/05_bq_job_health.sql`
- `sql/06_gcs_ingestion_from_logs.sql`
- `sql/07_datastream_health_from_logs.sql`
- `dashboard/ops_dashboard_template.md`

## How to run daily

1. Create Scheduled Queries in BigQuery for files `01`, `02`, `03`, `05`.
2. Create one Scheduled Query per critical table using `04_silver_duplicate_pk_template.sql`.
3. For `06` and `07`, first export Cloud Logging to BigQuery dataset (for example `ops_logs`).
4. Wire query outputs into Looker Studio or Cloud Monitoring custom dashboard tiles.
5. Configure alert policies with thresholds from `dashboard/ops_dashboard_template.md`.

## Notes

- Update project/dataset defaults in each SQL file.
- `02_stage_run_quality.sql` expects stage table naming pattern `*_cdc`.
- `03_silver_freshness.sql` expects silver tables to contain `__commit_ts`.
- Log-based queries (`06`, `07`) depend on your Log Router sink table names.
