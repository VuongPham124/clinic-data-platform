# Operations Dashboard Template (Daily)

Use this template to build one unified dashboard in Cloud Monitoring (infra) + BigQuery/Looker Studio (data quality).

## A. Data Quality Panels (BigQuery SQL)

1. `Watermark Health`
- Source: `monitoring/sql/01_watermark_health.sql`
- Metric: `mins_since_watermark_update`, `mins_since_last_commit`
- Alert: any table `mins_since_watermark_update > 180`

2. `Stage Run Quality`
- Source: `monitoring/sql/02_stage_run_quality.sql`
- Metric: `rows_total`, `lsn_num_null_ratio`, `lsn_inferred_ratio`, `rows_commit_ts_null`
- Alert: `lsn_num_null_ratio > 0`, or `rows_total = 0` for expected table

3. `Silver Freshness`
- Source: `monitoring/sql/03_silver_freshness.sql`
- Metric: `freshness_lag_minutes`, `soft_deleted_ratio`
- Alert: `freshness_lag_minutes > 120`

4. `Duplicate PK (Critical Tables)`
- Source: `monitoring/sql/04_silver_duplicate_pk_template.sql`
- Metric: duplicate key count
- Alert: any row returned (dup_count > 1)

5. `BQ Load/Merge Health`
- Source: `monitoring/sql/05_bq_job_health.sql`
- Metric: failed ratio, p95 duration, bytes processed, slot-ms
- Alert: failed ratio `> 0.05` or p95 duration spike over baseline

6. `GCS Ingestion from Logs`
- Source: `monitoring/sql/06_gcs_ingestion_from_logs.sql`
- Metric: objects created/deleted per hour, error events
- Alert: no create events in expected window, or error spike

7. `Datastream Health from Logs`
- Source: `monitoring/sql/07_datastream_health_from_logs.sql`
- Metric: error events, lag/error hints by stream
- Alert: error events > 0 in 2 consecutive windows

## B. Infra Panels (Cloud Monitoring)

Create panels in Cloud Monitoring dashboard using these service scopes:

1. `Composer / Airflow`
- DAG run success rate
- DAG duration p95
- task retry count
- queued task count
- scheduler health / heartbeat

2. `Dataproc`
- job success/failure count
- job duration p95
- cluster CPU/memory utilization
- YARN pending containers
- executor failure count / OOM

3. `Datastream`
- stream state (RUNNING/FAILED/PAUSED)
- replication lag
- backlog size / throughput
- error count by stream/object

4. `GCS`
- bucket bytes
- object count growth
- request count (2xx/4xx/5xx)
- write/read latency

5. `Cloud Run (PoC autoscaling service)`
- request count
- 5xx error rate
- p95 request latency
- instance count / cold start behavior

6. `Cloud Scheduler (PoC)`
- job execution count
- failed execution count
- schedule delay / misfire indicators

## C. Alert Policy Defaults

1. `Critical`
- Any DAG fail in core flows: `main_cdc_flow`, `cdc_daily_to_silver`, `silver_to_silver_curated_dq_v2_3`, `dbt_platinum_gold_build_test`
- Dataproc job failed
- BigQuery merge failed
- Duplicate PK > 0 in critical silver/platinum facts

2. `High`
- Freshness lag > 120 minutes
- Quarantine ratio > 2%
- Watermark stale > 180 minutes
- Datastream stream not RUNNING > 5 minutes

3. `Medium`
- Cloud Run 5xx > 1% for 15 minutes
- Scheduler failed runs >= 2 consecutive
- BQ p95 duration > 2x 7-day baseline

## D. Recommended Dashboard Layout

1. Row 1: End-to-end SLA (Freshness, Watermark, Failed jobs)
2. Row 2: CDC Stage Quality (lsn null/inferred/quarantine)
3. Row 3: Datastream + GCS ingestion
4. Row 4: Dataproc + Composer runtime
5. Row 5: BQ merge/load and dbt quality
6. Row 6: Cloud Run + Scheduler PoC controls
