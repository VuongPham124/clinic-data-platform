-- BigQuery job health for load/merge/dbt patterns.
-- Uses region INFORMATION_SCHEMA.JOBS_BY_PROJECT.

DECLARE v_project_id STRING DEFAULT 'wata-clinicdataplatform-gcp';
DECLARE lookback_hours INT64 DEFAULT 24;

WITH jobs AS (
  SELECT
    creation_time,
    end_time,
    user_email,
    job_id,
    job_type,
    state,
    error_result,
    total_bytes_processed,
    total_slot_ms,
    statement_type,
    query
  FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE project_id = v_project_id
    AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_hours HOUR)
    AND (
      job_type = 'LOAD'
      OR
      REGEXP_CONTAINS(LOWER(query), r'\bmerge\b')
      OR REGEXP_CONTAINS(LOWER(query), r'\bcreate table\b')
      OR REGEXP_CONTAINS(LOWER(query), r'\binsert into\b')
      OR REGEXP_CONTAINS(LOWER(query), r'dbt')
    )
)
SELECT
  TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
  COUNT(*) AS jobs_total,
  COUNTIF(error_result IS NOT NULL) AS jobs_failed,
  SAFE_DIVIDE(COUNTIF(error_result IS NOT NULL), COUNT(*)) AS failed_ratio,
  SUM(total_bytes_processed) AS bytes_processed,
  SUM(total_slot_ms) AS slot_ms,
  APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, creation_time, SECOND), 100)[OFFSET(95)] AS p95_duration_sec
FROM jobs
GROUP BY hour_bucket
ORDER BY hour_bucket DESC;
