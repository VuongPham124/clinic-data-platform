-- BigQuery job health for load/merge/dbt patterns.
-- Uses region INFORMATION_SCHEMA.JOBS_BY_PROJECT.
-- Non-script version for Scheduled Query with destination table.
-- Keep Scheduled Query location = us-central1.
-- If your jobs run in another location, replace region-us-central1 below.

SELECT
  TIMESTAMP_TRUNC(creation_time, HOUR) AS hour_bucket,
  COUNT(*) AS jobs_total,
  COUNTIF(error_result IS NOT NULL) AS jobs_failed,
  SAFE_DIVIDE(COUNTIF(error_result IS NOT NULL), COUNT(*)) AS failed_ratio,
  SUM(total_bytes_processed) AS bytes_processed,
  SUM(total_slot_ms) AS slot_ms,
  APPROX_QUANTILES(TIMESTAMP_DIFF(end_time, creation_time, SECOND), 100)[OFFSET(95)] AS p95_duration_sec
FROM `region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE project_id = 'wata-clinicdataplatform-gcp'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND (
    job_type = 'LOAD'
    OR REGEXP_CONTAINS(LOWER(query), r'\bmerge\b')
    OR REGEXP_CONTAINS(LOWER(query), r'\bcreate table\b')
    OR REGEXP_CONTAINS(LOWER(query), r'\binsert into\b')
    OR REGEXP_CONTAINS(LOWER(query), r'dbt')
  )
GROUP BY hour_bucket
ORDER BY hour_bucket DESC;
