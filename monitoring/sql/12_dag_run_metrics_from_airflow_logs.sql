-- Build dag_run_metrics from Composer/Airflow logs exported to BigQuery.
-- Source tables (ops_logs):
--   airflow_scheduler, airflow_worker
--
-- Recommended Scheduled Query destination:
--   wata-clinicdataplatform-gcp.ops_monitor.dag_run_metrics
-- Recommended write mode:
--   WRITE_TRUNCATE
-- Recommended schedule:
--   every 1 hour

WITH logs_union AS (
  SELECT
    timestamp,
    textPayload AS text_payload,
    'airflow_scheduler' AS source_component
  FROM `wata-clinicdataplatform-gcp.ops_logs.airflow_scheduler`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND textPayload IS NOT NULL

  UNION ALL

  SELECT
    timestamp,
    textPayload AS text_payload,
    'airflow_worker' AS source_component
  FROM `wata-clinicdataplatform-gcp.ops_logs.airflow_worker`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND textPayload IS NOT NULL
),
parsed AS (
  SELECT
    timestamp,
    source_component,
    text_payload,
    COALESCE(
      REGEXP_EXTRACT(text_payload, r'(?i)dag_id[=: ]+([a-zA-Z0-9_.-]+)'),
      REGEXP_EXTRACT(text_payload, r'(?i)\bdag[=: ]+([a-zA-Z0-9_.-]+)'),
      REGEXP_EXTRACT(text_payload, r'(?i)/dags/([a-zA-Z0-9_.-]+)')
    ) AS dag_id,
    COALESCE(
      REGEXP_EXTRACT(text_payload, r'(?i)run_id[=: ]+([^,\s]+)'),
      REGEXP_EXTRACT(text_payload, r'(?i)\brun[ _-]?id[=: ]+([^,\s]+)')
    ) AS run_id,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(text_payload), r'failed|failure') THEN 'failed'
      WHEN REGEXP_CONTAINS(LOWER(text_payload), r'success|successful') THEN 'success'
      WHEN REGEXP_CONTAINS(LOWER(text_payload), r'running|queued|start') THEN 'running'
      ELSE NULL
    END AS status_hint
  FROM logs_union
),
agg AS (
  SELECT
    dag_id,
    run_id,
    MIN(timestamp) AS start_ts,
    MAX(timestamp) AS end_ts,
    ARRAY_AGG(status_hint IGNORE NULLS ORDER BY timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS status,
    ARRAY_AGG(source_component ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)] AS source_component
  FROM parsed
  WHERE dag_id IS NOT NULL
  GROUP BY dag_id, run_id
)
SELECT
  dag_id,
  COALESCE(run_id, CONCAT('unknown_', FORMAT_TIMESTAMP('%Y%m%d%H', start_ts))) AS run_id,
  COALESCE(status, 'unknown') AS status,
  start_ts,
  end_ts,
  TIMESTAMP_DIFF(end_ts, start_ts, SECOND) AS duration_sec,
  source_component,
  CURRENT_TIMESTAMP() AS monitor_ts_utc
FROM agg
WHERE start_ts IS NOT NULL
  AND end_ts IS NOT NULL
ORDER BY dag_id, end_ts DESC;
