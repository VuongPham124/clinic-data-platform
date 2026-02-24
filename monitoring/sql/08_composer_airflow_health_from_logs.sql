-- Composer/Airflow health from Cloud Logging export tables.
-- Uses only tables that exist in ops_logs:
-- airflow_scheduler, airflow_triggerer, airflow_webserver, airflow_worker, dag_processor_manager
-- Non-script version for Scheduled Query with destination table.
-- This query assumes textPayload exists; jsonPayload may not exist in your sink schema.

WITH logs_union AS (
  SELECT
    timestamp,
    severity,
    textPayload AS text_payload,
    'airflow_scheduler' AS component
  FROM `wata-clinicdataplatform-gcp.ops_logs.airflow_scheduler`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)

  UNION ALL

  SELECT
    timestamp,
    severity,
    textPayload AS text_payload,
    'airflow_triggerer' AS component
  FROM `wata-clinicdataplatform-gcp.ops_logs.airflow_triggerer`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)

  UNION ALL

  SELECT
    timestamp,
    severity,
    textPayload AS text_payload,
    'airflow_webserver' AS component
  FROM `wata-clinicdataplatform-gcp.ops_logs.airflow_webserver`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)

  UNION ALL

  SELECT
    timestamp,
    severity,
    textPayload AS text_payload,
    'airflow_worker' AS component
  FROM `wata-clinicdataplatform-gcp.ops_logs.airflow_worker`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)

  UNION ALL

  SELECT
    timestamp,
    severity,
    textPayload AS text_payload,
    'dag_processor_manager' AS component
  FROM `wata-clinicdataplatform-gcp.ops_logs.dag_processor_manager`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
),
normalized AS (
  SELECT
    TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
    component,
    severity,
    LOWER(COALESCE(text_payload, '')) AS text_lc
  FROM logs_union
)
SELECT
  hour_bucket,
  component,
  COUNT(*) AS total_events,
  COUNTIF(severity IN ('ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY')) AS error_events,
  COUNTIF(REGEXP_CONTAINS(text_lc, r'failed|error|exception|traceback|timeout')) AS error_hints_text,
  0 AS error_hints_json
FROM normalized
GROUP BY hour_bucket, component
ORDER BY hour_bucket DESC, component;
