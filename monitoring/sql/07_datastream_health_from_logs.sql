-- Datastream health monitor from Cloud Logging export in BigQuery.
-- Prereq: export Cloud Logging to BigQuery (activity/system logs).

DECLARE logs_project STRING DEFAULT 'wata-clinicdataplatform-gcp';
DECLARE logs_dataset STRING DEFAULT 'ops_logs';
DECLARE lookback_hours INT64 DEFAULT 24;

EXECUTE IMMEDIATE FORMAT("""
  WITH raw_logs AS (
    SELECT
      timestamp,
      severity,
      resource.labels.stream_id AS stream_id,
      protoPayload.methodName AS method_name,
      protoPayload.resourceName AS resource_name,
      protoPayload.status.message AS status_message,
      logName
    FROM `%s.%s.cloudaudit_googleapis_com_activity`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d HOUR)
      AND (
        resource.type = 'datastream.googleapis.com/Stream'
        OR logName LIKE '%%datastream.googleapis.com%%'
      )
  )
  SELECT
    TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
    COALESCE(stream_id, 'unknown') AS stream_id,
    COUNT(*) AS total_events,
    COUNTIF(severity IN ('ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY')) AS error_events,
    COUNTIF(REGEXP_CONTAINS(LOWER(COALESCE(status_message, '')), r'lag|backlog|failed|error')) AS lag_or_error_hints,
    ARRAY_AGG(DISTINCT method_name IGNORE NULLS LIMIT 10) AS methods_seen
  FROM raw_logs
  GROUP BY hour_bucket, stream_id
  ORDER BY hour_bucket DESC, stream_id
""", logs_project, logs_dataset, lookback_hours);
