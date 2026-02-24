-- Datastream health monitor from Cloud Logging export in BigQuery.
-- Prereq: export Cloud Logging to BigQuery (activity/system logs).
-- Non-script version for Scheduled Query with destination table.
-- Replace project/dataset/table name if your log sink differs.

WITH raw_logs AS (
  SELECT
    timestamp,
    severity,
    textPayload AS text_payload,
    TO_JSON_STRING(jsonPayload) AS json_payload_str,
    TO_JSON_STRING(resource) AS resource_str,
    -- Try common candidates for method/status from json payload.
    COALESCE(
      JSON_VALUE(TO_JSON_STRING(jsonPayload), '$.methodName'),
      JSON_VALUE(TO_JSON_STRING(jsonPayload), '$.event_subtype'),
      JSON_VALUE(TO_JSON_STRING(jsonPayload), '$.event_type')
    ) AS method_name,
    COALESCE(
      JSON_VALUE(TO_JSON_STRING(jsonPayload), '$.status.message'),
      JSON_VALUE(TO_JSON_STRING(jsonPayload), '$.message'),
      textPayload
    ) AS status_message,
    logName
  FROM `wata-clinicdataplatform-gcp.ops_logs.datastream_googleapis_com_stream_activity`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
)
SELECT
  TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
  COALESCE(
    -- Parse stream id from resource/json/text if available.
    REGEXP_EXTRACT(resource_str, r'"stream_id"\s*:\s*"([^"]+)"'),
    REGEXP_EXTRACT(json_payload_str, r'"stream_id"\s*:\s*"([^"]+)"'),
    REGEXP_EXTRACT(json_payload_str, r'/streams/([^"/]+)'),
    REGEXP_EXTRACT(text_payload, r'/streams/([^/\s]+)'),
    'unknown'
  ) AS stream_id,
  COUNT(*) AS total_events,
  COUNTIF(severity IN ('ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY')) AS error_events,
  COUNTIF(REGEXP_CONTAINS(LOWER(COALESCE(status_message, '')), r'lag|backlog|failed|error')) AS lag_or_error_hints,
  ARRAY_AGG(DISTINCT method_name IGNORE NULLS LIMIT 10) AS methods_seen
FROM raw_logs
GROUP BY hour_bucket, stream_id
ORDER BY hour_bucket DESC, stream_id;
