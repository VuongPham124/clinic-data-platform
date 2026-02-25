-- Dataform act-as dry-run activity monitor from Cloud Logging export.
-- Uses table that exists in ops_logs: dataform_googleapis_com_actas_dry_run_result
-- Non-script version for Scheduled Query with destination table.
-- This query assumes textPayload exists; jsonPayload may not exist in your sink schema.

WITH base AS (
  SELECT
    TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
    severity,
    LOWER(COALESCE(textPayload, '')) AS text_lc
  FROM `wata-clinicdataplatform-gcp.ops_logs.dataform_googleapis_com_actas_dry_run_result`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
)
SELECT
  hour_bucket,
  COUNT(*) AS total_events,
  COUNTIF(severity IN ('ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY')) AS error_events,
  COUNTIF(REGEXP_CONTAINS(text_lc, r'failed|error|denied|permission')) AS text_error_hints,
  0 AS json_error_hints
FROM base
GROUP BY hour_bucket
ORDER BY hour_bucket DESC;
