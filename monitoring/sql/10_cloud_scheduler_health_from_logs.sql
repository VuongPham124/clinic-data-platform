-- Cloud Scheduler execution health from Cloud Logging export.
-- Source table: ops_logs.cloudscheduler_googleapis_com_executions
-- Non-script version for Scheduled Query with destination table.
--
-- Recommended destination:
--   wata-clinicdataplatform-gcp.ops_monitor.hourly_cloud_scheduler_health
-- Recommended location:
--   us-central1

SELECT
  TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
  resource.labels.location AS scheduler_location,
  resource.labels.job_id AS job_id,
  COUNT(*) AS total_events,
  COUNTIF(severity IN ('ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY')) AS error_events,
  COUNTIF(httpRequest.status >= 400) AS http_4xx_5xx_events,
  COUNTIF(httpRequest.status >= 500) AS http_5xx_events,
  COUNTIF(COALESCE(jsonpayload_logging_attemptfinished.status, '') = 'SUCCESS') AS success_events,
  COUNTIF(COALESCE(jsonpayload_logging_attemptfinished.status, '') != 'SUCCESS') AS non_success_events,
  SAFE_DIVIDE(
    COUNTIF(COALESCE(jsonpayload_logging_attemptfinished.status, '') = 'SUCCESS'),
    COUNT(*)
  ) AS success_ratio,
  MAX(timestamp) AS latest_event_ts
FROM `wata-clinicdataplatform-gcp.ops_logs.cloudscheduler_googleapis_com_executions`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY hour_bucket, scheduler_location, job_id
ORDER BY hour_bucket DESC, scheduler_location, job_id;
