-- GCS ingestion monitor from Cloud Logging export in BigQuery.
-- Prereq: create Log Router sink to BigQuery dataset.
-- Non-script version for Scheduled Query with destination table.
-- Replace project/dataset/table name if your log sink differs.

WITH base AS (
  SELECT
    TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
    protopayload_auditlog.methodName AS method_name,
    -- Parse bucket/object from either resourceName or requestUrl.
    COALESCE(
      REGEXP_EXTRACT(protopayload_auditlog.resourceName, r'projects/_/buckets/([^/]+)/objects/'),
      REGEXP_EXTRACT(httpRequest.requestUrl, r'/b/([^/]+)/o/')
    ) AS bucket_name,
    COALESCE(
      REGEXP_EXTRACT(protopayload_auditlog.resourceName, r'objects/([^\\s]+)$'),
      REGEXP_EXTRACT(httpRequest.requestUrl, r'/o/([^?\\s]+)')
    ) AS object_path,
    severity
  FROM `wata-clinicdataplatform-gcp.ops_logs.cloudaudit_googleapis_com_data_access`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
    AND protopayload_auditlog.serviceName = 'storage.googleapis.com'
)
SELECT
  hour_bucket,
  COALESCE(bucket_name, 'unknown') AS bucket_name,
  -- Keep prior semantic: table_id from cdc prefix if present.
  REGEXP_EXTRACT(object_path, r'^cdc/([^/]+)/') AS table_id,
  method_name,
  COUNT(*) AS total_events,
  COUNTIF(method_name IN ('storage.objects.create', 'storage.objects.insert')) AS objects_created,
  COUNTIF(method_name = 'storage.objects.delete') AS objects_deleted,
  COUNTIF(severity IN ('ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY')) AS error_events
FROM base
GROUP BY hour_bucket, bucket_name, table_id, method_name
ORDER BY hour_bucket DESC, bucket_name, method_name, table_id;
