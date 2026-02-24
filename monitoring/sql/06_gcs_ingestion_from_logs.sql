-- GCS ingestion monitor from Cloud Logging export in BigQuery.
-- Prereq: create Log Router sink to BigQuery dataset.
-- Non-script version for Scheduled Query with destination table.
-- Replace project/dataset/table name if your log sink differs.

SELECT
  TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
  REGEXP_EXTRACT(protopayload_auditlog.resourceName, r'projects/_/buckets/([^/]+)/objects/') AS bucket_name,
  REGEXP_EXTRACT(protopayload_auditlog.resourceName, r'objects/cdc/([^/]+)/') AS table_id,
  COUNTIF(protopayload_auditlog.methodName = 'storage.objects.create') AS objects_created,
  COUNTIF(protopayload_auditlog.methodName = 'storage.objects.delete') AS objects_deleted,
  COUNTIF(severity IN ('ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY')) AS error_events
FROM `wata-clinicdataplatform-gcp.ops_logs.cloudaudit_googleapis_com_data_access`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND protopayload_auditlog.serviceName = 'storage.googleapis.com'
  AND protopayload_auditlog.methodName IN ('storage.objects.create', 'storage.objects.delete')
GROUP BY hour_bucket, bucket_name, table_id
ORDER BY hour_bucket DESC, bucket_name, table_id;
