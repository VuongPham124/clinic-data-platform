-- GCS ingestion monitor from Cloud Logging export in BigQuery.
-- Prereq: create Log Router sink to BigQuery dataset.
-- Default table name below matches common export naming.

DECLARE logs_project STRING DEFAULT 'wata-clinicdataplatform-gcp';
DECLARE logs_dataset STRING DEFAULT 'ops_logs';
DECLARE lookback_hours INT64 DEFAULT 24;

EXECUTE IMMEDIATE FORMAT("""
  SELECT
    TIMESTAMP_TRUNC(timestamp, HOUR) AS hour_bucket,
    resource.labels.bucket_name AS bucket_name,
    REGEXP_EXTRACT(protoPayload.resourceName, r'objects/cdc/([^/]+)/') AS table_id,
    COUNTIF(protoPayload.methodName = 'storage.objects.create') AS objects_created,
    COUNTIF(protoPayload.methodName = 'storage.objects.delete') AS objects_deleted,
    COUNTIF(severity IN ('ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY')) AS error_events
  FROM `%s.%s.cloudaudit_googleapis_com_data_access`
  WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d HOUR)
    AND resource.type = 'gcs_bucket'
    AND protoPayload.serviceName = 'storage.googleapis.com'
    AND protoPayload.methodName IN ('storage.objects.create', 'storage.objects.delete')
  GROUP BY hour_bucket, bucket_name, table_id
  ORDER BY hour_bucket DESC, bucket_name, table_id
""", logs_project, logs_dataset, lookback_hours);
