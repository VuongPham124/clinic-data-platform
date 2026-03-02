CREATE TABLE IF NOT EXISTS `wata-clinicdataplatform-gcp.ops_monitor.alert_history` (
  alert_id STRING,
  alert_key STRING,
  alert_type STRING,
  severity STRING,
  source_table STRING,
  event_time TIMESTAMP,
  payload_json STRING,
  created_at TIMESTAMP,
  emitted BOOL
);
