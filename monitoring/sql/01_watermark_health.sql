-- Daily watermark health (silver_meta.watermarks)
-- Run as Scheduled Query once per day.
-- Replace defaults if your datasets differ.

DECLARE project_id STRING DEFAULT 'wata-clinicdataplatform-gcp';
DECLARE watermark_dataset STRING DEFAULT 'silver_meta';
DECLARE tz STRING DEFAULT 'Asia/Ho_Chi_Minh';

SELECT
  CURRENT_DATE(tz) AS monitor_date_local,
  table_name,
  last_lsn_num,
  last_inferred_seq,
  last_commit_ts,
  last_run_id,
  updated_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, MINUTE) AS mins_since_watermark_update,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_commit_ts, MINUTE) AS mins_since_last_commit
FROM `${project_id}.${watermark_dataset}.watermarks`
ORDER BY mins_since_watermark_update DESC, table_name;
