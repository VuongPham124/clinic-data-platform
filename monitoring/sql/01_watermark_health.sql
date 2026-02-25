-- Daily watermark health (silver_meta.watermarks)
-- Run as Scheduled Query once per day.
-- Non-script version for Scheduled Query with destination table.
-- Replace project/dataset literals if needed.

SELECT
  CURRENT_DATE('Asia/Ho_Chi_Minh') AS monitor_date_local,
  table_name,
  last_lsn_num,
  last_inferred_seq,
  last_commit_ts,
  last_run_id,
  updated_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, MINUTE) AS mins_since_watermark_update,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_commit_ts, MINUTE) AS mins_since_last_commit
FROM `wata-clinicdataplatform-gcp.silver_meta.watermarks`
ORDER BY mins_since_watermark_update DESC, table_name;
