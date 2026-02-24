-- Duplicate PK monitor template.
-- Recommended: create one scheduled query per critical silver table with explicit PK.
-- Non-script version for Scheduled Query with destination table.
-- Copy this file per table and edit:
--   1) table FQN in FROM
--   2) pk expression in SELECT/GROUP BY

SELECT
  CURRENT_TIMESTAMP() AS monitor_ts_utc,
  CURRENT_DATE('Asia/Ho_Chi_Minh') AS monitor_date_local,
  CAST(id AS STRING) AS pk_value,
  COUNT(*) AS dup_count
FROM `wata-clinicdataplatform-gcp.silver.public_users`
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
ORDER BY dup_count DESC;
