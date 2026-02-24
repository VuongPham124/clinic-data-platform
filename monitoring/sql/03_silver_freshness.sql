-- Silver freshness for all tables that contain __commit_ts.
-- Measures end-to-end lag at table level.

DECLARE project_id STRING DEFAULT 'wata-clinicdataplatform-gcp';
DECLARE silver_dataset STRING DEFAULT 'silver';

DECLARE sql_text STRING;

SET sql_text = (
  SELECT STRING_AGG(
    FORMAT("""
      SELECT
        '%s' AS silver_table,
        COUNT(*) AS row_count,
        MAX(CAST(__commit_ts AS TIMESTAMP)) AS max_commit_ts,
        MAX(CAST(__lsn_num AS INT64)) AS max_lsn_num,
        %s AS soft_deleted_rows,
        %s AS soft_deleted_ratio,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(CAST(__commit_ts AS TIMESTAMP)), MINUTE) AS freshness_lag_minutes
      FROM `%s.%s.%s`
    """,
    t.table_name,
    IF(
      has_is_deleted,
      'COUNTIF(CAST(is_deleted AS BOOL))',
      '0'
    ),
    IF(
      has_is_deleted,
      'SAFE_DIVIDE(COUNTIF(CAST(is_deleted AS BOOL)), COUNT(*))',
      '0.0'
    ),
    project_id,
    silver_dataset,
    t.table_name),
    " UNION ALL "
  )
  FROM (
    SELECT
      table_name,
      LOGICAL_OR(column_name = '__commit_ts') AS has_commit_ts,
      LOGICAL_OR(column_name = 'is_deleted') AS has_is_deleted
    FROM `${project_id}.${silver_dataset}.INFORMATION_SCHEMA.COLUMNS`
    GROUP BY table_name
  ) t
  WHERE has_commit_ts
);

IF sql_text IS NULL THEN
  SELECT
    'NO_SILVER_TABLES_WITH___commit_ts' AS silver_table,
    0 AS row_count,
    NULL AS max_commit_ts,
    NULL AS max_lsn_num,
    0 AS soft_deleted_rows,
    0.0 AS soft_deleted_ratio,
    NULL AS freshness_lag_minutes;
ELSE
  EXECUTE IMMEDIATE sql_text;
END IF;
