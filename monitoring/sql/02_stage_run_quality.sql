-- Stage quality by run_date from all bq_stage.*_cdc tables.
-- This query dynamically scans all CDC stage tables and returns
-- null-rate / inferred-rate / freshness indicators for a target date.

DECLARE project_id STRING DEFAULT 'wata-clinicdataplatform-gcp';
DECLARE stage_dataset STRING DEFAULT 'bq_stage';
DECLARE run_date STRING DEFAULT FORMAT_DATE('%Y-%m-%d', CURRENT_DATE('Asia/Ho_Chi_Minh'));

DECLARE sql_text STRING;

SET sql_text = (
  SELECT STRING_AGG(
    FORMAT("""
      SELECT
        '%s' AS stage_table,
        @run_date AS source_date_local,
        COUNT(*) AS rows_total,
        COUNTIF(SAFE_CAST(__lsn_num AS INT64) IS NULL) AS rows_lsn_num_null,
        SAFE_DIVIDE(COUNTIF(SAFE_CAST(__lsn_num AS INT64) IS NULL), COUNT(*)) AS lsn_num_null_ratio,
        COUNTIF(COALESCE(CAST(__lsn_num_inferred AS BOOL), FALSE)) AS rows_lsn_inferred,
        SAFE_DIVIDE(COUNTIF(COALESCE(CAST(__lsn_num_inferred AS BOOL), FALSE)), COUNT(*)) AS lsn_inferred_ratio,
        COUNTIF(CAST(__commit_ts AS TIMESTAMP) IS NULL) AS rows_commit_ts_null,
        MAX(CAST(__commit_ts AS TIMESTAMP)) AS max_commit_ts,
        MAX(CAST(__ingest_ts AS TIMESTAMP)) AS max_ingest_ts
      FROM `%s.%s.%s`
      WHERE CAST(__source_date_local AS STRING) = @run_date
    """, table_name, project_id, stage_dataset, table_name),
    " UNION ALL "
  )
  FROM `${project_id}.${stage_dataset}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name LIKE '%_cdc'
);

IF sql_text IS NULL THEN
  SELECT
    'NO_STAGE_TABLES_FOUND' AS stage_table,
    run_date AS source_date_local,
    0 AS rows_total,
    0 AS rows_lsn_num_null,
    0.0 AS lsn_num_null_ratio,
    0 AS rows_lsn_inferred,
    0.0 AS lsn_inferred_ratio,
    0 AS rows_commit_ts_null,
    NULL AS max_commit_ts,
    NULL AS max_ingest_ts;
ELSE
  EXECUTE IMMEDIATE sql_text USING run_date AS run_date;
END IF;
