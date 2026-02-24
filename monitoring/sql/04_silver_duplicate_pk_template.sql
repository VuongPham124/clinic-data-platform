-- Duplicate PK monitor template.
-- Recommended: create one scheduled query per critical silver table with explicit PK.

-- Example:
--   silver_table_fqn: `wata-clinicdataplatform-gcp.silver.public_users`
--   pk_expr: CAST(id AS STRING)

DECLARE silver_table_fqn STRING DEFAULT '`wata-clinicdataplatform-gcp.silver.public_users`';
DECLARE pk_expr STRING DEFAULT 'CAST(id AS STRING)';

EXECUTE IMMEDIATE FORMAT("""
  SELECT
    CURRENT_DATE('Asia/Ho_Chi_Minh') AS monitor_date_local,
    %s AS pk_value,
    COUNT(*) AS dup_count
  FROM %s
  GROUP BY 1, 2
  HAVING COUNT(*) > 1
  ORDER BY dup_count DESC
""", pk_expr, silver_table_fqn);
