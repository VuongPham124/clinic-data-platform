-- Critical DAG SLO metrics for restore planning.
-- Source table (you maintain/populate): ops_monitor.dag_run_metrics
-- Required columns:
--   dag_id STRING
--   run_id STRING
--   status STRING            -- e.g. success/failed
--   start_ts TIMESTAMP
--   end_ts TIMESTAMP
--   duration_sec FLOAT64     -- optional; query falls back to end-start
--
-- Destination (recommended):
--   wata-clinicdataplatform-gcp.ops_monitor.daily_critical_dag_slo

WITH critical_dags AS (
  -- Edit restore_window_minutes for your policy.
  SELECT 'main_cdc_flow' AS dag_id, 180 AS restore_window_minutes UNION ALL
  SELECT 'cdc_daily_to_silver' AS dag_id, 120 AS restore_window_minutes UNION ALL
  SELECT 'dbt_platinum_gold_build_test' AS dag_id, 180 AS restore_window_minutes
),
runs AS (
  SELECT
    dag_id,
    run_id,
    LOWER(COALESCE(status, 'unknown')) AS status,
    start_ts,
    end_ts,
    COALESCE(duration_sec, TIMESTAMP_DIFF(end_ts, start_ts, SECOND)) AS duration_sec
  FROM `wata-clinicdataplatform-gcp.ops_monitor.dag_run_metrics`
  WHERE start_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
success_p95 AS (
  SELECT
    d.dag_id,
    d.restore_window_minutes,
    APPROX_QUANTILES(r.duration_sec, 100)[OFFSET(95)] / 60.0 AS critical_p95_minutes
  FROM critical_dags d
  LEFT JOIN runs r
    ON r.dag_id = d.dag_id
   AND r.status = 'success'
   AND r.start_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
  GROUP BY d.dag_id, d.restore_window_minutes
),
fail_to_next_success AS (
  SELECT
    f.dag_id,
    f.run_id AS failed_run_id,
    f.end_ts AS failed_end_ts,
    MIN(s.start_ts) AS next_success_start_ts
  FROM runs f
  LEFT JOIN runs s
    ON s.dag_id = f.dag_id
   AND s.status = 'success'
   AND s.start_ts > f.end_ts
  WHERE f.status = 'failed'
    AND f.end_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
  GROUP BY f.dag_id, f.run_id, f.end_ts
),
recovery AS (
  SELECT
    dag_id,
    APPROX_QUANTILES(
      TIMESTAMP_DIFF(next_success_start_ts, failed_end_ts, MINUTE),
      100
    )[OFFSET(95)] AS recovery_time_minutes
  FROM fail_to_next_success
  WHERE next_success_start_ts IS NOT NULL
  GROUP BY dag_id
)
SELECT
  CURRENT_DATE('Asia/Ho_Chi_Minh') AS monitor_date_local,
  s.dag_id,
  s.restore_window_minutes,
  s.critical_p95_minutes,
  COALESCE(r.recovery_time_minutes, 0) AS recovery_time_minutes,
  s.restore_window_minutes - COALESCE(s.critical_p95_minutes, 0) AS restore_buffer_minutes,
  CURRENT_TIMESTAMP() AS monitor_ts_utc
FROM success_p95 s
LEFT JOIN recovery r
  ON r.dag_id = s.dag_id
ORDER BY s.dag_id;
