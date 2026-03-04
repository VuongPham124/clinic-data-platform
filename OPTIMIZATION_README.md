# Optimization Summary (Pipeline + dbt)

Updated: 2026-03-04  
Scope: Spark pipelines, CDC merge SQL, dbt platinum/gold, Composer dbt orchestration.

## 1) Curated DQ (`silver -> silver_curated`)
Status: Done

File:
- `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`

Applied:
- Enable Spark AQE + skew handling:
  - `spark.sql.adaptive.enabled=true`
  - `spark.sql.adaptive.coalescePartitions.enabled=true`
  - `spark.sql.adaptive.skewJoin.enabled=true`
- Replace fixed repartition (`16/8`) with adaptive partition resolver:
  - `resolve_partitions(...)`
  - optional table override from rules JSON:  
    `"performance": {"good_partitions": X, "bad_partitions": Y}`
- Reduce metrics overhead:
  - compute good/bad counts from one classified frame (`__is_bad`) instead of union path.

Expected impact:
- Better runtime stability across small/large tables.
- Less shuffle waste and fewer skew stalls.

## 2) CDC Merge (`bq_stage -> silver`)
Status: Done

File:
- `pipelines/cdc/merge/run_query_merge_v2.updated3.sh`

Applied:
- Introduced run-scoped source pruning before merge:
  - `CREATE TEMP TABLE run_scoped_source ...`
  - filter by `__source_date_local`, `__run_id`, and watermark thresholds.
- Single dedupe pass on scoped data:
  - `CREATE TEMP TABLE run_scoped_latest ... ROW_NUMBER() ...`
- Merge now uses `run_scoped_latest` instead of wide union windows over full stage.
- Watermark update hardened with monotonic logic:
  - `GREATEST(IFNULL(W...), IFNULL(X...))`

Expected impact:
- Lower bytes processed and slot-ms for merge-heavy tables.
- Better rerun safety (no watermark rollback).

## 3) Gen Master Pipelines
### 3.1 Drug code generation
Status: Done

File:
- `pipelines/gen_master/gen_master_drug_code_medicines_script_v2.py`

Applied:
- Removed driver-wide `toPandas()` flow.
- Replaced with partitioned `mapInPandas(...)`.
- Keep existing business rules, but execute distributed.
- Write output directly from Spark DataFrame to BigQuery.

Expected impact:
- Avoid driver memory bottleneck.
- Better scalability with larger medicine tables.

### 3.2 Patient ID generation
Status: Partial (performance optimization not fully active)

File:
- `pipelines/gen_master/gen_master_patient_id_script_patched.py`

Current runtime behavior:
- Uses Python UDF path (`master_id_udf = F.udf(...)`) for `master_patient_id`.
- Sample `.show()` logs were removed.

Note:
- File contains historical/commented sections from previous optimization attempts.
- If needed, this module can be moved to pure Spark SQL expression path in a follow-up patch.

## 4) dbt Gold Layer
Status: Mostly done

Converted to incremental/merge with partition+cluster and incremental filters:
- `dbt/models/gold/gold_visits_doctor_monthly.sql`
- `dbt/models/gold/gold_clinic_visits_doctor_monthly.sql`
- `dbt/models/gold/gold_clinic_doctor_operational_efficiency_weekly.sql`
- `dbt/models/gold/gold_prescription_value_daily.sql`
- `dbt/models/gold/gold_disease_top_by_clinic_age_weekly.sql`
- `dbt/models/gold/gold_best_selling_items.sql`
- `dbt/models/gold/rev_gold_revenue.sql`

Additional scan reduction (kept table materialization due snapshot/current-date semantics):
- `dbt/models/gold/gold_low_stock_items.sql` (column pruning)
- `dbt/models/gold/gold_near_expiry_drugs.sql` (column pruning)
- `dbt/models/gold/gold_slow_moving_items.sql` (column pruning)

Kept as table intentionally:
- `dbt/models/gold/gold_clinic_patient_visits_monthly.sql`  
  (depends on full-history first-visit logic; incremental can drift with late data)

## 5) dbt Platinum Facts
Status: Done for major high-volume facts

Converted to incremental/merge with partition+cluster + lookback filter:
- `dbt/models/platinum/facts/fact_operational_clinic_bookings.sql`
- `dbt/models/platinum/facts/fact_operational_clinic_bookings_valid.sql`
- `dbt/models/platinum/facts/fact_prescription.sql`
- `dbt/models/platinum/facts/fact_prescription_valid.sql`
- `dbt/models/platinum/facts/fact_inventory_export.sql`
- `dbt/models/platinum/facts/fact_inventory_export_valid.sql`
- `dbt/models/platinum/facts/fact_inventory_import.sql`
- `dbt/models/platinum/facts/fact_inventory_import_valid.sql`

Important fix:
- BigQuery range-partition limit issue resolved by changing interval to `100` for int date-key partitions.

## 6) Composer / dbt Full Refresh Operations
Status: Done

New DAG file:
- `dags/dbt_platinum_gold_full_refresh_dag.py`

Purpose:
- Run manual full refresh safely without modifying normal DAG:
  - platinum full-refresh build
  - then gold full-refresh build

## 7) Validation Checklist
Use this after deployment:

1. dbt full refresh bootstrap (one-time after materialization change)
   - Trigger `dbt_platinum_gold_full_refresh`.
2. Normal incremental run
   - Trigger existing `dbt_platinum_gold_build_test`.
3. Verify no schema drift
   - row counts, not_null tests, uniqueness keys.
4. Compare before/after cost and runtime
   - BigQuery bytes processed
   - slot-ms
   - DAG task durations (p50/p95)

## 8) Known Follow-up Items
- Clean up/comment-prune `gen_master_patient_id_script_patched.py` and migrate runtime to SQL-expression path if stricter performance is needed.
- Add automated benchmark notes (before/after metrics) into monitoring docs.
