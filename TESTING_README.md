# End-to-End System and Data Testing Guide

## 1) Scope
This document defines a full testing strategy for the current workspace (`dags`, `pipelines`, `dbt`, `contract`, `monitoring`).

It covers:
- System tests (orchestration, runtime behavior, failure recovery, idempotency)
- Data tests (schema, quality, freshness, consistency, business constraints)
- Execution steps (how to run)
- Acceptance criteria (what is considered PASS)

## 2) Pipeline Under Test
Current production-like flow has two entry paths:
- Full snapshot path: `main_full_batch_flow` -> snapshot pipeline -> curated -> master -> dbt
- Daily CDC path: `main_cdc_flow` -> CDC normalize/merge -> curated -> master -> dbt

Main technical components:
- Airflow DAGs in `dags/`
- Spark jobs in `pipelines/`
- BigQuery merge script: `pipelines/cdc/merge/run_query_merge_v2.updated3.sh`
- dbt models in `dbt/models/`
- Data contracts in `contract/`
- Monitoring SQL and SLO queries in `monitoring/sql/`

## 3) Test Environments
Use at least 3 environments:
- DEV: fast iteration, synthetic + sampled production-like data
- STAGING/UAT: production-scale validation, release gate
- PROD: smoke tests and continuous monitoring only (no destructive tests)

Recommended isolation:
- Separate GCS buckets per env (`*-raw`, `*-staging`, `*-quarantine`)
- Separate BigQuery datasets per env (`silver`, `silver_curated`, `platinum`, `gold`, `ops_monitor`)
- Separate Composer/Dataproc resources or dedicated namespaces

## 4) Test Pyramid (What to automate first)
- Level 1: Static checks (DAG parse, contract JSON validity, SQL compile)
- Level 2: Component integration tests (single pipeline script with controlled inputs)
- Level 3: End-to-end DAG tests (full path run and assertions)
- Level 4: Continuous data quality and observability checks (scheduled in BigQuery)

## 5) System Test Suites

## ST-01 DAG Parse and Dependency Integrity
Purpose:
- Ensure all DAG files load and task dependencies are valid before deployment.

How to run:
1. In Composer or CI image with Airflow installed, run DAG import checks.
2. Validate that all DAG IDs expected by trigger DAGs exist:
   - `full_snapshot_batch_pipeline`
   - `cdc_daily_to_silver`
   - `silver_to_silver_curated_dq_v2_3`
   - `gen_master_parallel`
   - `dbt_platinum_gold_build_test`

PASS criteria:
- 0 import errors
- Trigger DAGs (`main_full_batch_flow`, `main_cdc_flow`) reference existing DAG IDs only
- No circular dependencies

## ST-02 Contract and Runtime Argument Validation
Purpose:
- Prevent runtime failures caused by invalid contract JSON or bad parameters.

How to run:
1. Validate JSON syntax for:
   - `contract/staging_contract_snapshot_v1.json`
   - `contract/cdc_staging_contract_v1.json`
   - `contract/cdc_merge_contract_v1.json`
   - `contract/silver_curated_business_rules_v1.json`
2. Validate required fields per table:
   - `primary_key` must exist
   - `schema`/`columns` definition must exist
3. Validate naming safety:
   - table and column identifiers match safe patterns

PASS criteria:
- All contract files parse successfully
- No table has empty PK definition
- No unsafe identifiers

## ST-03 Snapshot Pipeline E2E
Purpose:
- Validate `postgres_to_gcs.py` -> `raw_to_staging.py` -> `staging_to_silver.py` flow.

How to run:
1. Trigger `full_snapshot_batch_pipeline` with a fixed `LOAD_DATE` and test `RUN_ID`.
2. Verify GCS outputs:
   - raw files under `snapshot/.../run_id=<RUN_ID>/`
   - staging parquet and quarantine paths
3. Verify BigQuery load to `silver.*` tables.
4. Verify history artifacts (`summary.json`, `staging_to_silver.json`).

PASS criteria:
- DAG run status = `success`
- For every non-empty source table: `silver` row count > 0
- History files exist and include matching `run_id`
- No unhandled exception in task logs

## ST-04 CDC Normalize + Merge E2E
Purpose:
- Validate CDC path from AVRO to merged `silver` tables.

How to run:
1. Trigger `cdc_daily_to_silver` with deterministic `source_date_local`.
2. Check staging manifests (`_manifests/cdc/...run_id=...json`).
3. Confirm stage load + merge completed for target tables.
4. Verify watermark table updates in `silver_meta.watermarks`.

PASS criteria:
- DAG run status = `success`
- At least one table processed (unless intentionally empty day)
- `silver_meta.watermarks.updated_at` refreshed for processed tables
- No merge SQL failure in `run_query_merge_v2.updated3.sh` logs

## ST-05 Idempotency and Re-run Safety
Purpose:
- Ensure same input and `run_id/source_date` do not produce duplicates or inconsistent state.

How to run:
1. Re-run CDC merge for same (`source_date_local`, `run_id`).
2. Re-run full snapshot with same `run_id` in isolated test dataset.
3. Run duplicate PK checks (`monitoring/sql/04_silver_duplicate_pk_template.sql`) for critical tables.

PASS criteria:
- No growth in duplicate PK count after second run
- Watermark values do not regress
- Row counts remain stable within expected tolerance (0 delta for strict idempotent case)

## ST-06 Curated DQ Pipeline
Purpose:
- Validate `silver_to_silver_curated_dq_v2_3.py` behavior with rule application.

How to run:
1. Trigger DAG `silver_to_silver_curated_dq_v2_3`.
2. Verify outputs:
   - `silver_curated.*`
   - `silver_curated_quarantine.*__quarantine`
   - `silver_curated_dq.dq_run_metrics`
3. Inject known invalid records in DEV and rerun.

PASS criteria:
- Invalid rows routed to quarantine with `dq_reason`
- Metrics table has correct `total/good/bad` counts
- Curated tables exclude rows violating configured rules

## ST-07 Master Data Generation
Purpose:
- Validate `gen_master_parallel` creates stable, deterministic master IDs/codes.

How to run:
1. Trigger `gen_master_parallel` DAG.
2. Validate outputs:
   - `master.patients_master_patient_id_v1`
   - `master.clinic_patients_master_patient_id_v1`
   - drug master output table configured in script args
3. Re-run with same input snapshot.

PASS criteria:
- Output tables written successfully
- Deterministic keys: same source record -> same generated ID/code
- No unexpected null IDs for records that have required source attributes

## ST-08 dbt Build and Test Chain
Purpose:
- Validate transformations and tests for `platinum` and `gold` layers.

How to run:
1. Trigger DAG `dbt_platinum_gold_build_test`.
2. Or run manually:
   - `dbt build --target platinum --select path:models/platinum --project-dir . --profiles-dir .`
   - `dbt build --target gold --select path:models/gold --project-dir . --profiles-dir .`
3. Review `dbt test` results from schema files.

PASS criteria:
- dbt command exit code = 0
- 0 failing critical tests (`not_null`, `unique`) on key dimensions/facts
- Gold models built from latest valid platinum sources

## ST-09 Failure and Recovery (Chaos/Resilience)
Purpose:
- Verify operational recovery when one stage fails.

How to run:
1. Simulate controlled failures in DEV:
   - missing contract file
   - invalid bucket permission
   - Dataproc job failure
2. Observe behavior of trigger DAGs and retries.
3. Apply fix and rerun failed DAG.

PASS criteria:
- Failures are visible in Airflow/monitoring within SLA
- Recovery run succeeds without manual table cleanup in normal failure modes
- No orphan partial state that breaks subsequent runs

## ST-10 Observability and SLO Checks
Purpose:
- Ensure monitoring pipeline correctly detects system/data issues.

How to run:
1. Schedule and execute monitoring SQL in `monitoring/sql/`.
2. Verify expected output tables in `ops_monitor`.
3. Validate dashboard widgets and alert conditions.

PASS criteria:
- Scheduled queries run successfully
- Fresh data appears in `ops_monitor.*` on schedule
- Alert thresholds are testable (at least one synthetic non-prod alert fire drill)

## 6) Data Test Suites

## DT-01 Contract Conformance (Raw/Staging)
Checks:
- Required columns exist
- Data types castable to contract definition
- Metadata fields exist for CDC (`__op`, `__lsn_num`, `__commit_ts`, `__run_id`, `__source_date_local`)

How to execute:
- Use Spark/BigQuery validation queries per table after staging load.

PASS criteria:
- 100% required columns present
- Cast failure ratio < 0.1% (or agreed threshold by domain)
- CDC metadata completeness >= 99.9%

## DT-02 Primary Key Integrity (Silver/Platinum)
Checks:
- PK non-null
- PK uniqueness for models where uniqueness is expected

How to execute:
- dbt tests from `dbt/models/platinum/schema.yml`
- `monitoring/sql/04_silver_duplicate_pk_template.sql`

PASS criteria:
- Duplicate PK count = 0 for critical tables
- Null PK count = 0 for critical tables

## DT-03 Freshness and Timeliness
Checks:
- `__commit_ts` lag in silver
- watermark update delay

How to execute:
- `monitoring/sql/01_watermark_health.sql`
- `monitoring/sql/03_silver_freshness.sql`

PASS criteria:
- Silver freshness lag <= 120 minutes (adjust by SLA)
- Watermark stale <= 180 minutes

## DT-04 Quarantine Ratio and Quality Drift
Checks:
- Quarantine ratio by table and by run
- Sudden spikes compared to 7-day baseline

How to execute:
- `monitoring/sql/02_stage_run_quality.sql`
- `silver_curated_dq.dq_run_metrics`

PASS criteria:
- Daily quarantine ratio <= 2% (default operational threshold)
- No unexplained spike > 3x rolling median

## DT-05 Cross-layer Reconciliation
Checks:
- Snapshot raw -> staging -> silver row-count reasonability
- CDC stage -> silver merge consistency
- Fact tables reference existing dim keys (where modeled)

How to execute:
- Build reconciliation SQL per domain (bookings, prescriptions, inventory, health records)
- Compare row deltas by date partition and run_id

PASS criteria:
- Reconciliation difference within agreed tolerance
- No unexplained negative jumps in business-critical marts

## DT-06 Business Rule Validation (Gold)
Checks (examples):
- `gold_low_stock_items`: only items below configured threshold
- `gold_near_expiry_drugs`: expiry window logic respected
- `gold_prescription_value_daily`: totals/averages non-negative and consistent with source facts

How to execute:
- Domain SQL assertions on gold output tables
- Sample-based manual verification with business owners

PASS criteria:
- 0 failed business rule assertions
- Sample validation sign-off from data consumer for release

## DT-07 Invalid Buckets for Fact Models
Checks:
- `*_valid` and `*_invalid` models split correctly
- `invalid_reason` non-null in invalid tables

How to execute:
- Query pairwise counts and anti-joins on each fact pair
  - inventory import/export/snapshot
  - health/disease
  - operational bookings
  - prescription

PASS criteria:
- `valid U invalid` equals base fact rows (no missing rows)
- Invalid rows always have `invalid_reason`

## 7) Release Gate (Go/No-Go)
A release is `GO` only if all conditions below are satisfied in STAGING:
1. ST-01..ST-08 passed
2. DT-02 (PK integrity) passed with zero critical violations
3. DT-03 freshness and watermark within SLA
4. DT-04 quarantine ratio below threshold or approved exception
5. dbt build/test success with no critical failures
6. Monitoring scheduled queries and alerts verified

`NO-GO` triggers:
- Any critical DAG fails twice consecutively
- Duplicate PK in critical silver/platinum entities
- Freshness/watermark beyond SLA without approved maintenance window
- Broken lineage to gold models

## 8) Suggested Automation Plan
Phase 1 (fast wins):
- Add CI job for contract JSON validation + SQL lint + dbt compile
- Run DAG parse test in Composer image

Phase 2:
- Add nightly STAGING E2E run for `main_cdc_flow`
- Auto-run monitoring SQL and publish pass/fail summary

Phase 3:
- Add synthetic data regression pack for edge cases:
  - malformed timestamps
  - missing PK
  - duplicated CDC events
  - delete/reinsert patterns

## 9) Test Evidence to Store
For each test run, keep:
- Airflow DAG run IDs + task states
- Dataproc job IDs and logs
- BigQuery job IDs for merge/dbt/monitoring queries
- Row-count snapshots by layer
- DQ summary (`dq_run_metrics`)
- Incident notes for failed scenarios

Store evidence in a dated folder in GCS or artifact store for auditability.

## 10) Ownership Model
- Data Platform: system reliability tests (ST-01..ST-10)
- Analytics Engineering: dbt and gold rule tests (ST-08, DT-06)
- Data QA/Ops: continuous monitoring and release gate enforcement

This split avoids ownership gaps and keeps release criteria enforceable.
