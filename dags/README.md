# DAGs README

## Purpose
The `dags/` folder contains Airflow DAGs for orchestrating 2 main flows:
- `full-batch` (snapshot).
- `cdc-daily`.

Additional DAGs are included for curated DQ, gen_master, and dbt build/test.

## DAG Inventory

### `full_batch_dag.py` (`dag_id=full_snapshot_batch_pipeline`)
- End-to-end snapshot flow:
  1. `pg_to_gcs_raw_snapshot` (PythonOperator -> `pipelines.postgres_to_gcs.run`).
  2. `raw_to_staging_snapshot` (DataprocSubmitJobOperator -> runs `pipelines/raw_to_staging.py`).
  3. `staging_to_silver_snapshot` (PythonOperator -> `pipelines.staging_to_silver.run`).
- Runtime params:
  - `LOAD_DATE={{ ds }}`
  - `RUN_ID=prod_{{ ts_nodash }}`
- Buckets/datasets: raw `amaz-raw`, staging `amaz-staging`, quarantine `amaz-quarantine`, silver BQ.

### `cdc_daily_to_silver.py` (`dag_id=cdc_daily_to_silver`)
- CDC daily flow with 2 steps:
  1. Spark normalize CDC AVRO -> parquet staging (Dataproc).
  2. Bash merge staging -> BigQuery silver (`run_query_merge_v2.updated3.sh`).
- Key points:
  - `catchup=False`, `max_active_runs=1` to prevent watermark race conditions.
  - `RUN_ID` is derived from Airflow run type (`sched/manual/backfill`).
  - `DAGS_DIR` is auto-resolved for compatibility across different Composer layouts.

### `silver_to_silver_curated_dq.py` (`dag_id=silver_to_silver_curated_dq_v2_3`)
- Runs Dataproc script `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`.
- Pushes 2 contract/rules files via `file_uris`.
- Input: BigQuery `silver`; Output: `silver_curated`, `silver_curated_quarantine`, `silver_curated_dq`.

### `gen_master_parallel_dag.py` (`dag_id=gen_master_parallel`)
- Runs 2 Dataproc jobs in parallel:
  - `spark_gen_master_patient_id`.
  - `spark_gen_master_drug_code`.
- Includes an allowlist to control the BigQuery connector jar (`BIGQUERY_CONNECTOR_JAR_URI`).
- Reads `TEMP_GCS_BUCKET` from env.

### `dbt_platinum_gold_dag.py` (`dag_id=dbt_platinum_gold_build_test`)
- Runs dbt via BashOperator, building in order:
  1. `dbt_build_platinum` (`--select path:models/platinum`).
  2. `dbt_build_gold` (`--select path:models/gold`).
- Auto-detects dbt command (`dbt`, `python -m dbt`, `python3 -m dbt`).
- Uses env vars to set project/location/dataset/threads/target.

### `main_full_batch_flow_dag.py` (`dag_id=main_full_batch_flow`)
- Orchestration DAG for the full-batch flow:
  1. Trigger `full_snapshot_batch_pipeline`
  2. Trigger `silver_to_silver_curated_dq_v2_3`
  3. Trigger `gen_master_parallel`
  4. Trigger `dbt_platinum_gold_build_test`
- All triggers use `wait_for_completion=True`.

### `main_cdc_flow_dag.py` (`dag_id=main_cdc_flow`)
- Orchestration DAG for the CDC flow:
  1. Trigger `cdc_daily_to_silver`
  2. Trigger `silver_to_silver_curated_dq_v2_3`
  3. Trigger `gen_master_parallel`
  4. Trigger `dbt_platinum_gold_build_test`

## Design Notes
- The two `main_*` DAGs act as coordinators.
- Hard-coded values still present: `PROJECT_ID`, `REGION`, `CLUSTER_NAME`, bucket names — should be standardized via env vars before multi-environment rollout.

## Environment Variables
- `COMPOSER_BUCKET` / `COMPOSER_GCS_BUCKET` / `DAGS_BUCKET` / `GCS_BUCKET`: resolve GCS paths for DAG assets.
- `TEMP_GCS_BUCKET`: temporary bucket for the Spark BigQuery connector.
- `DBT_*`: `DBT_GCP_PROJECT`, `DBT_BQ_LOCATION`, `DBT_DATASET_PLATINUM`, `DBT_DATASET_GOLD`, `DBT_THREADS`, `DBT_TARGET`.

## Suggested Trigger Order
- Full batch flow: trigger `main_full_batch_flow`.
- CDC daily flow: trigger `main_cdc_flow`.
