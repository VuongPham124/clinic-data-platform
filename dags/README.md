# DAGs README

## Purpose
Folder `dags/` chứa các Airflow DAG để orchestration 2 luồng:
- `full-batch` (snapshot).
- `cdc-daily`.

Ngoài ra có DAG cho curated DQ, gen_master, và dbt build/test.

## DAG Inventory

### `full_batch_dag.py` (`dag_id=full_snapshot_batch_pipeline`)
- Luồng snapshot end-to-end:
  1. `pg_to_gcs_raw_snapshot` (PythonOperator -> `pipelines.postgres_to_gcs.run`).
  2. `raw_to_staging_snapshot` (DataprocSubmitJobOperator -> chạy `pipelines/raw_to_staging.py`).
  3. `staging_to_silver_snapshot` (PythonOperator -> `pipelines.staging_to_silver.run`).
- Runtime params:
  - `LOAD_DATE={{ ds }}`
  - `RUN_ID=prod_{{ ts_nodash }}`
- Buckets/datasets: raw `amaz-raw`, staging `amaz-staging`, quarantine `amaz-quarantine`, silver BQ.

### `cdc_daily_to_silver.py` (`dag_id=cdc_daily_to_silver`)
- Luồng CDC daily gồm 2 bước:
  1. Spark normalize CDC AVRO -> parquet staging (Dataproc).
  2. Bash merge staging -> BigQuery silver (`run_query_merge_v2.updated3.sh`).
- Ðiểm quan trọng:
  - `catchup=False`, `max_active_runs=1` để tránh watermark race.
  - Sinh `RUN_ID` từ Airflow run-type (`sched/manual/backfill`).
  - Tự resolve `DAGS_DIR` để tương thích nhiều layout Composer.

### `silver_to_silver_curated_dq.py` (`dag_id=silver_to_silver_curated_dq_v2_3`)
- Chạy Dataproc script `pipelines/curated/silver_to_silver_curated_dq_v2_3.py`.
- Ðẩy 2 file contract/rules bằng `file_uris`.
- Input: BigQuery `silver`; Output: `silver_curated`, `silver_curated_quarantine`, `silver_curated_dq`.

### `gen_master_parallel_dag.py` (`dag_id=gen_master_parallel`)
- Chạy song song 2 Dataproc jobs:
  - `spark_gen_master_patient_id`.
  - `spark_gen_master_drug_code`.
- Có allowlist kiểm soát jar BigQuery connector (`BIGQUERY_CONNECTOR_JAR_URI`).
- Nhận `TEMP_GCS_BUCKET` từ env.

### `dbt_platinum_gold_dag.py` (`dag_id=dbt_platinum_gold_build_test`)
- Chạy dbt qua BashOperator, build theo:
  1. `dbt_build_platinum` (`--select path:models/platinum`).
  2. `dbt_build_gold` (`--select path:models/gold`).
- Tự detect command (`dbt`, `python -m dbt`, `python3 -m dbt`).
- Dùng env vars để set project/location/dataset/threads/target.

### `main_full_batch_flow_dag.py` (`dag_id=main_full_batch_flow`)
- DAG orchestration cho full-batch:
  1. Trigger `full_snapshot_batch_pipeline`
  2. Trigger `silver_to_silver_curated_dq_v2_3`
  3. Trigger `gen_master_parallel`
  4. Trigger `dbt_platinum_gold_build_test`
- Mọi trigger `wait_for_completion=True`.

### `main_cdc_flow_dag.py` (`dag_id=main_cdc_flow`)
- DAG orchestration cho CDC:
  1. Trigger `cdc_daily_to_silver`
  2. Trigger `silver_to_silver_curated_dq_v2_3`
  3. Trigger `gen_master_parallel`
  4. Trigger `dbt_platinum_gold_build_test`

## Design Notes
- Hai DAG `main_*` dóng vai trò coordinator.
- các giá trị cứng: `PROJECT_ID`, `REGION`, `CLUSTER_NAME`, bucket names.

## Environment Variables
- `COMPOSER_BUCKET` / `COMPOSER_GCS_BUCKET` / `DAGS_BUCKET` / `GCS_BUCKET`: resolve GCS path của DAG assets.
- `TEMP_GCS_BUCKET`: temporary bucket cho Spark BigQuery connector.
- `DBT_*`: `DBT_GCP_PROJECT`, `DBT_BQ_LOCATION`, `DBT_DATASET_PLATINUM`, `DBT_DATASET_GOLD`, `DBT_THREADS`, `DBT_TARGET`.

## Suggested Trigger Order
- Full batch: `main_full_batch_flow`.
- CDC daily: `main_cdc_flow`.



