# Pipelines README

## Purpose
The `pipelines/` folder contains all ETL/ELT scripts for 3 main flows:
- Full-batch snapshot: Postgres -> GCS Raw -> GCS Staging -> BigQuery Silver.
- Daily CDC: Datastream AVRO -> GCS staging parquet -> BigQuery silver MERGE.
- Curated + Master data: Silver -> Silver Curated/DQ, master tables for code normalization.

## Folder Structure
- `postgres_to_gcs.py`
- `raw_to_staging.py`
- `staging_to_silver.py`
- `cdc/normalize/*.py`
- `cdc/merge/run_query_merge_v2.updated3.sh`
- `curated/silver_to_silver_curated_dq_v2_3.py`
- `gen_master/*.py`

## Snapshot Pipelines

### `postgres_to_gcs.py`
- Role: extracts a snapshot from PostgreSQL (Airflow connection `postgres_source`) and writes CSV.GZ files to `gs://amaz-raw/snapshot/...`.
- Mechanism:
  - Iterates over a hard-coded table list in `TABLES`.
  - Splits into chunks by `id` (`COPY_CHUNK_SIZE`, default 5,000,000 rows).
  - Normalizes data types before export (`timestamp -> UTC string`, `json/jsonb/array/numeric -> text`).
- Output:
  - Data: `snapshot/<schema.table>/load_date=<...>/run_id=<...>/part-xxxxx.csv.gz`
  - Metadata: `snapshot/_HISTORY/.../summary.json`

### `raw_to_staging.py`
- Role: Spark normalizes raw snapshot CSV data into Parquet staging + quarantine.
- Input:
  - `gs://<raw_bucket>/snapshot/.../*.csv.gz`
  - Contract JSON (`spark.normalize.contract_path`).
- Key processing:
  - String trimming, casting per contract, etc.
  - Uses `is_deleted`, generates metadata `__lsn_num`, `__commit_ts`.
  - Re-validates PK/required/timestamp fields.
  - Deduplicates by PK with priority on `updated_at`, `created_at`.
- Output:
  - Staging parquet: `gs://<staging_bucket>/snapshot/...`
  - Quarantine parquet: `gs://<quarantine_bucket>/raw-to-staging/snapshot/...`
  - History JSON under staging `_HISTORY`.

### `staging_to_silver.py`
- Role: loads staging parquet into BigQuery `silver` dataset.
- Input: contract tables + parquet prefix by `load_date`, `run_id`.
- Mechanism:
  - Checks if prefix exists on GCS.
  - `WRITE_TRUNCATE` each target table `silver.<schema_table>`.
- Output:
  - BQ silver tables.
  - History JSON `staging_to_silver.json` at staging bucket.

## CDC Pipelines

### `cdc/normalize/main.py`
- Entry point for the CDC normalize Spark job.
- Parses args (`--tables`, `--contract-path`, `--source-date-local`, `--run-id`, bucket params, etc.).
- If `run_id` is missing, auto-generates one using convention: `<env>_<cadence>_<timestamp>_<tz>_<trigger>`.
- Loads contract and calls `run_all_tables`.

### `cdc/normalize/cdc_engine.py`
- Selects the table list (`ALL` or a specific list).
- Calls `process_table(...)` for each table.

### `cdc/normalize/table_processor.py`
- Main worker for each CDC table.
- Flow:
  - Resolves AVRO input prefix.
  - Reads AVRO, normalizes CDC metadata.
  - Casts schema/timestamps per contract.
  - Splits into `ok_df` / `bad_df` per rules.
  - Writes staging parquet and quarantine.
  - Writes manifest `_manifests/cdc/...run_id.json`.

### `cdc/normalize/cdc_normalizer.py`
- Normalizes Datastream records:
  - Maps change type INSERT/UPDATE/DELETE -> `__op`.
  - Derives `__commit_ts` from source metadata.
  - Parses LSN (`__lsn_num`) or falls back to inferred sequence for backfill.
  - Creates metadata fields (`__uuid`, `__raw_path`, `__source_date_local`, `__run_id`, etc.).
  - Applies soft-delete logic (`deleted_at`/`__op`).

### `cdc/normalize/schema_applier.py`
- Casts schema per contract (`apply_schema_casting`).
- Casts timestamp fields (`apply_timestamp_casting`).
- Builds invalid condition (`build_invalid_condition`) based on PK + required fields.

### `cdc/normalize/io_utils.py`
- Resolves input prefix by day/month/latest.
- Writes manifest JSON to GCS via `gsutil cp`.

### `cdc/normalize/contract_loader.py`
- Loads contract from a local path or `gs://...` (downloads temporarily to `/tmp/contracts`).

### `cdc/merge/run_query_merge_v2.updated3.sh`
- Shell pipeline that merges staging parquet into BigQuery silver.
- Key features:
  - Validates args and contract.
  - Creates schema/table metadata (`bq_stage`, `silver`, `silver_meta.watermarks`).
  - Loads parquet into a temp table, idempotent upsert into `bq_stage` by (`__source_date_local`, `__run_id`).
  - MERGEs into `silver` by PK, prioritizing the latest record by `__lsn_num`/`__commit_ts`.
  - Updates real watermark (`last_lsn_num`) and inferred watermark (`last_inferred_seq`).

## Curated Pipeline

### `curated/silver_to_silver_curated_dq_v2_3.py`
- Spark pipeline from BigQuery `silver` -> `silver_curated` + quarantine + DQ metrics.
- Core steps:
  - Loads contract + business rules JSON.
  - Flexible rule lookup by table alias (`public.clinics`, `public_clinics`, `clinics`).
  - Applies null policy (`FILL_STRING`, `FILL_NUMBER`, etc.).
  - DQ checks: `not_null`, `length>0`, `range[a,b]`, `ge0`, `gt0`, `parseable_ts`, `accepted{}`.
  - Deduplicates to latest record by PK and CDC metadata.
  - Writes curated table (overwrite), quarantine (append/overwrite), and `dq_run_metrics`.

## Master Data Pipelines

### `gen_master/gen_master_patient_id_script_patched.py`
- Generates `master_patient_id` from users/patients and clinic_users/clinic_patients data.
- Key rules:
  - Normalizes name (removes diacritics, takes `last_name + initials`).
  - Standardizes DOB to `YYYYMMDD`.
  - Standardizes phone number (keeps digits, preserves leading zero).
  - Builds ID: `<name_token>_<yyyymmdd>_<phone>`.
- I/O: reads/writes BigQuery via Spark BigQuery connector.
- Default output tables:
  - `master.patients_master_patient_id_v1`
  - `master.clinic_patients_master_patient_id_v1`

### `gen_master/gen_master_drug_code_medicines_script_v2.py`
- Master drug code generation script (v2, full logic in file).
- Focuses on normalizing active ingredients, dosage, manufacturer, etc.

### `gen_master/gen_master_drug_code_medicines_script_patched.py`
- Dataproc-friendly patched version.
- Reads from BigQuery/CSV, processes and normalizes active ingredients/dosage/manufacturer, generates:
  - `mathuoc_new`
  - `manufacturer_code`
  - `master_drug_code`
- Has `max_input_rows` guardrail due to `toPandas()` usage.
- Writes results back to BigQuery.

## Runtime Dependencies
- Python libs: `google-cloud-storage`, `google-cloud-bigquery`, `psycopg2`, `pyspark`, `pandas`.
- Infrastructure: Airflow/Composer, Dataproc, GCS, BigQuery.
- Some scripts require CLI tools on the worker: `gsutil`, `bq`, `python3`.

## Operational Notes
- Several scripts contain hard-coded paths/buckets (`amaz-raw`, `amaz-staging`, etc.) — standardize via env vars before multi-environment rollout.
- CDC merge depends on consistent `run_id` naming and `source_date_local` partitioning.
- Contract/rules JSON files should be versioned to enable safe rollback.
