# Pipelines README

## Purpose
Folder `pipelines/` chứa toàn bộ ETL/ELT scripts cho 3 luồng chính:
- Snapshot full-batch: Postgres -> GCS Raw -> GCS Staging -> BigQuery Silver.
- CDC daily: Datastream AVRO -> GCS staging parquet -> BigQuery silver MERGE.
- Curated + Master data: Silver -> Silver Curated/DQ, Bảng master phục vụ chuẩn hóa mã.

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
- Vai trò: extract snapshot từ PostgreSQL (Airflow connection `postgres_source`) và ghi CSV.GZ vào `gs://amaz-raw/snapshot/...`.
- Cơ chế:
  - Duyệt danh sách bảng hard-code trong `TABLES`.
  - Tách chunk theo `id` (`COPY_CHUNK_SIZE`, mặc định 5,000,000 rows).
  - Chuẩn hóa kiểu dữ liệu trước export (`timestamp -> UTC string`, `json/jsonb/array/numeric -> text`).
- Output:
  - Dữ liệu: `snapshot/<schema.table>/load_date=<...>/run_id=<...>/part-xxxxx.csv.gz`
  - Metadata: `snapshot/_HISTORY/.../summary.json`

### `raw_to_staging.py`
- Vai trò: Spark normalize dữ liệu snapshot raw CSV sang Parquet staging + quarantine.
- Input:
  - `gs://<raw_bucket>/snapshot/.../*.csv.gz`
  - contract JSON (`spark.normalize.contract_path`).
- Xử lý chính:
  - Trim string, cast theo contract,...
  - Dùng `is_deleted`, gen metadata `__lsn_num`, `__commit_ts`.
  - Validate PK/required/timestamp lại.
  - Deduplicate theo PK với uu tiên `updated_at`, `created_at`.
- Output:
  - Staging parquet: `gs://<staging_bucket>/snapshot/...`
  - Quarantine parquet: `gs://<quarantine_bucket>/raw-to-staging/snapshot/...`
  - History JSON trong staging `_HISTORY`.

### `staging_to_silver.py`
- Vai trò: load parquet staging vào BigQuery dataset `silver`.
- Input: contract tables + prefix parquet theo `load_date`, `run_id`.
- Cơ chế:
  - Kiểm tra prefix t?n t?i trên GCS.
  - `WRITE_TRUNCATE` từng bảng đích `silver.<schema_table>`.
- Output:
  - BQ silver tables.
  - History JSON `staging_to_silver.json` tại staging bucket.

## CDC Pipelines

### `cdc/normalize/main.py`
- Entry-point Spark job CDC normalize.
- Parse args (`--tables`, `--contract-path`, `--source-date-local`, `--run-id`, bucket params...).
- Nếu thiếu `run_id` thì tự sinh theo convention: `<env>_<cadence>_<timestamp>_<tz>_<trigger>`.
- Load contract và gọi `run_all_tables`.

### `cdc/normalize/cdc_engine.py`
- Chọn danh sách bảng (`ALL` hoặc danh sách cụ thể).
- Gọi `process_table(...)` cho từng bảng.

### `cdc/normalize/table_processor.py`
- Worker chính cho từng bảng CDC.
- Flow:
  - Resolve input prefix AVRO.
  - Read AVRO, normalize metadata CDC.
  - Cast schema/timestamp theo contract.
  - Split `ok_df` / `bad_df` theo rule.
  - Ghi parquet staging và quarantine.
  - Ghi manifest `_manifests/cdc/...run_id.json`.

### `cdc/normalize/cdc_normalizer.py`
- Chuẩn hóa record Datastream:
  - map change type INSERT/UPDATE/DELETE -> `__op`.
  - derive `__commit_ts` từ source metadata.
  - parse LSN (`__lsn_num`) hoặc fallback inferred sequence cho backfill.
  - tạo metadata (`__uuid`, `__raw_path`, `__source_date_local`, `__run_id`, ...).
  - soft-delete logic (`deleted_at`/`__op`).

### `cdc/normalize/schema_applier.py`
- Cast schema theo contract (`apply_schema_casting`).
- Cast timestamp fields (`apply_timestamp_casting`).
- Build điều kiện invalid (`build_invalid_condition`) dựa trên PK + required fields.

### `cdc/normalize/io_utils.py`
- Resolve input prefix theo ngày/tháng/latest.
- Ghi manifest JSON lên GCS bằng `gsutil cp`.

### `cdc/normalize/contract_loader.py`
- Load contract từ local path hoặc `gs://...` (download t?m v? `/tmp/contracts`).

### `cdc/normalize/cdc_engine.py`
- Ðiều phối lặp bảng CDC theo contract.

### `cdc/merge/run_query_merge_v2.updated3.sh`
- Shell pipeline merge staging parquet vào BigQuery silver.
- Tính nang chính:
  - Validate args và contract.
  - Tạo schema/table metadata (`bq_stage`, `silver`, `silver_meta.watermarks`).
  - Load parquet vào bảng tạm, upsert idempotent vào `bq_stage` theo (`__source_date_local`, `__run_id`).
  - MERGE sang `silver` theo PK, uu tiên bản ghi mới nhất theo `__lsn_num`/`__commit_ts`.
  - Cập nhật watermark thật (`last_lsn_num`) và inferred (`last_inferred_seq`).

## Curated Pipeline

### `curated/silver_to_silver_curated_dq_v2_3.py`
- Spark pipeline từ BigQuery `silver` -> `silver_curated` + quarantine + metrics DQ.
- Core steps:
  - Load contract + business rules JSON.
  - Rule lookup linh họat theo alias bảng (`public.clinics`, `public_clinics`, `clinics`).
  - Apply null policy (`FILL_STRING`, `FILL_NUMBER`, ...).
  - DQ checks: `not_null`, `length>0`, `range[a,b]`, `ge0`, `gt0`, `parseable_ts`, `accepted{}`.
  - Dedup latest theo PK và metadata CDC.
  - Ghi bảng curated (overwrite), quarantine (append/overwrite), metrics `dq_run_metrics`.

## Master Data Pipelines

### `gen_master/gen_master_patient_id_script_patched.py`
- Sinh `master_patient_id` từ dữ liệu users/patients và clinic_users/clinic_patients.
- Rule chính:
  - Normalize tên (bỏ dấu, lấy `last_name + initials`).
  - Chuẩn hóa DOB về `YYYYMMDD`.
  - Chuẩn hóa phone (gi? digits, bảo toàn leading zero).
  - Build ID: `<name_token>_<yyyymmdd>_<phone>`.
- IO: đọc/ghi BigQuery qua Spark BigQuery connector.
- Output mặc định:
  - `master.patients_master_patient_id_v1`
  - `master.clinic_patients_master_patient_id_v1`

### `gen_master/gen_master_drug_code_medicines_script_v2.py`
- Script sinh master drug code (phiên bản v2, logic đầy đủ trong file).
- Tập trung chuẩn hóa active ingredients, dosage, hãng sản xuất,...

### `gen_master/gen_master_drug_code_medicines_script_patched.py`
- Bản patched Dataproc-friendly.
- Ðọc từ BigQuery/CSV, xử lý và normalize họat chất/hàm lượng/manufacturer, sinh:
  - `mathuoc_new`
  - `manufacturer_code`
  - `master_drug_code`
- Có guardrail `max_input_rows` vì dùng `toPandas()`.
- Ghi kết quả về BigQuery.

## Runtime Dependencies
- Python libs: `google-cloud-storage`, `google-cloud-bigquery`, `psycopg2`, `pyspark`, `pandas`.
- Hạ tầng: Airflow/Composer, Dataproc, GCS, BigQuery.
- Một số script yêu cầu CLI trên worker: `gsutil`, `bq`, `python3`.

## Operational Notes
- Các script dùng nhiều path/bucket hard-code (`amaz-raw`, `amaz-staging`, ...)
- CDC merge phụ thuộc chuẩn naming `run_id` và partition `source_date_local`.
- Nên version hóa contract/rules JSON dễ rollback an toàn.

